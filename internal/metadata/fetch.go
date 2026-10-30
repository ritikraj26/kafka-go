package metadata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ritiraj/kafka-go/internal/storage"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrNotLeader        = errors.New("not leader or assigned replica")
	ErrFencedEpoch      = errors.New("fenced leader epoch")
	ErrUnknownEpoch     = errors.New("unknown leader epoch")
	ErrInvalidFetch     = errors.New("invalid fetch")
)

type FetchOptions struct {
	BrokerID, ReplicaID, LeaderEpoch int32
	Offset                           int64
	MaxBytes                         int
	AllowOversizedBatch              bool
}

type FetchResult struct {
	HighWatermark int64
	LogStart      int64
	Records       []byte
}

// FetchRecords holds the partition lock through a bounded read so append, election,
// visibility and replica-progress checks all observe the same partition state.
func (p *Partition) FetchRecords(options FetchOptions) (FetchResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := FetchResult{HighWatermark: p.HighWatermark}
	if options.BrokerID != p.LeaderID || !p.ReplicaAvailable(options.BrokerID) ||
		(options.ReplicaID >= 0 && !p.ReplicaAvailable(options.ReplicaID)) ||
		(options.ReplicaID >= 0 && (options.ReplicaID == p.LeaderID || !slices.Contains(p.ReplicaNodes, options.ReplicaID))) {
		return result, ErrNotLeader
	}
	if options.LeaderEpoch >= 0 {
		if options.LeaderEpoch < p.LeaderEpoch {
			return result, ErrFencedEpoch
		}
		if options.LeaderEpoch > p.LeaderEpoch {
			return result, ErrUnknownEpoch
		}
	}
	if options.MaxBytes < 0 || options.ReplicaID < -1 {
		return result, ErrInvalidFetch
	}
	dir := p.LogDirForBroker(options.BrokerID)
	if dir == "" {
		return result, fmt.Errorf("missing leader log directory")
	}
	segments, err := listSegments(dir)
	if err != nil {
		return result, err
	}
	if len(segments) == 0 {
		return result, fmt.Errorf("missing leader log segments")
	}
	result.LogStart = segments[0].baseOffset
	if options.Offset < result.LogStart || options.Offset > p.LogEndOffset {
		return result, ErrOffsetOutOfRange
	}
	limit := min(p.HighWatermark, p.LogEndOffset)
	if options.ReplicaID >= 0 {
		limit = p.LogEndOffset
	}
	if options.Offset < limit {
		result.Records, err = readBatches(dir, segments, options, limit)
		if err != nil {
			return result, err
		}
	}
	if options.ReplicaID >= 0 {
		// The offset in this request describes earlier durable receipt, not the
		// records being returned now. Never acknowledge the response in advance.
		if p.ReplicaLEO == nil {
			p.ReplicaLEO = make(map[int32]int64)
		}
		if p.ReplicaLastSeen == nil {
			p.ReplicaLastSeen = make(map[int32]int64)
		}
		p.ReplicaLEO[options.ReplicaID] = options.Offset
		p.ReplicaLastSeen[options.ReplicaID] = time.Now().UnixMilli()
		p.AdvanceHighWatermark()
		result.HighWatermark = p.HighWatermark
	}
	return result, nil
}

// indexPosition reads only the index entries needed by binary search.
func indexPosition(dir string, segment logSegment, offset int64) (int64, int64, error) {
	path := filepath.Join(dir, strings.TrimSuffix(segment.name, ".log")+".index")
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, segment.baseOffset, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size()%8 != 0 {
		return 0, 0, fmt.Errorf("incomplete offset index: %s", path)
	}
	lo, hi := int64(0), info.Size()/8-1
	position, base := int64(0), segment.baseOffset
	var entry [8]byte
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if _, err := f.ReadAt(entry[:], mid*8); err != nil {
			return 0, 0, err
		}
		relative := int64(int32(binary.BigEndian.Uint32(entry[:4])))
		pos := int64(int32(binary.BigEndian.Uint32(entry[4:])))
		if relative < 0 || pos < 0 || segment.baseOffset > math.MaxInt64-relative {
			return 0, 0, fmt.Errorf("invalid offset index entry: %s", path)
		}
		candidate := segment.baseOffset + relative
		if candidate <= offset {
			position, base = pos, candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return position, base, nil
}

func readBatches(dir string, segments []logSegment, options FetchOptions, limit int64) ([]byte, error) {
	start := 0
	for i, segment := range segments {
		if segment.baseOffset > options.Offset {
			break
		}
		start = i
	}
	position, next, err := indexPosition(dir, segments[start], options.Offset)
	if err != nil {
		return nil, err
	}
	var records []byte
	for i := start; i < len(segments) && next < limit; i++ {
		if i > start {
			position = 0
			if segments[i].baseOffset != next {
				return nil, fmt.Errorf("noncontiguous segment %s", segments[i].name)
			}
		}
		path := filepath.Join(dir, segments[i].name)
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		part, end, stopped, readErr := readSegmentBatches(f, position, next, options, limit, len(records))
		closeErr := f.Close()
		if readErr != nil {
			return nil, fmt.Errorf("fetch %s: %w", path, readErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		records = append(records, part...)
		next = end
		if stopped {
			return records, nil
		}
	}
	if next < limit {
		return nil, fmt.Errorf("log ends before advertised readable offset %d", limit)
	}
	return records, nil
}

func readSegmentBatches(f *os.File, position, next int64, options FetchOptions, limit int64, alreadyRead int) ([]byte, int64, bool, error) {
	var records []byte
	for next < limit {
		var header [27]byte
		n, err := f.ReadAt(header[:], position)
		if err == io.EOF && n == 0 {
			return records, next, false, nil
		}
		if err != nil {
			return nil, next, false, err
		}
		base := int64(binary.BigEndian.Uint64(header[:8]))
		length := int64(int32(binary.BigEndian.Uint32(header[8:12])))
		delta := int64(int32(binary.BigEndian.Uint32(header[23:27])))
		if base != next || delta < 0 || base > math.MaxInt64-delta-1 ||
			length < 49 || length > storage.MaxRecordBatchBytes-12 {
			return nil, next, false, fmt.Errorf("%w: invalid batch header at byte %d", storage.ErrCorruptRecord, position)
		}
		end, size := base+delta+1, length+12
		if options.ReplicaID >= 0 && base < options.Offset && end > options.Offset {
			return nil, next, false, fmt.Errorf("%w: replica offset is not a batch boundary", ErrInvalidFetch)
		}
		if end > limit {
			// A batch crossing HW must not leak records above the visibility boundary.
			return records, next, true, nil
		}
		if end > options.Offset {
			remaining := options.MaxBytes - alreadyRead - len(records)
			if size > int64(remaining) && !(options.AllowOversizedBatch && alreadyRead+len(records) == 0) {
				return records, next, true, nil
			}
			batch, err := storage.ReadRecordBatch(io.NewSectionReader(f, position, size))
			if err != nil {
				return nil, next, false, err
			}
			records = append(records, batch.Data...)
			if alreadyRead+len(records) >= options.MaxBytes {
				return records, end, true, nil
			}
		}
		position += size
		next = end
	}
	return records, next, true, nil
}

// ReplicaPosition is a coherent view used to schedule a follower fetch.
func (p *Partition) ReplicaPosition(id int32) (leader int32, epoch int32, offset int64, assigned bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.LeaderID, p.LeaderEpoch, p.ReplicaLEO[id], slices.Contains(p.ReplicaNodes, id)
}

// AppendFetchedRecords advances the local follower position only after fsync.
// A response from a superseded leader is rejected, not appended to the new log.
func (p *Partition) AppendFetchedRecords(id, leader, epoch int32, offset int64, records []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.LeaderID != leader || p.LeaderEpoch != epoch || id == leader || !slices.Contains(p.ReplicaNodes, id) || !p.ReplicaAvailable(id) {
		return ErrNotLeader
	}
	if p.replicaErrors[id] != nil {
		return fmt.Errorf("replica writes fenced until recovery: %w", p.replicaErrors[id])
	}
	if p.ReplicaLEO[id] != offset {
		return fmt.Errorf("replica position changed during fetch")
	}
	next, err := p.appendReplicaRecords(records, p.LogDirForBroker(id), offset)
	if err != nil {
		if p.replicaErrors == nil {
			p.replicaErrors = make(map[int32]error)
		}
		p.replicaErrors[id] = err
		nodes := slices.DeleteFunc(slices.Clone(p.ISRNodes), func(node int32) bool { return node == id })
		if isrErr := p.ReplaceISR(nodes); isrErr != nil {
			p.appendErr = errors.Join(err, isrErr)
			p.signalChange()
			return p.appendErr
		}
		return err
	}
	if p.ReplicaLEO == nil {
		p.ReplicaLEO = make(map[int32]int64)
	}
	p.ReplicaLEO[id] = next
	return nil
}
