package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	MaxBatches                       int
	StrictFirstBatchLimit            bool
	FirstBatchMaxBytes               int
}

type FetchResult struct {
	HighWatermark int64
	LogStart      int64
	Records       []byte
}

// FetchRecords reads an append-only log snapshot, revalidating leadership before
// materialization. Network callers reserve memory between PlanFetch and ReadFetchPlan.
func (p *Partition) FetchRecords(options FetchOptions) (FetchResult, error) {
	return p.FetchRecordsContext(context.Background(), options)
}

func (p *Partition) FetchRecordsContext(ctx context.Context, options FetchOptions) (FetchResult, error) {
	if options.MaxBatches == 0 {
		options.MaxBatches = 10000
	}
	plan, err := p.PlanFetch(ctx, options)
	if plan == nil {
		return FetchResult{}, err
	}
	result := FetchResult{HighWatermark: plan.HighWatermark, LogStart: plan.LogStart}
	if err != nil {
		return result, err
	}
	result.Records, err = p.ReadFetchPlan(ctx, plan)
	return result, err
}

type batchSpan struct {
	segment  string
	position int64
	size     int
	base     int64
	delta    int32
}

type FetchPlan struct {
	HighWatermark, LogStart int64
	Bytes, Batches          int
	Limited                 bool
	owner                   *Partition
	options                 FetchOptions
	epoch                   int32
	dir                     string
	spans                   []batchSpan
}

// PlanFetch inspects only bounded headers. No record payload is allocated here.
func (p *Partition) PlanFetch(ctx context.Context, options FetchOptions) (*FetchPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result := &FetchPlan{HighWatermark: p.HighWatermark, owner: p, options: options, epoch: p.LeaderEpoch}
	if err := ctx.Err(); err != nil {
		return result, err
	}
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
	if options.MaxBytes < 0 || options.ReplicaID < -1 || options.MaxBatches < 0 {
		return result, ErrInvalidFetch
	}
	dir := p.LogDirForBroker(options.BrokerID)
	result.dir = dir
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
		err = planBatches(ctx, dir, segments, options, limit, result)
		if err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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

func planBatches(ctx context.Context, dir string, segments []logSegment, options FetchOptions, limit int64, plan *FetchPlan) error {
	start := 0
	for i, segment := range segments {
		if segment.baseOffset > options.Offset {
			break
		}
		start = i
	}
	position, next, err := safeIndexPosition(ctx, dir, segments[start], options.Offset, &plan.owner.indexCheckpoint)
	if err != nil {
		return err
	}
	for i := start; i < len(segments) && next < limit; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i > start {
			position = 0
			if segments[i].baseOffset != next {
				return fmt.Errorf("noncontiguous segment %s", segments[i].name)
			}
		}
		path := filepath.Join(dir, segments[i].name)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		end, stopped, readErr := planSegment(ctx, f, segments[i].name, position, next, options, limit, plan)
		closeErr := f.Close()
		if readErr != nil {
			return fmt.Errorf("fetch %s: %w", path, readErr)
		}
		if closeErr != nil {
			return closeErr
		}
		next = end
		if stopped {
			return nil
		}
	}
	if next < limit {
		return fmt.Errorf("log ends before advertised readable offset %d", limit)
	}
	return nil
}

func planSegment(ctx context.Context, f *os.File, segment string, position, next int64, options FetchOptions, limit int64, plan *FetchPlan) (int64, bool, error) {
	for next < limit {
		if err := ctx.Err(); err != nil {
			return next, false, err
		}
		var header [27]byte
		n, err := f.ReadAt(header[:], position)
		if err == io.EOF && n == 0 {
			return next, false, nil
		}
		if err != nil {
			return next, false, err
		}
		base := int64(binary.BigEndian.Uint64(header[:8]))
		length := int64(int32(binary.BigEndian.Uint32(header[8:12])))
		delta := int64(int32(binary.BigEndian.Uint32(header[23:27])))
		if base != next || delta < 0 || base > math.MaxInt64-delta-1 ||
			length < 49 || length > storage.MaxRecordBatchBytes-12 {
			return next, false, fmt.Errorf("%w: invalid batch header at byte %d", storage.ErrCorruptRecord, position)
		}
		end, size := base+delta+1, length+12
		if options.ReplicaID >= 0 && base < options.Offset && end > options.Offset {
			return next, false, fmt.Errorf("%w: replica offset is not a batch boundary", ErrInvalidFetch)
		}
		if end > limit {
			// A batch crossing HW must not leak records above the visibility boundary.
			return next, true, nil
		}
		if end > options.Offset {
			remaining := options.MaxBytes - plan.Bytes
			firstAllowed := options.AllowOversizedBatch && plan.Bytes == 0 &&
				(!options.StrictFirstBatchLimit || size <= int64(options.FirstBatchMaxBytes))
			if plan.Batches >= options.MaxBatches || (size > int64(remaining) && !firstAllowed) {
				plan.Limited = true
				return next, true, nil
			}
			plan.spans = append(plan.spans, batchSpan{segment: segment, position: position, size: int(size), base: base, delta: int32(delta)})
			plan.Bytes += int(size)
			plan.Batches++
			if plan.Bytes >= options.MaxBytes {
				plan.Limited = true
				return end, true, nil
			}
		}
		position += size
		next = end
	}
	return next, true, nil
}

// ReadFetchPlan allocates exactly the selected payload bytes after admission.
// The append-only log may grow, but leadership and the selected headers must
// still match. No per-record value slices are built for a fetch response.
func (p *Partition) ReadFetchPlan(ctx context.Context, plan *FetchPlan) (data []byte, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan.owner != p || p.LeaderID != plan.options.BrokerID || p.LeaderEpoch != plan.epoch || !p.ReplicaAvailable(p.LeaderID) {
		return nil, ErrNotLeader
	}
	expected := 0
	for _, span := range plan.spans {
		expected += span.size
	}
	if expected != plan.Bytes {
		return nil, fmt.Errorf("fetch plan size was modified")
	}
	if plan.Bytes == 0 {
		return nil, nil
	}
	data = make([]byte, plan.Bytes)
	var file *os.File
	var current string
	defer func() {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			data = nil
		}
	}()
	position := 0
	for _, span := range plan.spans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if current != span.segment {
			if file != nil {
				closeErr := file.Close()
				file = nil
				if closeErr != nil {
					return nil, closeErr
				}
			}
			file, err = os.Open(filepath.Join(plan.dir, span.segment))
			if err != nil {
				return nil, err
			}
			current = span.segment
		}
		batch := data[position : position+span.size]
		if _, err := file.ReadAt(batch, span.position); err != nil {
			return nil, err
		}
		if int64(binary.BigEndian.Uint64(batch[:8])) != span.base || int32(binary.BigEndian.Uint32(batch[23:27])) != span.delta {
			return nil, fmt.Errorf("%w: batch changed since fetch planning", storage.ErrCorruptRecord)
		}
		if err := storage.ValidateRecordBatch(batch); err != nil {
			return nil, err
		}
		position += span.size
	}
	return data, nil
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
	return p.AppendFetchedRecordsContext(context.Background(), id, leader, epoch, offset, records)
}

func (p *Partition) AppendFetchedRecordsContext(ctx context.Context, id, leader, epoch int32, offset int64, records []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
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
