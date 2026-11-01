package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/storage"
)

// topic with metadata
type Topic struct {
	Name       string
	ID         uuid.UUID
	IsInternal bool
	Partitions []Partition
}

// partition within a topic
type Partition struct {
	Index           int32
	LeaderID        int32
	ReplicaNodes    []int32
	ISRNodes        []int32
	OfflineReplicas []int32
	LeaderEpoch     int32
	PartitionEpoch  int32
	BrokerLogDirs   map[int32]string // Per-broker log directories (brokerID -> dir)

	// Offset tracking
	NextOffset      int64 // Next record offset to assign
	HighWatermark   int64 // Exclusive offset replicated to all ISR members
	LogEndOffset    int64 // = NextOffset (alias for replication clarity)
	mu              sync.Mutex
	hwCond          *sync.Cond // signalled when HighWatermark advances
	appendErr       error      // Fence leader writes after an ambiguous disk failure
	replicaErrors   map[int32]error
	pendingProduces map[*pendingProduce]struct{}
	changed         chan struct{}

	// Per-replica LEO for replication (broker_id -> LEO)
	ReplicaLEO      map[int32]int64
	ReplicaLastSeen map[int32]int64 // broker_id -> unix millis of last fetch
}

// initCond ensures hwCond is initialised (idempotent, call under mu or before sharing).
func (p *Partition) initCond() {
	if p.hwCond == nil {
		p.hwCond = sync.NewCond(&p.mu)
	}
}

// GetHighWatermark returns the current high watermark (thread-safe).
func (p *Partition) GetHighWatermark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.HighWatermark
}

// GetLogEndOffset returns the current log end offset (thread-safe).
func (p *Partition) GetLogEndOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.LogEndOffset
}

// GetReplicaLEO returns the LEO tracked for a specific broker (thread-safe).
// Returns 0 if the broker has no tracked LEO yet.
func (p *Partition) GetReplicaLEO(brokerID int32) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ReplicaLEO == nil {
		return 0
	}
	return p.ReplicaLEO[brokerID]
}

// LogDirForBroker returns the log directory for a specific broker.
func (p *Partition) LogDirForBroker(brokerID int32) string {
	if p.BrokerLogDirs != nil {
		if dir, ok := p.BrokerLogDirs[brokerID]; ok {
			return dir
		}
	}
	return ""
}

// WaitForHighWatermark blocks until HW >= target or timeout expires.
// Returns true if the target was reached, false on timeout.
func (p *Partition) WaitForHighWatermark(target int64, timeout time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initCond()

	if p.HighWatermark >= target {
		return true
	}
	if timeout <= 0 {
		return false
	}

	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.hwCond.Broadcast()
	})
	defer timer.Stop()

	for p.HighWatermark < target {
		p.hwCond.Wait()
		if p.HighWatermark >= target {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
	}
	return p.HighWatermark >= target
}

// UpdateReplicaState sets a replica's LEO and last-seen timestamp (thread-safe).
func (p *Partition) UpdateReplicaState(brokerID int32, leo int64, lastSeenMs int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ReplicaLEO == nil {
		p.ReplicaLEO = make(map[int32]int64)
	}
	if p.ReplicaLastSeen == nil {
		p.ReplicaLastSeen = make(map[int32]int64)
	}
	p.ReplicaLEO[brokerID] = leo
	p.ReplicaLastSeen[brokerID] = lastSeenMs
}

// RunUnderLock executes fn while holding the partition lock.
// Use for compound read-modify-write operations that need atomicity.
func (p *Partition) RunUnderLock(fn func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn()
}

// AppendRecords writes records to the partition's log file under its mutex,
// appends a sparse index entry, and returns the base offset on success.
func (p *Partition) AppendRecords(records []byte, logDir string) (baseOffset int64, err error) {
	records = bytes.Clone(records)
	batches, err := storage.ValidateRecordBatches(records)
	if err != nil {
		return -1, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.appendRecords(records, batches, logDir)
}

// appendRecords requires p.mu and validated batches backed by a private buffer.
func (p *Partition) appendRecords(records []byte, batches []storage.RecordBatch, logDir string) (int64, error) {
	if p.appendErr != nil {
		return -1, fmt.Errorf("partition unavailable after append failure: %w", p.appendErr)
	}
	baseOffset := p.NextOffset
	nextOffset := baseOffset
	for i := range batches {
		batch := &batches[i]
		count := int64(batch.RecordsCount)
		if nextOffset < 0 || nextOffset > math.MaxInt64-count {
			return -1, fmt.Errorf("%w: offset overflow", storage.ErrCorruptRecord)
		}
		batch.BaseOffset = nextOffset
		// Base offset and leader epoch are outside the CRC32C-covered region.
		binary.BigEndian.PutUint64(batch.Data[:8], uint64(nextOffset))
		binary.BigEndian.PutUint32(batch.Data[12:16], uint32(p.LeaderEpoch))
		nextOffset += count
	}

	writtenFile, filePosBefore, writeErr := writeRecords(logDir, records, baseOffset, nextOffset-1)
	if writeErr != nil {
		p.appendErr = writeErr
		p.signalChange()
		return -1, writeErr
	}

	segmentBase, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(writtenFile), ".log"), 10, 64)
	if err != nil {
		p.appendErr = err
		p.signalChange()
		return -1, err
	}
	indexPath := strings.TrimSuffix(writtenFile, ".log") + ".index"
	for _, batch := range batches {
		if indexErr := appendIndexEntry(indexPath, int32(batch.BaseOffset-segmentBase), int32(filePosBefore)); indexErr != nil {
			logger.L.Warn("failed to write index entry", "err", indexErr)
			break
		}
		filePosBefore += int64(len(batch.Data))
	}

	p.NextOffset = nextOffset
	p.LogEndOffset = nextOffset

	// Update leader's own replica LEO
	if p.ReplicaLEO == nil {
		p.ReplicaLEO = make(map[int32]int64)
	}
	p.ReplicaLEO[p.LeaderID] = p.LogEndOffset

	// Advance HW — if leader is the only ISR member, HW advances immediately
	p.AdvanceHighWatermark()

	return baseOffset, nil
}

// appendIndexEntry appends an 8-byte entry to the given sparse offset index file.
// Format: relativeOffset INT32 (big-endian) + filePosition INT32 (big-endian)
func appendIndexEntry(indexFilePath string, relativeOffset int32, filePos int32) error {
	f, err := os.OpenFile(indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	entry := make([]byte, 8)
	binary.BigEndian.PutUint32(entry[0:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(entry[4:8], uint32(filePos))
	if _, err = f.Write(entry); err != nil {
		return err
	}
	return f.Sync()
}

// SeekToOffset finds the segment file and byte position for targetOffset.
// It selects the last segment whose base offset <= targetOffset, then
// binary-searches that segment's sparse index for the closest byte position.
// Returns (segmentFileName, byteOffset, error).
func (p *Partition) SeekToOffset(targetOffset int64, logDir ...string) (string, int64, error) {
	dir := p.LogDirForBroker(p.LeaderID)
	if len(logDir) > 0 && logDir[0] != "" {
		dir = logDir[0]
	}

	segs, err := listSegments(dir)
	if err != nil {
		return "", 0, err
	}
	if len(segs) == 0 {
		return "00000000000000000000.log", 0, nil
	}

	// Pick the last segment whose base offset <= targetOffset.
	chosen := segs[0]
	for _, s := range segs {
		if s.baseOffset <= targetOffset {
			chosen = s
		} else {
			break
		}
	}

	position, _, err := indexPosition(dir, chosen, targetOffset)
	return chosen.name, position, err
}

// AppendReplicaRecords writes fetched record data to a follower's log directory.
// Unlike AppendRecords this does NOT advance NextOffset (that tracks the leader's state).
// It preserves the leader-assigned offsets; replica progress is tracked separately.
// replicaLEO is the follower's current log-end-offset, used to name any newly rolled
// segment file correctly (prevents all rolled segments being named "00000000000000000000.log").
func (p *Partition) AppendReplicaRecords(records []byte, replicaLogDir string, replicaLEO int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.appendReplicaRecords(records, replicaLogDir, replicaLEO)
	return err
}

func (p *Partition) appendReplicaRecords(records []byte, replicaLogDir string, replicaLEO int64) (int64, error) {
	if len(records) == 0 {
		return replicaLEO, nil
	}
	batches, err := storage.ValidateRecordBatches(records)
	if err != nil {
		return replicaLEO, err
	}
	next := replicaLEO
	for _, batch := range batches {
		if batch.BaseOffset != next || next < 0 || next > math.MaxInt64-int64(batch.RecordsCount) {
			return replicaLEO, fmt.Errorf("%w: unexpected replica offset %d, want %d", storage.ErrCorruptRecord, batch.BaseOffset, next)
		}
		next += int64(batch.RecordsCount)
	}
	_, _, err = writeRecords(replicaLogDir, records, replicaLEO, next-1)
	if err != nil {
		return replicaLEO, err
	}
	return next, nil
}

// AdvanceHighWatermark recalculates HW as min(LEO) across all ISR replicas.
// Caller must hold p.mu. Broadcasts hwCond if HW advances.
func (p *Partition) AdvanceHighWatermark() {
	if !slices.Contains(p.ISRNodes, p.LeaderID) || !p.ReplicaAvailable(p.LeaderID) {
		return
	}
	if p.ReplicaLEO == nil {
		return
	}
	minLEO := p.LogEndOffset
	found := false
	for _, id := range p.ISRNodes {
		leo, ok := p.ReplicaLEO[id]
		if !ok {
			leo = 0
		}
		found = true
		if leo < minLEO {
			minLEO = leo
		}
	}
	if !found {
		return
	}
	if minLEO > p.HighWatermark {
		p.HighWatermark = minLEO
		p.signalChange()
	}
}

// MaxSegmentBytes is the maximum size of a single log segment file before rolling.
var MaxSegmentBytes int64 = 1 << 30 // 1 GB

// logSegment represents a single .log segment file on disk.
type logSegment struct {
	baseOffset int64  // first offset in the segment (encoded in the file name)
	name       string // e.g. "00000000000001000000.log"
}

// listSegments returns all .log segments in logDir sorted by base offset ascending.
func listSegments(logDir string) ([]logSegment, error) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var segs []logSegment
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".log" {
			continue
		}
		if !e.Type().IsRegular() {
			return nil, fmt.Errorf("segment is not a regular file: %s", filepath.Join(logDir, name))
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil || base < 0 || len(name) != 24 || name != segmentNameForOffset(base) {
			return nil, fmt.Errorf("invalid segment name: %s", name)
		}
		segs = append(segs, logSegment{baseOffset: base, name: name})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })
	return segs, nil
}

// segmentNameForOffset formats a segment file name from a base offset.
func segmentNameForOffset(baseOffset int64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

// writeRecords appends raw record batch bytes to the partition's active log segment.
// Rolls to a new segment if the active file exceeds MaxSegmentBytes.
// Returns (logFilePath, filePosBefore, error). filePosBefore is the byte offset
// at which these records were written — used to build the sparse index entry.
func writeRecords(logDir string, records []byte, nextOffset, lastOffset int64) (logFilePath string, filePosBefore int64, err error) {
	if logDir == "" {
		return "", 0, fmt.Errorf("missing partition log directory")
	}
	if err = os.MkdirAll(logDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create log directory: %w", err)
	}

	segs, err := listSegments(logDir)
	if err != nil {
		return "", 0, err
	}
	active := logSegment{baseOffset: nextOffset, name: segmentNameForOffset(nextOffset)}
	if len(segs) > 0 {
		active = segs[len(segs)-1]
	}
	segName := active.name
	logFile := filepath.Join(logDir, segName)

	// Capture size before the write (used as the sparse index byte position).
	if fi, statErr := os.Stat(logFile); statErr == nil {
		filePosBefore = fi.Size()
	} else if !os.IsNotExist(statErr) {
		return "", 0, statErr
	}

	// Roll to a new segment if the current one exceeds the size limit.
	if filePosBefore >= MaxSegmentBytes || filePosBefore+int64(len(records)) > math.MaxInt32 ||
		lastOffset-active.baseOffset > math.MaxInt32 {
		newName := segmentNameForOffset(nextOffset)
		logger.L.Info("rolling log segment", "dir", logDir, "oldSegment", segName, "newSegment", newName)
		logFile = filepath.Join(logDir, newName)
		filePosBefore = 0
	}

	f, openErr := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr != nil {
		return "", 0, fmt.Errorf("failed to open log file: %w", openErr)
	}
	defer f.Close()
	if n, writeErr := f.Write(records); writeErr != nil {
		return "", 0, fmt.Errorf("failed to write records: %w", writeErr)
	} else if n != len(records) {
		return "", 0, io.ErrShortWrite
	}
	if syncErr := f.Sync(); syncErr != nil {
		return "", 0, fmt.Errorf("failed to fsync log file: %w", syncErr)
	}
	return logFile, filePosBefore, nil
}

// newTopic constructs a topic after the manager validates its configuration.
func newTopic(name string, numPartitions int, ids []int32, rf int) *Topic {
	topic := &Topic{
		Name:       name,
		ID:         uuid.New(),
		IsInternal: false,
		Partitions: make([]Partition, numPartitions),
	}

	// initialise partitions with round-robin replica assignment
	for i := 0; i < numPartitions; i++ {
		// Pick replicas starting at (i % len(ids)), wrapping around
		replicas := make([]int32, rf)
		for r := 0; r < rf; r++ {
			replicas[r] = ids[(i+r)%len(ids)]
		}

		topic.Partitions[i] = Partition{
			Index:           int32(i),
			LeaderID:        replicas[0],
			ReplicaNodes:    replicas,
			ISRNodes:        append([]int32{}, replicas...), // initially all in-sync
			OfflineReplicas: []int32{},
			LeaderEpoch:     0,
			PartitionEpoch:  0,
			BrokerLogDirs:   make(map[int32]string),
			ReplicaLEO:      make(map[int32]int64),
			ReplicaLastSeen: make(map[int32]int64),
		}
	}

	return topic
}
