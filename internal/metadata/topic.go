package metadata

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/logger"
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
	LogDir          string           // Legacy: leader's log directory (kept for backward compat)
	BrokerLogDirs   map[int32]string // Per-broker log directories (brokerID -> dir)

	// Offset tracking
	NextOffset    int64 // Next offset to assign to an incoming RecordBatch
	HighWatermark int64 // Highest offset replicated to all ISR members
	LogEndOffset  int64 // = NextOffset (alias for replication clarity)
	mu            sync.Mutex
	hwCond        *sync.Cond // signalled when HighWatermark advances

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
// Falls back to the legacy LogDir field if BrokerLogDirs is not set.
func (p *Partition) LogDirForBroker(brokerID int32) string {
	if p.BrokerLogDirs != nil {
		if dir, ok := p.BrokerLogDirs[brokerID]; ok {
			return dir
		}
	}
	return p.LogDir
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

	deadline := time.Now().Add(timeout)

	// Spawn a goroutine that broadcasts after timeout to unblock Wait.
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(timeout):
			p.hwCond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

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
	p.mu.Lock()
	defer p.mu.Unlock()

	writtenFile, filePosBefore, writeErr := writeRecords(logDir, records, p.NextOffset)
	if writeErr != nil {
		return -1, writeErr
	}

	baseOffset = p.NextOffset

	// Build the index path from the actual segment file that was written.
	indexPath := strings.TrimSuffix(writtenFile, ".log") + ".index"
	if indexErr := appendIndexEntry(indexPath, int32(baseOffset), int32(filePosBefore)); indexErr != nil {
		// Non-fatal: index is a performance optimisation, not correctness-critical
		logger.L.Warn("failed to write index entry", "err", indexErr)
	}

	p.NextOffset++
	p.LogEndOffset = p.NextOffset

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
	dir := p.LogDir
	if len(logDir) > 0 && logDir[0] != "" {
		dir = logDir[0]
	}

	segs := listSegments(dir)
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

	// Binary-search the chosen segment's index file.
	indexFile := filepath.Join(dir, strings.TrimSuffix(chosen.name, ".log")+".index")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		// No index for this segment — start reading from the beginning of it.
		return chosen.name, 0, nil
	}

	const entrySize = 8
	numEntries := len(data) / entrySize
	if numEntries == 0 {
		return chosen.name, 0, nil
	}

	// Binary search for largest relativeOffset <= targetOffset
	lo, hi := 0, numEntries-1
	result := int64(0)
	for lo <= hi {
		mid := (lo + hi) / 2
		relOff := int64(binary.BigEndian.Uint32(data[mid*entrySize : mid*entrySize+4]))
		filePos := int64(binary.BigEndian.Uint32(data[mid*entrySize+4 : mid*entrySize+8]))
		if relOff <= targetOffset {
			result = filePos
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return chosen.name, result, nil
}

// AppendReplicaRecords writes fetched record data to a follower's log directory.
// Unlike AppendRecords this does NOT advance NextOffset (that tracks the leader's state).
// It only writes the raw bytes to disk and updates the follower's local LEO.
// replicaLEO is the follower's current log-end-offset, used to name any newly rolled
// segment file correctly (prevents all rolled segments being named "00000000000000000000.log").
func (p *Partition) AppendReplicaRecords(records []byte, replicaLogDir string, replicaLEO int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(records) == 0 {
		return nil
	}
	_, _, err := writeRecords(replicaLogDir, records, replicaLEO)
	return err
}

// AdvanceHighWatermark recalculates HW as min(LEO) across all ISR replicas.
// Caller must hold p.mu. Broadcasts hwCond if HW advances.
func (p *Partition) AdvanceHighWatermark() {
	if len(p.ISRNodes) == 0 {
		return
	}
	if p.ReplicaLEO == nil {
		return
	}
	minLEO := int64(math.MaxInt64)
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
		p.initCond()
		p.hwCond.Broadcast()
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
func listSegments(logDir string) []logSegment {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil
	}
	var segs []logSegment
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".log" || len(name) != 24 {
			continue
		}
		var base int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(name, ".log"), "%d", &base); err != nil {
			continue
		}
		segs = append(segs, logSegment{baseOffset: base, name: name})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })
	return segs
}

// activeSegmentName returns the name of the active (latest) .log segment in logDir.
func activeSegmentName(logDir string) string {
	segs := listSegments(logDir)
	if len(segs) == 0 {
		return "00000000000000000000.log"
	}
	return segs[len(segs)-1].name
}

// segmentNameForOffset formats a segment file name from a base offset.
func segmentNameForOffset(baseOffset int64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

// writeRecords appends raw record batch bytes to the partition's active log segment.
// Rolls to a new segment if the active file exceeds MaxSegmentBytes.
// Returns (logFilePath, filePosBefore, error). filePosBefore is the byte offset
// at which these records were written — used to build the sparse index entry.
func writeRecords(logDir string, records []byte, nextOffset int64) (logFilePath string, filePosBefore int64, err error) {
	if err = os.MkdirAll(logDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create log directory: %w", err)
	}

	segName := activeSegmentName(logDir)
	logFile := filepath.Join(logDir, segName)

	// Capture size before the write (used as the sparse index byte position).
	if fi, statErr := os.Stat(logFile); statErr == nil {
		filePosBefore = fi.Size()
	}

	// Roll to a new segment if the current one exceeds the size limit.
	if filePosBefore >= MaxSegmentBytes {
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
	if _, writeErr := f.Write(records); writeErr != nil {
		return "", 0, fmt.Errorf("failed to write records: %w", writeErr)
	}
	if syncErr := f.Sync(); syncErr != nil {
		return "", 0, fmt.Errorf("failed to fsync log file: %w", syncErr)
	}
	return logFile, filePosBefore, nil
}

// create a new topic with the given name and ID
// brokerIDs and replicationFactor control replica placement. If brokerIDs is
// nil or empty, falls back to a single-broker default (broker 1).
func NewTopic(name string, numPartitions int, brokerIDs ...[]int32) *Topic {
	topic := &Topic{
		Name:       name,
		ID:         uuid.New(),
		IsInternal: false,
		Partitions: make([]Partition, numPartitions),
	}

	// Determine broker list
	var ids []int32
	if len(brokerIDs) > 0 && len(brokerIDs[0]) > 0 {
		ids = brokerIDs[0]
	} else {
		ids = []int32{1}
	}

	// initialise partitions with round-robin replica assignment
	for i := 0; i < numPartitions; i++ {
		// Pick replicas starting at (i % len(ids)), wrapping around
		rf := len(ids)
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
