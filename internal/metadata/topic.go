package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
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
	LogDir          string // Path to the partition's log directory

	// Offset tracking
	NextOffset int64      // Next offset to assign to an incoming RecordBatch
	mu         sync.Mutex // Serializes writes to this partition's log file
}

// AppendRecords writes records to the partition's log file under its mutex,
// appends a sparse index entry, and returns the base offset on success.
func (p *Partition) AppendRecords(records []byte, logDir string) (baseOffset int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current file size to record as the byte position in the index.
	logFile := filepath.Join(logDir, "00000000000000000000.log")
	var filePos int64
	if fi, statErr := os.Stat(logFile); statErr == nil {
		filePos = fi.Size()
	}

	if err = writeRecords(logDir, records); err != nil {
		return -1, err
	}

	baseOffset = p.NextOffset

	// Append a 12-byte sparse index entry: relativeOffset(4) + filePosition(4) + baseOffset(8→4 truncated)
	// We store: relativeOffset INT32 + filePosition INT32 (sufficient for files < 4GB)
	if indexErr := appendIndexEntry(logDir, int32(baseOffset), int32(filePos)); indexErr != nil {
		// Non-fatal: index is a performance optimisation, not correctness-critical
		fmt.Printf("Warning: failed to write index entry: %v\n", indexErr)
	}

	p.NextOffset++
	return baseOffset, nil
}

// appendIndexEntry appends an 8-byte entry to the sparse offset index file.
// Format: relativeOffset INT32 (big-endian) + filePosition INT32 (big-endian)
func appendIndexEntry(logDir string, relativeOffset int32, filePos int32) error {
	indexFile := filepath.Join(logDir, "00000000000000000000.index")
	f, err := os.OpenFile(indexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	entry := make([]byte, 8)
	binary.BigEndian.PutUint32(entry[0:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(entry[4:8], uint32(filePos))
	_, err = f.Write(entry)
	return err
}

// SeekToOffset binary-searches the sparse index to find the file byte position
// for the largest index entry whose relativeOffset <= targetOffset.
// Returns 0 if the index doesn't exist or targetOffset is before the first entry.
func (p *Partition) SeekToOffset(targetOffset int64) (int64, error) {
	indexFile := filepath.Join(p.LogDir, "00000000000000000000.index")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		return 0, nil // no index → start from beginning
	}

	const entrySize = 8
	numEntries := len(data) / entrySize
	if numEntries == 0 {
		return 0, nil
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
	return result, nil
}

// writeRecords appends raw record batch bytes to the partition's log file.
func writeRecords(logDir string, records []byte) error {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	logFile := filepath.Join(logDir, "00000000000000000000.log")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer f.Close()
	if _, err = f.Write(records); err != nil {
		return fmt.Errorf("failed to write records: %w", err)
	}
	return nil
}

// create a new topic with the given name and ID
func NewTopic(name string, numPartitions int) *Topic {
	topic := &Topic{
		Name:       name,
		ID:         uuid.New(),
		IsInternal: false,
		Partitions: make([]Partition, numPartitions),
	}

	// initiallze partitions with default values
	for i := 0; i < numPartitions; i++ {
		topic.Partitions[i] = Partition{
			Index:           int32(i),
			LeaderID:        1,          // Default broker ID
			ReplicaNodes:    []int32{1}, // Single replica on broker 1
			ISRNodes:        []int32{1}, // Replica is in-sync
			OfflineReplicas: []int32{},  // No offline replicas
			LeaderEpoch:     0,
			PartitionEpoch:  0,
		}
	}

	return topic
}
