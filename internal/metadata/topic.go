package metadata

import (
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

// AppendRecords writes records to the partition's log file under its mutex
// and returns the base offset (offset before the write) on success.
func (p *Partition) AppendRecords(records []byte, logDir string) (baseOffset int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err = writeRecords(logDir, records); err != nil {
		return -1, err
	}

	baseOffset = p.NextOffset
	p.NextOffset++
	return baseOffset, nil
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
