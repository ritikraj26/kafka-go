package metadata

import (
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
