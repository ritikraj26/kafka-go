package metadata

import (
	"testing"
)

func TestElectLeader(t *testing.T) {
	m := NewManager()
	topic := m.CreateTopic("elect-topic", 1)

	// Initially leader is first replica
	origLeader := topic.Partitions[0].LeaderID

	// Set ISR with multiple brokers
	topic.Partitions[0].ReplicaNodes = []int32{1, 2, 3}
	topic.Partitions[0].ISRNodes = []int32{1, 2, 3}

	err := m.ElectLeader("elect-topic", 0)
	if err != nil {
		t.Fatalf("ElectLeader error: %v", err)
	}

	newLeader := topic.Partitions[0].LeaderID
	if newLeader == origLeader && len(topic.Partitions[0].ISRNodes) > 1 {
		t.Errorf("leader should change from %d, but got %d", origLeader, newLeader)
	}
	if topic.Partitions[0].LeaderEpoch != 1 {
		t.Errorf("LeaderEpoch = %d, want 1", topic.Partitions[0].LeaderEpoch)
	}
}

func TestElectLeader_UnknownTopic(t *testing.T) {
	m := NewManager()
	err := m.ElectLeader("no-such-topic", 0)
	if err == nil {
		t.Error("expected error for unknown topic")
	}
}
