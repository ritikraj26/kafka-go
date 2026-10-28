package metadata

import (
	"testing"

	"github.com/ritiraj/kafka-go/internal/broker"
)

func TestElectLeader(t *testing.T) {
	m := NewManager()
	if err := m.SetLogDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	m.Brokers = broker.NewRegistry(1)
	for id := int32(1); id <= 3; id++ {
		m.Brokers.Register(&broker.Broker{ID: id})
	}
	topic, err := m.CreateTopic("elect-topic", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Initially leader is first replica
	origLeader := topic.Partitions[0].LeaderID

	err = m.ElectLeader("elect-topic", 0)
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
