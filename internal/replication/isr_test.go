package replication

import (
	"testing"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

func TestUpdateISR_ShrinkOnLag(t *testing.T) {
	p := &metadata.Partition{
		Index:        0,
		LeaderID:     1,
		ReplicaNodes: []int32{1, 2, 3},
		ISRNodes:     []int32{1, 2, 3},
		LogEndOffset: 10,
		ReplicaLEO:   map[int32]int64{1: 10, 2: 10, 3: 10},
		ReplicaLastSeen: map[int32]int64{
			1: time.Now().UnixMilli(),
			2: time.Now().UnixMilli(),
			3: time.Now().UnixMilli() - 20000, // broker 3 lagging 20s
		},
	}

	UpdateISR(p, DefaultReplicaLagTimeMaxMs)

	if len(p.ISRNodes) != 2 {
		t.Fatalf("ISR size = %d, want 2", len(p.ISRNodes))
	}
	for _, id := range p.ISRNodes {
		if id == 3 {
			t.Error("broker 3 should have been removed from ISR")
		}
	}
}

func TestExpandISR(t *testing.T) {
	p := &metadata.Partition{
		Index:        0,
		LeaderID:     1,
		ReplicaNodes: []int32{1, 2},
		ISRNodes:     []int32{1}, // broker 2 was removed
		LogEndOffset: 5,
		ReplicaLEO:   map[int32]int64{1: 5, 2: 5}, // broker 2 caught up
	}

	ExpandISR(p, 2)

	if len(p.ISRNodes) != 2 {
		t.Fatalf("ISR size = %d, want 2", len(p.ISRNodes))
	}
}

func TestShrinkISR(t *testing.T) {
	p := &metadata.Partition{
		Index:        0,
		LeaderID:     1,
		ReplicaNodes: []int32{1, 2, 3},
		ISRNodes:     []int32{1, 2, 3},
	}

	ShrinkISR(p, 2)

	if len(p.ISRNodes) != 2 {
		t.Fatalf("ISR size = %d, want 2", len(p.ISRNodes))
	}
}
