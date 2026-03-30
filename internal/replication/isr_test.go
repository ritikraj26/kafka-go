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

// TestUpdateISR_ShrinkOnLEOLag verifies that a replica heartbeating on time but
// lagging more than maxLeoLag (10 000 messages) behind the leader is evicted.
func TestUpdateISR_ShrinkOnLEOLag(t *testing.T) {
	p := &metadata.Partition{
		Index:        0,
		LeaderID:     1,
		ReplicaNodes: []int32{1, 2, 3},
		ISRNodes:     []int32{1, 2, 3},
		LogEndOffset: 20000,
		ReplicaLEO:   map[int32]int64{1: 20000, 2: 20000, 3: 5000}, // broker 3 is 15 000 messages behind
		ReplicaLastSeen: map[int32]int64{
			1: time.Now().UnixMilli(),
			2: time.Now().UnixMilli(),
			3: time.Now().UnixMilli(), // recent heartbeat — passes time check, but fails LEO check
		},
	}

	UpdateISR(p, DefaultReplicaLagTimeMaxMs)

	if len(p.ISRNodes) != 2 {
		t.Fatalf("ISR size = %d, want 2 (broker 3 should be evicted for LEO lag)", len(p.ISRNodes))
	}
	for _, id := range p.ISRNodes {
		if id == 3 {
			t.Error("broker 3 should have been removed from ISR due to LEO lag")
		}
	}
}
