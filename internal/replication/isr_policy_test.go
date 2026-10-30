package replication

import (
	"slices"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/metadata"
)

func TestRemovedReplicaMustFullyCatchUp(t *testing.T) {
	p := &metadata.Partition{
		LeaderID: 1, ReplicaNodes: []int32{1, 2}, ISRNodes: []int32{1}, LogEndOffset: 10,
		ReplicaLEO: map[int32]int64{1: 10, 2: 9}, ReplicaLastSeen: map[int32]int64{2: time.Now().UnixMilli()},
	}
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.ISRNodes, []int32{1}) {
		t.Fatal("a merely recent but lagging replica rejoined ISR")
	}
	p.ReplicaLEO[2] = 10
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.ISRNodes, []int32{1, 2}) {
		t.Fatal("caught-up replica did not rejoin ISR")
	}
}

func TestOfflineReplicaStaysOutUntilRecovery(t *testing.T) {
	p := &metadata.Partition{
		LeaderID: 1, ReplicaNodes: []int32{1, 2}, ISRNodes: []int32{1, 2}, LogEndOffset: 10,
		ReplicaLEO: map[int32]int64{1: 10, 2: 9}, ReplicaLastSeen: map[int32]int64{2: time.Now().UnixMilli()},
	}
	if err := p.SetReplicaOnline(2, false); err != nil {
		t.Fatal(err)
	}
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(p.ISRNodes, 2) {
		t.Fatal("ISR tick undid controller's offline decision")
	}
	if err := p.SetReplicaOnline(2, true); err != nil {
		t.Fatal(err)
	}
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(p.ISRNodes, 2) {
		t.Fatal("reachability alone restored ISR membership")
	}
	p.ReplicaLEO[2] = 10
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(p.ISRNodes, 2) {
		t.Fatal("healthy caught-up replica did not rejoin")
	}
}

func TestISRMembershipChangeWithSameCountAdvancesEpoch(t *testing.T) {
	now := time.Now().UnixMilli()
	p := &metadata.Partition{
		LeaderID: 1, ReplicaNodes: []int32{1, 2, 3}, ISRNodes: []int32{1, 2}, LogEndOffset: 5,
		ReplicaLEO: map[int32]int64{1: 5, 2: 0, 3: 5}, ReplicaLastSeen: map[int32]int64{2: now - 20000, 3: now},
	}
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if p.PartitionEpoch != 1 || !slices.Equal(p.ISRNodes, []int32{1, 3}) || p.HighWatermark != 5 {
		t.Fatal("equal-sized membership change failed to update epoch/HW")
	}
	if err := UpdateISR(p, DefaultReplicaLagTimeMaxMs); err != nil {
		t.Fatal(err)
	}
	if p.PartitionEpoch != 1 {
		t.Fatal("unchanged membership advanced the epoch")
	}
}

func TestISRRejectsUnknownReplica(t *testing.T) {
	p := &metadata.Partition{
		LeaderID: 1, ReplicaNodes: []int32{1}, ISRNodes: []int32{1},
		ReplicaLEO: map[int32]int64{1: 0, 9: 0},
	}
	if err := ExpandISR(p, 9); err == nil {
		t.Fatal("unassigned replica joined ISR")
	}
	if !slices.Equal(p.ISRNodes, []int32{1}) {
		t.Fatal("rejected ISR mutation changed membership")
	}
}
