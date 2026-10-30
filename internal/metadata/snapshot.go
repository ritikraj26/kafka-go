package metadata

import (
	"fmt"
	"slices"
)

type PartitionSnapshot struct {
	Index, LeaderID, LeaderEpoch int32
	Replicas, ISR, Offline       []int32
}

func (p *Partition) Snapshot() PartitionSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PartitionSnapshot{
		Index: p.Index, LeaderID: p.LeaderID, LeaderEpoch: p.LeaderEpoch,
		Replicas: slices.Clone(p.ReplicaNodes), ISR: slices.Clone(p.ISRNodes), Offline: slices.Clone(p.OfflineReplicas),
	}
}

// OffsetBounds returns the retained start and consumer-visible end on the leader.
func (p *Partition) OffsetBounds(brokerID int32, replica bool) (int64, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if brokerID != p.LeaderID || !p.ReplicaAvailable(brokerID) {
		return 0, 0, ErrNotLeader
	}
	segments, err := listSegments(p.LogDirForBroker(brokerID))
	if err != nil {
		return 0, 0, err
	}
	if len(segments) == 0 {
		return 0, 0, fmt.Errorf("missing partition segments")
	}
	end := p.HighWatermark
	if replica {
		end = p.LogEndOffset
	}
	return segments[0].baseOffset, end, nil
}
