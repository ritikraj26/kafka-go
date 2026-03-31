package replication

import (
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
)

// DefaultReplicaLagTimeMaxMs is the default max time (ms) a replica can lag before ISR removal.
// Equivalent to Kafka's replica.lag.time.max.ms (default 30 s; we use 10 s for faster detection).
const DefaultReplicaLagTimeMaxMs int64 = 10000 // 10 seconds

// defaultMaxLeoLag is the maximum allowed message-count difference between a
// replica's LEO and the leader's LEO before the replica is removed from the ISR.
const defaultMaxLeoLag int64 = 10000

// UpdateISR checks each replica for a partition and shrinks/expands the ISR
// based on how recently it fetched and whether its LEO has caught up.
// lagTimeMaxMs controls the maximum allowed lag time in milliseconds.
// Caller must hold p's lock.
func UpdateISR(p *metadata.Partition, lagTimeMaxMs int64) {
	if p.ReplicaLastSeen == nil || p.ReplicaLEO == nil {
		return
	}

	nowMs := time.Now().UnixMilli()

	var newISR []int32
	for _, brokerID := range p.ReplicaNodes {
		if brokerID == p.LeaderID {
			// Leader is always in ISR
			newISR = append(newISR, brokerID)
			continue
		}
		lastSeen, ok := p.ReplicaLastSeen[brokerID]
		if !ok {
			// Never fetched — not in ISR
			continue
		}

		lagTime := nowMs - lastSeen
		if lagTime > lagTimeMaxMs {
			// Replica hasn't fetched recently — remove from ISR
			logger.L.Warn("shrinking ISR: replica time lag",
				"broker", brokerID, "lagMs", lagTime)
			continue
		}

		// LEO check: replica must be within defaultMaxLeoLag messages of the leader.
		// Prevents a fast-heartbeating but slow-replicating replica from staying in ISR.
		replicaLEO, hasLEO := p.ReplicaLEO[brokerID]
		if !hasLEO || (p.LogEndOffset-replicaLEO) > defaultMaxLeoLag {
			logger.L.Warn("shrinking ISR: replica LEO lag",
				"broker", brokerID,
				"replicaLEO", replicaLEO,
				"leaderLEO", p.LogEndOffset,
				"lag", p.LogEndOffset-replicaLEO)
			continue
		}

		// Both time and LEO checks passed — keep in ISR
		newISR = append(newISR, brokerID)
	}

	if len(newISR) != len(p.ISRNodes) {
		logger.L.Info("ISR updated", "partition", p.Index,
			"old_isr", p.ISRNodes, "new_isr", newISR)
		p.PartitionEpoch++
	}

	p.ISRNodes = newISR
}

// ShrinkISR removes a specific broker from the ISR.
// Caller must hold p's lock.
func ShrinkISR(p *metadata.Partition, brokerID int32) {
	var newISR []int32
	for _, id := range p.ISRNodes {
		if id != brokerID {
			newISR = append(newISR, id)
		}
	}
	p.ISRNodes = newISR
}

// ExpandISR adds a broker back to the ISR if it has caught up.
// Caller must hold p's lock.
func ExpandISR(p *metadata.Partition, brokerID int32) {
	for _, id := range p.ISRNodes {
		if id == brokerID {
			return // already in ISR
		}
	}
	replicaLEO, ok := p.ReplicaLEO[brokerID]
	if !ok {
		return
	}
	if replicaLEO >= p.LogEndOffset {
		p.ISRNodes = append(p.ISRNodes, brokerID)
		logger.L.Info("expanded ISR", "partition", p.Index, "broker", brokerID)
	}
}
