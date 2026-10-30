package metadata

import (
	"fmt"
	"math"
	"slices"
)

// ElectLeader performs a simple leader election for a partition.
// It picks the next in-sync replica from ISRNodes and increments LeaderEpoch.
func (m *Manager) ElectLeader(topicName string, partitionIndex int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.persistenceErr != nil {
		return fmt.Errorf("metadata writes fenced until restart: %w", m.persistenceErr)
	}
	topic, ok := m.topics[topicName]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topicName)
	}

	for i := range topic.Partitions {
		p := &topic.Partitions[i]
		if p.Index != partitionIndex {
			continue
		}
		p.mu.Lock()
		defer p.mu.Unlock()

		if len(p.ISRNodes) == 0 {
			return fmt.Errorf("no ISR members for %s-%d", topicName, partitionIndex)
		}

		// Pick first ISR member that is not the current leader
		elected := p.ISRNodes[0]
		for _, id := range p.ISRNodes {
			if id != p.LeaderID {
				elected = id
				break
			}
		}

		if !slices.Contains(p.ReplicaNodes, elected) || p.LeaderEpoch == math.MaxInt32 {
			return fmt.Errorf("invalid leader or exhausted leader epoch for %s-%d", topicName, partitionIndex)
		}
		definition := m.catalog[topicName]
		definition.Partitions = slices.Clone(definition.Partitions)
		definition.Partitions[i].LeaderID = elected
		definition.Partitions[i].LeaderEpoch = p.LeaderEpoch + 1
		next := cloneCatalog(m.catalog)
		next[topicName] = definition
		if err := m.persistCatalog(next); err != nil {
			// A rename may have succeeded before directory synchronization failed.
			p.appendErr = err
			p.signalChange()
			return err
		}
		m.catalog = next
		p.LeaderID = elected
		p.LeaderEpoch++
		p.signalChange()
		return nil
	}

	return fmt.Errorf("partition %d not found in topic %s", partitionIndex, topicName)
}
