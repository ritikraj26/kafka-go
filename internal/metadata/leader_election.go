package metadata

import "fmt"

// ElectLeader performs a simple leader election for a partition.
// It picks the next in-sync replica from ISRNodes and increments LeaderEpoch.
func (m *Manager) ElectLeader(topicName string, partitionIndex int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, ok := m.topics[topicName]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topicName)
	}

	for i := range topic.Partitions {
		p := &topic.Partitions[i]
		if p.Index != partitionIndex {
			continue
		}

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

		p.LeaderID = elected
		p.LeaderEpoch++
		return nil
	}

	return fmt.Errorf("partition %d not found in topic %s", partitionIndex, topicName)
}
