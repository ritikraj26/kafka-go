package metadata

import (
	"time"

	"github.com/ritiraj/kafka-go/internal/memory"
)

// ReplicationResponseBudget is separate from server-side response admission:
// receiving a response must not wait for capacity held by its sender.
func (m *Manager) ReplicationResponseBudget() (*memory.Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.replicationResponses == nil {
		waiters := 1
		if m.Brokers != nil {
			waiters = max(1, len(m.Brokers.IDs()))
		}
		pool, err := memory.NewPool(256<<20, waiters, 2*time.Second)
		if err != nil {
			return nil, err
		}
		m.replicationResponses = pool
	}
	return m.replicationResponses, nil
}
