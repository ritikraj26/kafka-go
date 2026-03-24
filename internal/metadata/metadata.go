package metadata

import (
	"sync"
)

// Manager holds cluster-level information and topic metadata
type Manager struct {
	mu     sync.RWMutex
	topics map[string]*Topic // topic_name -> Topic
}

// NewManager creates a new metadata manager
func NewManager() *Manager {
	return &Manager{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic creates a new topic with the given name and number of partitions
func (m *Manager) CreateTopic(name string, numPartitions int) *Topic {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic := NewTopic(name, numPartitions)
	m.topics[name] = topic
	return topic
}

// GetTopic retrieves a topic by name, returns nil if not found
func (m *Manager) GetTopic(name string) *Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.topics[name]
}

// ListTopics returns all topic names
func (m *Manager) ListTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.topics))
	for name := range m.topics {
		names = append(names, name)
	}
	return names
}

// TopicExists checks if a topic exists
func (m *Manager) TopicExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.topics[name]
	return exists
}
