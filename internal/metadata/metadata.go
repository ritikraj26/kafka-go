package metadata

import (
	"fmt"
	"sync"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/schema"
)

// Manager holds cluster-level information and topic metadata
type Manager struct {
	mu                sync.RWMutex
	topics            map[string]*Topic // topic_name -> Topic
	logDir            string            // Base directory for log files
	Schemas           *schema.Registry  // AI schema registry (topic -> inferred schema)
	Brokers           *broker.Registry  // Cluster broker registry
	ReplicationFactor int               // Default replication factor for new topics
}

// NewManager creates a new metadata manager
func NewManager() *Manager {
	return &Manager{
		topics:  make(map[string]*Topic),
		logDir:  "/tmp/kraft-broker-logs", // Default
		Schemas: schema.NewRegistry(),
	}
}

// SetLogDir sets the log directory path
func (m *Manager) SetLogDir(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logDir = dir
}

// GetLogDir returns the log directory path
func (m *Manager) GetLogDir() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logDir
}

// BrokerLogDir returns the per-broker log directory: {baseLogDir}/broker-{id}/
func (m *Manager) BrokerLogDir(brokerID int32) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf("%s/broker-%d", m.logDir, brokerID)
}

// PartitionLogDir returns the log directory for a specific partition owned by a broker.
func (m *Manager) PartitionLogDir(brokerID int32, topicName string, partitionIndex int32) string {
	return fmt.Sprintf("%s/%s-%d", m.BrokerLogDir(brokerID), topicName, partitionIndex)
}

// CreateTopic creates a new topic with the given name and number of partitions
func (m *Manager) CreateTopic(name string, numPartitions int) *Topic {
	m.mu.Lock()
	defer m.mu.Unlock()

	var brokerIDs []int32
	if m.Brokers != nil {
		brokerIDs = m.Brokers.IDs()
	}
	topic := NewTopic(name, numPartitions, brokerIDs)
	// Set each partition's LogDir based on its leader, and BrokerLogDirs for all replicas
	for i := range topic.Partitions {
		p := &topic.Partitions[i]
		p.LogDir = fmt.Sprintf("%s/broker-%d/%s-%d", m.logDir, p.LeaderID, name, p.Index)
		for _, brokerID := range p.ReplicaNodes {
			p.BrokerLogDirs[brokerID] = fmt.Sprintf("%s/broker-%d/%s-%d", m.logDir, brokerID, name, p.Index)
		}
	}
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

// GetOrCreateDLQTopic returns the dead-letter-queue topic for the given source
// topic, creating it (with 1 partition) if it does not yet exist.
// The DLQ topic's partition LogDir is set from the manager's logDir so that
// AppendRecords can write to disk immediately.
func (m *Manager) GetOrCreateDLQTopic(sourceTopic string) *Topic {
	dlqName := sourceTopic + ".dlq"
	if t := m.GetTopic(dlqName); t != nil {
		return t
	}
	t := m.CreateTopic(dlqName, 1)
	// Set LogDir for the single partition so disk writes work immediately.
	logDir := m.GetLogDir()
	if logDir != "" && len(t.Partitions) > 0 {
		t.Partitions[0].LogDir = logDir + "/" + dlqName + "-0"
	}
	return t
}

// GetTopicByID retrieves a topic by UUID, returns nil if not found
func (m *Manager) GetTopicByID(id [16]byte) *Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, topic := range m.topics {
		topicIDBytes := topic.ID[:]
		var topicIDArray [16]byte
		copy(topicIDArray[:], topicIDBytes)
		if topicIDArray == id {
			return topic
		}
	}
	return nil
}
