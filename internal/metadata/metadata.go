package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/memory"
	"github.com/ritiraj/kafka-go/internal/schema"
)

// Manager holds cluster-level information and topic metadata
type Manager struct {
	mu                   sync.RWMutex
	topics               map[string]*Topic // topic_name -> Topic
	logDir               string            // Base directory for log files
	loaded               bool
	catalog              map[string]topicMetadata
	persistenceErr       error
	replicationResponses *memory.Pool
	Schemas              *schema.Registry // AI schema registry (topic -> inferred schema)
	Brokers              *broker.Registry // Cluster broker registry
	ReplicationFactor    int              // Default replication factor for new topics
	MinInSyncReplicas    int              // Broker-wide acks=-1 policy; configure before starting workers
}

// NewManager creates a new metadata manager
func NewManager() *Manager {
	return &Manager{
		topics:            make(map[string]*Topic),
		catalog:           make(map[string]topicMetadata),
		logDir:            "/tmp/kraft-broker-logs", // Default
		Schemas:           schema.NewRegistry(),
		MinInSyncReplicas: DefaultMinInSyncReplicas,
	}
}

// SetLogDir sets the log directory path
func (m *Manager) SetLogDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setLogDir(dir)
}

func (m *Manager) setLogDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("log directory must not be empty")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if m.loaded && absolute != m.logDir {
		return fmt.Errorf("cannot change log directory after metadata initialization")
	}
	m.logDir = absolute
	return nil
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

// CreateTopic persists the definition before exposing the topic to callers.
// Repeating the same creation returns the existing topic without replacing it.
func (m *Manager) CreateTopic(name string, numPartitions int) (*Topic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateTopicName(name); err != nil {
		return nil, err
	}
	if numPartitions < 1 || numPartitions > maxTopicPartitions {
		return nil, fmt.Errorf("partition count must be between 1 and %d", maxTopicPartitions)
	}
	if m.persistenceErr != nil {
		return nil, fmt.Errorf("metadata writes fenced until restart: %w", m.persistenceErr)
	}
	if !m.loaded {
		if err := m.loadTopics(); err != nil {
			return nil, err
		}
	}
	if topic := m.topics[name]; topic != nil {
		if len(topic.Partitions) != numPartitions {
			return nil, fmt.Errorf("topic %s already exists with %d partitions", name, len(topic.Partitions))
		}
		return topic, nil
	}
	var brokerIDs []int32
	if m.Brokers != nil {
		brokerIDs = m.Brokers.IDs()
		if len(brokerIDs) == 0 {
			return nil, fmt.Errorf("cannot create a topic with an empty broker registry")
		}
	} else {
		brokerIDs = []int32{1}
	}
	slices.Sort(brokerIDs)
	for _, id := range brokerIDs {
		if id <= 0 {
			return nil, fmt.Errorf("invalid broker ID %d", id)
		}
	}
	rf := m.ReplicationFactor
	if rf == 0 {
		rf = len(brokerIDs)
	}
	if rf < 1 || rf > len(brokerIDs) {
		return nil, fmt.Errorf("replication factor %d exceeds available brokers or is invalid", rf)
	}
	topic := newTopic(name, numPartitions, brokerIDs, rf)
	definition := topicMetadata{Name: name, ID: topic.ID, Partitions: make([]partitionMetadata, numPartitions)}
	for i := range topic.Partitions {
		p := &topic.Partitions[i]
		d := partitionMetadata{Index: p.Index, LeaderID: p.LeaderID, Replicas: slices.Clone(p.ReplicaNodes), LogDirs: make(map[int32]string)}
		for _, id := range p.ReplicaNodes {
			relative := filepath.Join(fmt.Sprintf("broker-%d", id), fmt.Sprintf("%s-%d", name, p.Index))
			d.LogDirs[id] = relative
			p.BrokerLogDirs[id] = filepath.Join(m.logDir, relative)
			if _, err := os.Lstat(p.BrokerLogDirs[id]); !os.IsNotExist(err) {
				if err == nil {
					err = fmt.Errorf("partition directory already exists: %s", relative)
				}
				m.persistenceErr = err
				return nil, err
			}
			if err := initializeLog(p.BrokerLogDirs[id]); err != nil {
				m.persistenceErr = err
				return nil, fmt.Errorf("initialize %s: %w", relative, err)
			}
		}
		definition.Partitions[i] = d
	}
	next := cloneCatalog(m.catalog)
	next[name] = definition
	if err := m.persistCatalog(next); err != nil {
		m.persistenceErr = err
		return nil, err
	}
	m.catalog = next
	m.topics[name] = topic
	return topic, nil
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
func (m *Manager) GetOrCreateDLQTopic(sourceTopic string) (*Topic, error) {
	return m.CreateTopic(sourceTopic+".dlq", 1)
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
