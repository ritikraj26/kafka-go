package broker

import "sync"

// Broker represents a single Kafka broker node.
type Broker struct {
	ID   int32
	Host string
	Port int32
}

// Registry tracks all known brokers in the cluster.
type Registry struct {
	mu      sync.RWMutex
	brokers map[int32]*Broker
	LocalID int32
}

// NewRegistry creates a registry with the given local broker ID.
func NewRegistry(localID int32) *Registry {
	return &Registry{
		brokers: make(map[int32]*Broker),
		LocalID: localID,
	}
}

// Register adds or updates a broker in the registry.
func (r *Registry) Register(b *Broker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.brokers[b.ID] = b
}

// Get returns a broker by ID, or nil if not found.
func (r *Registry) Get(id int32) *Broker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.brokers[id]
}

// All returns a snapshot of all registered brokers.
func (r *Registry) All() []*Broker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Broker, 0, len(r.brokers))
	for _, b := range r.brokers {
		result = append(result, b)
	}
	return result
}

// IDs returns all broker IDs.
func (r *Registry) IDs() []int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]int32, 0, len(r.brokers))
	for id := range r.brokers {
		ids = append(ids, id)
	}
	return ids
}

// Local returns the local broker.
func (r *Registry) Local() *Broker {
	return r.Get(r.LocalID)
}
