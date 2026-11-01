package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ritiraj/kafka-go/internal/logger"
)

// FieldType represents the JSON type of a field.
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeNumber  FieldType = "number"
	TypeInteger FieldType = "integer"
	TypeBoolean FieldType = "boolean"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeNull    FieldType = "null"
)

// SimpleSchema is a flat map of top-level field names to their expected type.
type SimpleSchema struct {
	Fields map[string]FieldType
}

// Registry is a thread-safe store of topic → SimpleSchema.
type Registry struct {
	mu       sync.RWMutex
	schemas  map[string]*SimpleSchema
	inflight map[string]*inferenceJob
	ctx      context.Context
	cancel   context.CancelFunc
	closed   bool
	done     chan struct{}
	workers  sync.WaitGroup
	limits   Limits
	retained int64
	slots    int
}

type inferenceJob struct {
	cancel   context.CancelFunc
	reserved bool
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{schemas: make(map[string]*SimpleSchema), inflight: make(map[string]*inferenceJob), limits: DefaultLimits(), ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// Register stores a schema for a topic. Overwrites any existing schema.
func (r *Registry) Register(topic string, s *SimpleSchema) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.register(topic, s)
	if err != nil {
		logger.L.Warn("schema registration rejected", "topic", topic, "err", err)
	}
	return err
}

func (r *Registry) register(topic string, s *SimpleSchema) error {
	if r.closed {
		return context.Canceled
	}
	if s == nil {
		return fmt.Errorf("nil schema")
	}
	if len(s.Fields) > r.limits.MaxFields {
		return ErrQuota
	}
	cost, err := schemaCharge(topic, s)
	if err != nil {
		return err
	}
	if len(s.Fields) > r.limits.MaxFields || cost > r.limits.MaxSchemaBytes {
		return ErrQuota
	}
	var old int64
	newSlot := true
	if previous := r.schemas[topic]; previous != nil {
		old, _ = schemaCharge(topic, previous)
		newSlot = false
	} else if job := r.inflight[topic]; job != nil && job.reserved {
		old = r.limits.MaxSchemaBytes
		newSlot = false
	}
	if (newSlot && r.slots >= r.limits.MaxSchemas) || cost-old > r.limits.MaxBytes-r.retained {
		return ErrQuota
	}
	owned := cloneSchema(s)
	r.schemas[strings.Clone(topic)] = owned
	r.retained += cost - old
	if newSlot {
		r.slots++
	}
	if job := r.inflight[topic]; job != nil {
		job.reserved = false
		job.cancel()
	}
	return nil
}

// Get returns the schema for a topic, or nil if none is registered.
func (r *Registry) Get(topic string) *SimpleSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneSchema(r.schemas[topic])
}

func (r *Registry) Has(topic string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[topic] != nil
}

// Validate checks that payload is valid JSON whose top-level fields match the schema.
// Returns nil if no schema is registered yet (first-message case — caller handles inference).
func (r *Registry) Validate(topic string, payload []byte) error {
	r.mu.RLock()
	s := r.schemas[topic]
	r.mu.RUnlock()
	if s == nil {
		return nil
	}

	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return fmt.Errorf("not valid JSON: %w", err)
	}

	for field, expectedType := range s.Fields {
		val, ok := m[field]
		if !ok {
			return fmt.Errorf("missing required field %q", field)
		}
		if err := checkType(field, val, expectedType); err != nil {
			return err
		}
	}
	return nil
}

// InferFromJSON derives a SimpleSchema from a raw JSON object payload.
// Used as a local fallback when Ollama is unavailable.
func InferFromJSON(payload []byte) (*SimpleSchema, error) {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("not valid JSON: %w", err)
	}
	fields := make(map[string]FieldType, len(m))
	for k, v := range m {
		fields[k] = jsonTypeOf(v)
	}
	return &SimpleSchema{Fields: fields}, nil
}

func jsonTypeOf(v any) FieldType {
	switch v.(type) {
	case string:
		return TypeString
	case bool:
		return TypeBoolean
	case []any:
		return TypeArray
	case map[string]any:
		return TypeObject
	case nil:
		return TypeNull
	default:
		if n, ok := v.(float64); ok {
			if n == float64(int64(n)) {
				return TypeInteger
			}
			return TypeNumber
		}
		return TypeNumber
	}
}

func checkType(field string, val any, expected FieldType) error {
	actual := jsonTypeOf(val)
	if expected == TypeNumber && actual == TypeInteger {
		return nil
	}
	if actual != expected {
		return fmt.Errorf("field %q: expected %s, got %s", field, expected, actual)
	}
	return nil
}

// InferAsync kicks off schema inference in a background goroutine.
// It reserves a schema slot and worst-case schema bytes before starting work.
func (r *Registry) InferAsync(topic string, payload []byte) error {
	if topic == "" || len(topic) > 249 {
		logger.L.Warn("invalid inference topic", "topic", topic)
		return fmt.Errorf("invalid inference topic")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		logger.L.Debug("schema inference skipped after shutdown", "topic", topic)
		return context.Canceled
	}
	if r.schemas[topic] != nil || r.inflight[topic] != nil {
		r.mu.Unlock()
		return nil
	}
	if len(payload) > r.limits.MaxSampleBytes || len(r.inflight) >= r.limits.MaxJobs || r.slots >= r.limits.MaxSchemas ||
		r.limits.MaxSchemaBytes > r.limits.MaxBytes-r.retained {
		r.mu.Unlock()
		logger.L.Warn("optional inference skipped at resource limit", "topic", topic)
		return ErrQuota
	}
	ctx, cancel := context.WithCancel(r.ctx)
	job := &inferenceJob{cancel: cancel, reserved: true}
	r.inflight[strings.Clone(topic)] = job
	r.retained += r.limits.MaxSchemaBytes
	r.slots++
	r.workers.Add(1)
	r.mu.Unlock()
	// Copy payload to prevent races on the caller's buffer.
	buf := make([]byte, len(payload))
	copy(buf, payload)
	go func() {
		defer r.workers.Done()
		defer func() {
			cancel()
			r.mu.Lock()
			defer r.mu.Unlock()
			if job.reserved {
				r.retained -= r.limits.MaxSchemaBytes
				r.slots--
			}
			delete(r.inflight, topic)
		}()
		s, err := InferWithOllamaContext(ctx, buf)
		if ctx.Err() != nil {
			return
		}
		if err != nil || s == nil {
			logger.L.Debug("using local schema inference", "topic", topic, "err", err)
			s, err = InferFromJSON(buf)
		}
		if s != nil && err == nil {
			r.mu.Lock()
			if !r.closed && r.schemas[topic] == nil {
				err = r.register(topic, s)
			}
			r.mu.Unlock()
			if err != nil {
				logger.L.Warn("inferred schema rejected", "topic", topic, "err", err)
			}
		} else if err != nil {
			logger.L.Debug("schema inference unavailable", "topic", topic, "err", err)
		}
	}()
	return nil
}

// Close cancels HTTP inference and joins every owned worker. No new inference
// can be admitted once closing starts.
func (r *Registry) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		<-r.done
		return
	}
	r.closed = true
	r.cancel()
	r.mu.Unlock()
	r.workers.Wait()
	r.mu.Lock()
	r.schemas = make(map[string]*SimpleSchema)
	r.retained = 0
	r.slots = 0
	r.mu.Unlock()
	close(r.done)
}
