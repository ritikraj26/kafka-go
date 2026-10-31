package schema

import (
	"context"
	"encoding/json"
	"fmt"
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
	inflight sync.Map // topic → struct{}: deduplicates in-progress async inferences
	ctx      context.Context
	cancel   context.CancelFunc
	closed   bool
	done     chan struct{}
	workers  sync.WaitGroup
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{schemas: make(map[string]*SimpleSchema), ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// Register stores a schema for a topic. Overwrites any existing schema.
func (r *Registry) Register(topic string, s *SimpleSchema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[topic] = s
}

// Get returns the schema for a topic, or nil if none is registered.
func (r *Registry) Get(topic string) *SimpleSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemas[topic]
}

// Validate checks that payload is valid JSON whose top-level fields match the schema.
// Returns nil if no schema is registered yet (first-message case — caller handles inference).
func (r *Registry) Validate(topic string, payload []byte) error {
	s := r.Get(topic)
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
// Uses sync.Map to ensure only one inference runs per topic.
func (r *Registry) InferAsync(topic string, payload []byte) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		logger.L.Debug("schema inference skipped after shutdown", "topic", topic)
		return
	}
	if _, loaded := r.inflight.LoadOrStore(topic, struct{}{}); loaded {
		r.mu.Unlock()
		return // inference already in progress
	}
	r.workers.Add(1)
	r.mu.Unlock()
	// Copy payload to prevent races on the caller's buffer.
	buf := make([]byte, len(payload))
	copy(buf, payload)
	go func() {
		defer r.workers.Done()
		defer r.inflight.Delete(topic)
		s, err := InferWithOllamaContext(r.ctx, buf)
		if r.ctx.Err() != nil {
			return
		}
		if err != nil || s == nil {
			logger.L.Debug("using local schema inference", "topic", topic, "err", err)
			s, err = InferFromJSON(buf)
		}
		if s != nil && err == nil {
			r.mu.Lock()
			if !r.closed {
				r.schemas[topic] = s
			}
			r.mu.Unlock()
		} else if err != nil {
			logger.L.Debug("schema inference unavailable", "topic", topic, "err", err)
		}
	}()
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
	close(r.done)
}
