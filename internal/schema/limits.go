package schema

import (
	"errors"
	"fmt"
	"strings"
)

var ErrQuota = errors.New("schema quota exceeded")

type Limits struct {
	MaxSchemas     int
	MaxBytes       int64
	MaxSchemaBytes int64
	MaxFields      int
	MaxJobs        int
	MaxSampleBytes int
}

func DefaultLimits() Limits {
	return Limits{MaxSchemas: 1000, MaxBytes: 16 << 20, MaxSchemaBytes: 64 << 10, MaxFields: 1024, MaxJobs: 8, MaxSampleBytes: 1 << 20}
}

func (l Limits) Validate() error {
	if l.MaxSchemas < 1 || l.MaxBytes < l.MaxSchemaBytes || l.MaxSchemaBytes < 256 || l.MaxFields < 1 || l.MaxJobs < 1 || l.MaxSampleBytes < 1 {
		return fmt.Errorf("invalid schema limits")
	}
	return nil
}

func (r *Registry) SetLimits(limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || len(r.schemas) > 0 || len(r.inflight) > 0 {
		return fmt.Errorf("configure schema limits before use")
	}
	r.limits = limits
	return nil
}

type Stats struct {
	Schemas, ReservedSchemas, Jobs int
	Bytes                          int64
}

func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Stats{Schemas: len(r.schemas), ReservedSchemas: r.slots - len(r.schemas), Jobs: len(r.inflight), Bytes: r.retained}
}

func schemaCharge(topic string, s *SimpleSchema) (int64, error) {
	if topic == "" || len(topic) > 249 || s == nil {
		return 0, fmt.Errorf("invalid schema or topic")
	}
	cost := int64(256) + 2*int64(len(topic))
	for name, kind := range s.Fields {
		switch kind {
		case TypeString, TypeNumber, TypeInteger, TypeBoolean, TypeObject, TypeArray, TypeNull:
		default:
			return 0, fmt.Errorf("unsupported schema field type (%d bytes)", len(kind))
		}
		cost += 128 + 2*int64(len(name)+len(kind))
	}
	return cost, nil
}

func cloneSchema(s *SimpleSchema) *SimpleSchema {
	if s == nil {
		return nil
	}
	cloned := &SimpleSchema{Fields: make(map[string]FieldType, len(s.Fields))}
	for name, kind := range s.Fields {
		cloned.Fields[strings.Clone(name)] = FieldType(strings.Clone(string(kind)))
	}
	return cloned
}
