package main

import (
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/schema"
)

func TestStateLimitConfiguration(t *testing.T) {
	t.Setenv("NUM_BROKERS", "3")
	t.Setenv("BROKER_ID", "1")
	t.Setenv("REPLICATION_FACTOR", "3")
	t.Setenv("MIN_INSYNC_REPLICAS", "2")
	t.Setenv("MAX_GROUPS", "1000")
	t.Setenv("MAX_GROUP_STATE_BYTES", "67108864")
	t.Setenv("MAX_GROUP_BYTES", "8388608")
	t.Setenv("MAX_OFFSET_ENTRIES", "100000")
	t.Setenv("MAX_OFFSET_STATE_BYTES", "67108864")
	t.Setenv("MAX_SCHEMAS", "1000")
	t.Setenv("MAX_SCHEMA_STATE_BYTES", "16777216")
	t.Setenv("MAX_RESPONSE_BYTES", "16777216")
	t.Setenv("MAX_INFLIGHT_RESPONSE_BYTES", "134217728")
	cfg, err := loadConfig(nil)
	if err != nil || cfg.stateLimits != coordinator.DefaultStateLimits() || cfg.schemaLimits != schema.DefaultLimits() {
		t.Fatalf("unexpected retained-state configuration: %+v %v", cfg, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"MAX_GROUPS", "0"}, {"MAX_GROUP_BYTES", "1023"}, {"MAX_GROUP_STATE_BYTES", "8388607"},
		{"MAX_OFFSET_ENTRIES", "0"}, {"MAX_OFFSET_STATE_BYTES", "511"},
		{"MAX_SCHEMAS", "0"}, {"MAX_SCHEMA_STATE_BYTES", "65535"},
		{"MAX_RESPONSE_BYTES", "0"}, {"MAX_INFLIGHT_RESPONSE_BYTES", "33554431"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := loadConfig(nil); err == nil {
				t.Fatalf("invalid %s accepted", tc.key)
			}
		})
	}
}
