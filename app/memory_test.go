package main

import (
	"testing"

	"github.com/ritiraj/kafka-go/internal/network"
)

func TestMemoryEnvironmentConfiguration(t *testing.T) {
	t.Setenv("NUM_BROKERS", "3")
	t.Setenv("BROKER_ID", "1")
	t.Setenv("REPLICATION_FACTOR", "3")
	t.Setenv("MIN_INSYNC_REPLICAS", "2")
	t.Setenv("REPLICA_LAG_TIME_MAX_MS", "10000")
	t.Setenv("MAX_CONNECTIONS", "256")
	t.Setenv("MAX_REQUEST_BYTES", "104857600")
	t.Setenv("MAX_INFLIGHT_REQUEST_BYTES", "268435456")
	t.Setenv("ADMISSION_WAIT_MS", "2000")
	t.Setenv("MAX_DECODED_REQUEST_BYTES", "16777216")
	t.Setenv("MAX_INFLIGHT_DECODE_BYTES", "134217728")
	t.Setenv("MAX_REQUEST_ENTRIES", "10000")
	t.Setenv("FETCH_MAX_BYTES", "16777216")
	t.Setenv("MAX_INFLIGHT_FETCH_BYTES", "536870912")
	cfg, err := loadConfig(nil)
	if err != nil || cfg.memoryLimits != network.DefaultMemoryLimits() {
		t.Fatalf("memory defaults: %+v %v", cfg.memoryLimits, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"MAX_DECODED_REQUEST_BYTES", "1023"},
		{"MAX_INFLIGHT_DECODE_BYTES", "16777215"},
		{"MAX_REQUEST_ENTRIES", "0"},
		{"MAX_REQUEST_ENTRIES", "100001"},
		{"FETCH_MAX_BYTES", "0"},
		{"FETCH_MAX_BYTES", "104857601"},
		{"MAX_INFLIGHT_FETCH_BYTES", "104857600"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := loadConfig(nil); err == nil {
				t.Fatalf("invalid %s accepted", tc.key)
			}
		})
	}
}
