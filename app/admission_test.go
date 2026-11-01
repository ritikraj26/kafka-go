package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/network"
)

func TestAdmissionEnvironmentConfiguration(t *testing.T) {
	t.Setenv("NUM_BROKERS", "3")
	t.Setenv("BROKER_ID", "1")
	t.Setenv("REPLICATION_FACTOR", "3")
	t.Setenv("MIN_INSYNC_REPLICAS", "2")
	t.Setenv("REPLICA_LAG_TIME_MAX_MS", "10000")
	for _, tc := range []struct{ key, value string }{
		{"MAX_CONNECTIONS", "0"},
		{"MAX_REQUEST_BYTES", "7"},
		{"MAX_REQUEST_BYTES", "104857601"},
		{"MAX_INFLIGHT_REQUEST_BYTES", "104857599"},
		{"ADMISSION_WAIT_MS", "0"},
		{"ADMISSION_WAIT_MS", "30001"},
		{"MAX_CONNECTIONS", "not-an-integer"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("NUM_BROKERS", "3")
			t.Setenv("BROKER_ID", "1")
			t.Setenv("REPLICATION_FACTOR", "3")
			t.Setenv("MIN_INSYNC_REPLICAS", "2")
			t.Setenv("REPLICA_LAG_TIME_MAX_MS", "10000")
			t.Setenv("MAX_CONNECTIONS", "256")
			t.Setenv("MAX_REQUEST_BYTES", "104857600")
			t.Setenv("MAX_INFLIGHT_REQUEST_BYTES", "268435456")
			t.Setenv("ADMISSION_WAIT_MS", "2000")
			t.Setenv(tc.key, tc.value)
			if _, err := loadConfig(nil); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("invalid admission setting accepted: %v", err)
			}
		})
	}
	t.Setenv("MAX_CONNECTIONS", "8")
	t.Setenv("MAX_REQUEST_BYTES", "1024")
	t.Setenv("MAX_INFLIGHT_REQUEST_BYTES", "4096")
	t.Setenv("ADMISSION_WAIT_MS", "25")
	cfg, err := loadConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := network.Limits{MaxConnections: 8, MaxRequestBytes: 1024, MaxInFlightBytes: 4096, AdmissionWait: 25 * time.Millisecond}
	if cfg.limits != want {
		t.Fatalf("configuration=%+v want %+v", cfg.limits, want)
	}
}

func TestInternalBindFailureClosesAllReservedListeners(t *testing.T) {
	cfg := testRuntimeConfig(t)
	var opened []net.Listener
	injected := errors.New("injected replication bind failure")
	err := runWithConfig(context.Background(), cfg, func(ctx context.Context, network, address string) (net.Listener, error) {
		if len(opened) == 4 {
			return nil, injected
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			opened = append(opened, listener)
		}
		return listener, err
	})
	if !errors.Is(err, injected) || len(opened) != 4 {
		t.Fatalf("internal bind failure was not propagated: %v", err)
	}
	for _, listener := range opened {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Fatal("startup failure leaked a public or internal listener")
		}
	}
}
