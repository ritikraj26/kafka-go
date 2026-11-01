package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/network"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func testRuntimeConfig(t *testing.T) runtimeConfig {
	t.Helper()
	return runtimeConfig{brokerID: 2, numBrokers: 3, replicationFactor: 3, minISR: 2, lagTimeMaxMs: 10000, logDir: t.TempDir(), limits: network.DefaultLimits(), memoryLimits: network.DefaultMemoryLimits(), stateLimits: coordinator.DefaultStateLimits(), schemaLimits: schema.DefaultLimits()}
}

type countedListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *countedListener) Accept() (net.Conn, error) {
	l.accepts.Add(1)
	return l.Listener.Accept()
}

func TestPartialBindFailureClosesReservedListeners(t *testing.T) {
	cfg := testRuntimeConfig(t)
	var first *countedListener
	calls := 0
	injected := errors.New("injected second listener failure")
	err := runWithConfig(context.Background(), cfg, func(ctx context.Context, network, address string) (net.Listener, error) {
		calls++
		if calls == 2 {
			return nil, injected
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		first = &countedListener{Listener: listener}
		return first, nil
	})
	if !errors.Is(err, injected) || calls != 2 || first == nil {
		t.Fatalf("bind failure not propagated: %v", err)
	}
	if first.accepts.Load() != 0 {
		t.Fatal("partial startup began accepting requests")
	}
	conn, dialErr := net.DialTimeout("tcp", first.Addr().String(), 100*time.Millisecond)
	if dialErr == nil {
		conn.Close()
		t.Fatal("failed startup left an open listener")
	}
}

func runtimeConnection(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if _, err := conn.Write(kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 42)); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	var size int32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if size < 4 || size > 1024 {
		conn.Close()
		t.Fatalf("invalid readiness response size %d", size)
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(frame[:4]) != 42 {
		conn.Close()
		t.Fatal("unexpected response correlation")
	}
	response := kmsg.NewPtrApiVersionsResponse()
	if err := response.ReadFrom(frame[4:]); err != nil || response.ErrorCode != 0 {
		conn.Close()
		t.Fatalf("runtime is not responsive: %v", err)
	}
	return conn
}

func TestRuntimeJoinsAllBrokersOnCancellationAndListenerFailure(t *testing.T) {
	for _, failListener := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "listener error"}[failListener], func(t *testing.T) {
			cfg := testRuntimeConfig(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bound := make(chan net.Listener, 3)
			addresses := make(chan string, 3)
			result := make(chan error, 1)
			go func() {
				result <- runWithConfig(ctx, cfg, func(ctx context.Context, network, address string) (net.Listener, error) {
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err == nil && address != "127.0.0.1:0" {
						bound <- listener
						addresses <- address
					}
					return listener, err
				})
			}()
			var listeners []net.Listener
			var clients []net.Conn
			for i := 0; i < 3; i++ {
				select {
				case listener := <-bound:
					listeners = append(listeners, listener)
					conn := runtimeConnection(t, listener.Addr().String())
					clients = append(clients, conn)
					defer conn.Close()
				case <-time.After(3 * time.Second):
					t.Fatal("runtime did not bind every broker")
				}
			}
			for _, want := range []string{"0.0.0.0:9092", "0.0.0.0:9093", "0.0.0.0:9094"} {
				if got := <-addresses; got != want {
					t.Fatalf("BROKER_ID=2 caused wrong/duplicate port assignment: %s", got)
				}
			}
			if failListener {
				listeners[1].Close()
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if failListener && err == nil {
					t.Fatal("unexpected accept error was swallowed")
				}
				if !failListener && err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("runtime did not join listeners and workers")
			}
			for _, conn := range clients {
				if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				var b [1]byte
				if n, err := conn.Read(b[:]); n != 0 || err != io.EOF {
					t.Fatalf("active client was not closed: n=%d err=%v", n, err)
				}
			}
		})
	}
}

func TestRuntimeConfigurationRejectsSilentDefaults(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"NUM_BROKERS", "0"}, {"NUM_BROKERS", "999999"}, {"BROKER_ID", "4"},
		{"REPLICATION_FACTOR", "0"}, {"REPLICATION_FACTOR", "4"}, {"REPLICA_LAG_TIME_MAX_MS", "bad"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("NUM_BROKERS", "3")
			t.Setenv("BROKER_ID", "1")
			t.Setenv("REPLICATION_FACTOR", "3")
			t.Setenv("REPLICA_LAG_TIME_MAX_MS", "10000")
			t.Setenv("MIN_INSYNC_REPLICAS", "2")
			t.Setenv(tc.key, tc.value)
			if _, err := loadConfig(nil); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("invalid %s accepted: %v", tc.key, err)
			}
		})
	}
	if _, err := loadConfig([]string{t.TempDir() + "/missing.properties"}); err == nil {
		t.Fatal("missing config file silently used defaults")
	}
}
