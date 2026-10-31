package controller

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestControllerLifecycleAndCancelledProbes(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	reg := broker.NewRegistry(1)
	reg.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: 1})
	ctrl := NewController(m, reg, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	if err := ctrl.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx); err == nil {
		t.Fatal("controller started twice")
	}
	cancel()
	done := make(chan struct{})
	go func() { ctrl.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controller did not join probes")
	}
	if !ctrl.IsAlive(1) {
		t.Fatal("shutdown probes declared a broker dead")
	}
	if ctrl.probe(ctx, &broker.Broker{Host: "::1", Port: 1}) {
		t.Fatal("cancelled probe succeeded")
	}
	if err := NewController(m, reg, 0).Start(context.Background()); err == nil {
		t.Fatal("zero controller interval accepted")
	}
}

func TestControllerProbeSupportsIPv6(t *testing.T) {
	listener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer listener.Close()
	m := testutil.NewMetadataManager(t)
	ctrl := NewController(m, broker.NewRegistry(1), time.Second)
	if !ctrl.probe(context.Background(), &broker.Broker{Host: "::1", Port: int32(listener.Addr().(*net.TCPAddr).Port)}) {
		t.Fatal("IPv6 listener was not reachable through controller probe")
	}
}
