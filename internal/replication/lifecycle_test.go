package replication

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestFollowerCancellationInterruptsUnresponsiveLeader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(listener.Addr().(*net.TCPAddr).Port), ReplicationAddress: listener.Addr().String()})
	m.Brokers.Register(&broker.Broker{ID: 2})
	testutil.CreateTopic(t, m, "events", 1)
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fm := NewFollowerManager(m, 2, time.Millisecond)
	if err := fm.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var peer net.Conn
	select {
	case peer = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("follower did not connect")
	}
	defer peer.Close()
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var size int32
	if err := binary.Read(peer, binary.BigEndian, &size); err != nil {
		t.Fatal(err)
	}
	if size <= 0 || size > 1<<20 {
		t.Fatalf("unexpected fetch frame size %d", size)
	}
	if _, err := io.CopyN(io.Discard, peer, int64(size)); err != nil {
		t.Fatal(err)
	}
	// The leader deliberately never sends a response.
	cancel()
	done := make(chan struct{})
	go func() { fm.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("follower waited for its network deadline")
	}
	fm.mu.Lock()
	count := len(fm.conns)
	fm.mu.Unlock()
	if count != 0 {
		t.Fatal("follower retained connections after shutdown")
	}
	if err := fm.Start(context.Background()); err == nil {
		t.Fatal("follower started twice")
	}
}

func TestWorkerStartRejectsInvalidIntervalsAndCancelledContexts(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewFollowerManager(m, 1, time.Second).Start(ctx); err == nil {
		t.Fatal("cancelled follower started")
	}
	if err := NewFollowerManager(m, 1, 0).Start(context.Background()); err == nil {
		t.Fatal("zero follower interval accepted")
	}
	if err := NewISRManager(m, 0, 1000).Start(context.Background()); err == nil {
		t.Fatal("zero ISR interval accepted")
	}
	if err := NewISRManager(m, time.Second, 1000).Start(ctx); err == nil {
		t.Fatal("cancelled ISR worker started")
	}
}
