package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOversizedNonFetchResponseClosesWithoutPartialFrame(t *testing.T) {
	mem := DefaultMemoryLimits()
	mem.ResponseMaxBytes = 32
	mem.ResponseBudget = 64
	a, err := NewAdmissionWithMemory(DefaultLimits(), mem)
	if err != nil {
		t.Fatal(err)
	}
	listener := admissionListener(t)
	limitedServer(t, a, testutil.NewMetadataManager(t), listener)
	conn := admissionClient(t, listener)
	if _, err := conn.Write(kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 1)); err != nil {
		t.Fatal(err)
	}
	assertAdmissionClosed(t, conn)
	waitLifecycle(t, func() bool { return a.Stats().Connections == 0 })
	if s := a.Stats(); s.Responses.Used != 0 || s.Responses.Peak > 64 || s.InFlightBytes != 0 || s.Decoded.Used != 0 {
		t.Fatalf("response overflow leaked: %+v", s)
	}
}

func TestNonFetchReservationHeldThroughWrite(t *testing.T) {
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	observed := &observedWriteConn{Conn: server, writing: make(chan struct{})}
	cancel, done := limitedServer(t, a, testutil.NewMetadataManager(t), &pipeListener{conn: observed, closed: make(chan struct{})})
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed.writing:
	case <-time.After(time.Second):
		t.Fatal("response write was not reached")
	}
	if a.Stats().Responses.Used == 0 {
		t.Fatal("response was not charged during blocked delivery")
	}
	cancel()
	<-done
	if a.Stats().Responses.Used != 0 {
		t.Fatal("cancelled response leaked capacity")
	}
}

func TestAcksZeroDoesNotNeedResponseCapacity(t *testing.T) {
	m, p := ackManager(t)
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	held, err := a.responses.Acquire(context.Background(), a.memoryLimits.ResponseBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	listener := admissionListener(t)
	limitedServer(t, a, m, listener)
	conn := admissionClient(t, listener)
	writeProduceWithoutReading(t, conn, ackProduceBody(0, 1000, "events", ackBatch(t)))
	waitLifecycle(t, func() bool { return p.GetLogEndOffset() == 1 && a.Stats().InFlightBytes == 0 })
	if a.Stats().Responses.Waiting != 0 {
		t.Fatal("acks=0 waited for nonexistent response bytes")
	}
}

func TestPendingJoinDoesNotReserveResponseBuffer(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	c.RebalanceDelay = time.Hour
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	listener := admissionListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := ServeWithAdmission(ctx, listener, m, c, 1, a); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		c.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})
	conn := admissionClient(t, listener)
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group, req.ProtocolType, req.SessionTimeoutMillis = "waiting", "consumer", 3600000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
	if _, err := conn.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)); err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, func() bool { g := c.FindGroup("waiting"); return g != nil && g.MemberCount() == 1 })
	if a.Stats().Responses.Used != 0 {
		t.Fatal("blocked join occupied response capacity before encoding")
	}
}
