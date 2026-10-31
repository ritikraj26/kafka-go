package network

import (
	"context"
	"errors"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func waitLifecycle(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("lifecycle precondition was not reached")
}

func TestServeCancelsConnectionsAndBlockedRequests(t *testing.T) {
	for _, mode := range []string{"idle", "partial frame", "fetch", "produce", "join"} {
		t.Run(mode, func(t *testing.T) {
			m := testutil.NewMetadataManager(t)
			m.Brokers = broker.NewRegistry(1)
			m.Brokers.Register(&broker.Broker{ID: 1})
			m.Brokers.Register(&broker.Broker{ID: 2})
			p := &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
			m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
			c := coordinator.NewCoordinator()
			c.RebalanceDelay = time.Hour
			defer c.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- Serve(ctx, listener, m, c, 1) }()
			client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "partial frame":
				if _, err := client.Write([]byte{0, 0}); err != nil {
					t.Fatal(err)
				}
			case "fetch":
				req := kmsg.NewPtrFetchRequest()
				req.Version, req.MaxWaitMillis, req.MinBytes, req.MaxBytes = 16, math.MaxInt32, 1, 1024
				part := kmsg.NewFetchRequestTopicPartition()
				part.PartitionMaxBytes = 1024
				req.Topics = []kmsg.FetchRequestTopic{{TopicID: m.GetTopic("events").ID, Partitions: []kmsg.FetchRequestTopicPartition{part}}}
				if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)); err != nil {
					t.Fatal(err)
				}
			case "produce":
				writeProduceWithoutReading(t, client, ackProduceBody(-1, math.MaxInt32, "events", ackBatch(t)))
				waitLifecycle(t, func() bool { return p.GetLogEndOffset() == 1 })
			case "join":
				req := kmsg.NewPtrJoinGroupRequest()
				req.Group, req.SessionTimeoutMillis, req.ProtocolType = "waiting", math.MaxInt32, "consumer"
				req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
				if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)); err != nil {
					t.Fatal(err)
				}
				waitLifecycle(t, func() bool { g := c.FindGroup("waiting"); return g != nil && g.MemberCount() == 1 })
			}
			cancel()
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown waited for client-controlled timeout")
			}
			var b [1]byte
			if n, err := client.Read(b[:]); n != 0 || err == nil {
				t.Fatal("cancelled connection remained open or received success bytes")
			}
			if mode == "produce" && p.GetLogEndOffset() != 1 {
				t.Fatal("cancellation rolled back an existing append")
			}
		})
	}
}

type pipeListener struct {
	conn     net.Conn
	mu       sync.Mutex
	accepted bool
	closed   chan struct{}
	once     sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.accepted {
		l.accepted = true
		l.mu.Unlock()
		return l.conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

type observedWriteConn struct {
	net.Conn
	writing       chan struct{}
	once          sync.Once
	clampDeadline bool
	deadline      chan time.Time
}

func (c *observedWriteConn) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(data)
}
func (c *observedWriteConn) SetWriteDeadline(deadline time.Time) error {
	if c.deadline != nil {
		c.deadline <- deadline
	}
	if c.clampDeadline {
		deadline = time.Now().Add(20 * time.Millisecond)
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func TestServeCancelsBlockedResponseWrite(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	conn := &observedWriteConn{Conn: server, writing: make(chan struct{})}
	listener := &pipeListener{conn: conn, closed: make(chan struct{})}
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, listener, m, c, 1) }()
	request := kmsg.NewPtrApiVersionsRequest()
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.writing:
	case <-time.After(time.Second):
		t.Fatal("handler did not reach blocked response write")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked write prevented shutdown")
	}
}

func TestConnectionAppliesWriteDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	conn := &observedWriteConn{Conn: server, writing: make(chan struct{}), clampDeadline: true, deadline: make(chan time.Time, 1)}
	c := coordinator.NewCoordinator()
	defer c.Close()
	m := testutil.NewMetadataManager(t)
	done := make(chan struct{})
	go func() { defer close(done); handleConnection(context.Background(), conn, m, c, 1) }()
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case deadline := <-conn.deadline:
		if deadline.IsZero() || time.Until(deadline) <= 0 || time.Until(deadline) > connectionWriteTimeout {
			t.Fatal("invalid response deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("no write deadline was set")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler ignored write timeout")
	}
}

type failingListener struct{ calls int }

func (l *failingListener) Accept() (net.Conn, error) {
	l.calls++
	return nil, errors.New("injected accept failure")
}
func (l *failingListener) Close() error   { return nil }
func (l *failingListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestServeReturnsAcceptErrorWithoutSpinning(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	defer c.Close()
	listener := &failingListener{}
	if err := Serve(context.Background(), listener, m, c, 1); err == nil {
		t.Fatal("accept failure was swallowed")
	}
	if listener.calls != 1 {
		t.Fatalf("accept loop spun %d times", listener.calls)
	}
}

func TestStartOnPortReturnsBindFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := StartOnPort(ctx, int32(occupied.Addr().(*net.TCPAddr).Port), m, c, 1); err == nil {
		t.Fatal("bind failure returned success")
	}
}
