package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func limitedServer(t *testing.T, a *Admission, m *metadata.Manager, listener net.Listener) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	c := coordinator.NewCoordinator()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := ServeWithAdmission(ctx, listener, m, c, 1, a); err != nil {
			t.Errorf("limited server: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		c.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("limited server did not stop")
		}
	})
	return cancel, done
}

func admissionListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func admissionClient(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func readAdmissionResponse(t *testing.T, conn net.Conn, correlation int32) {
	t.Helper()
	var size int32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		t.Fatal(err)
	}
	if size < 4 || size > 1024 {
		t.Fatalf("unexpected response size %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		t.Fatal(err)
	}
	if int32(binary.BigEndian.Uint32(data[:4])) != correlation {
		t.Fatal("wrong response correlation")
	}
}

func assertAdmissionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	n, err := conn.Read(one[:])
	if n != 0 || err == nil {
		t.Fatal("rejected connection returned data or stayed open")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("rejected connection was not closed promptly")
	}
}

func TestConnectionBudgetSharedAcrossListeners(t *testing.T) {
	a := testAdmission(t, 1, 64, time.Second)
	m := testutil.NewMetadataManager(t)
	first, second := admissionListener(t), admissionListener(t)
	limitedServer(t, a, m, first)
	limitedServer(t, a, m, second)
	client := admissionClient(t, first)
	waitLifecycle(t, func() bool { return a.Stats().Connections == 1 })
	rejected := admissionClient(t, second)
	assertAdmissionClosed(t, rejected)
	if stats := a.Stats(); stats.Connections != 1 || stats.PeakConnections != 1 || stats.RejectedConnections != 1 {
		t.Fatalf("shared limit not enforced: %+v", stats)
	}
	client.Close()
	waitLifecycle(t, func() bool { return a.Stats().Connections == 0 })
	replacement := admissionClient(t, second)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 5)
	if _, err := replacement.Write(frame); err != nil {
		t.Fatal(err)
	}
	readAdmissionResponse(t, replacement, 5)
	replacement.Close()
	waitLifecycle(t, func() bool { return a.Stats().Connections == 0 && a.Stats().InFlightBytes == 0 })
}

type frameReadObserver struct {
	net.Conn
	a       *Admission
	size    int64
	total   atomic.Int64
	payload chan struct{}
	once    sync.Once
	invalid atomic.Bool
}

func (c *frameReadObserver) Read(data []byte) (int, error) {
	if c.total.Load() >= 4 && c.total.Load() < 4+c.size {
		if c.a.Stats().InFlightBytes < c.size {
			c.invalid.Store(true)
		}
		c.once.Do(func() { close(c.payload) })
	}
	n, err := c.Conn.Read(data)
	c.total.Add(int64(n))
	return n, err
}

func TestFrameReservationPrecedesPayloadAndSurvivesResponseWrite(t *testing.T) {
	a := testAdmission(t, 1, 64, time.Second)
	held, err := a.Reserve(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	server, client := net.Pipe()
	defer client.Close()
	observed := &frameReadObserver{Conn: server, a: a, size: 64, payload: make(chan struct{})}
	listener := &pipeListener{conn: observed, closed: make(chan struct{})}
	limitedServer(t, a, testutil.NewMetadataManager(t), listener)
	frame := kmsg.NewRequestFormatter(kmsg.FormatterClientID(strings.Repeat("x", 54))).AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 9)
	if len(frame) != 68 {
		t.Fatal("fixture is not a 64-byte frame")
	}
	if _, err := client.Write(frame[:4]); err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
	select {
	case <-observed.payload:
		t.Fatal("payload read started before reservation")
	default:
	}
	if observed.total.Load() != 4 {
		t.Fatal("more than the size prefix was read while queued")
	}
	held()
	select {
	case <-observed.payload:
	case <-time.After(time.Second):
		t.Fatal("capacity release did not resume reading")
	}
	if _, err := client.Write(frame[4:]); err != nil {
		t.Fatal(err)
	}
	if a.Stats().InFlightBytes != 64 {
		t.Fatal("reservation released before response delivery")
	}
	readAdmissionResponse(t, client, 9)
	waitLifecycle(t, func() bool { return a.Stats().InFlightBytes == 0 })
	client.Close()
	waitLifecycle(t, func() bool { return a.Stats().Connections == 0 })
	if observed.invalid.Load() {
		t.Fatal("payload was read without its full reservation")
	}
}

func TestAdmissionTimeoutAndShutdownReleaseWaiters(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		a := testAdmission(t, 1, 64, 30*time.Millisecond)
		held, err := a.Reserve(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		listener := admissionListener(t)
		cancel, done := limitedServer(t, a, testutil.NewMetadataManager(t), listener)
		client := admissionClient(t, listener)
		if err := binary.Write(client, binary.BigEndian, int32(64)); err != nil {
			t.Fatal(err)
		}
		waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
		if shutdown {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown left admission waiters")
			}
		}
		assertAdmissionClosed(t, client)
		waitLifecycle(t, func() bool { return a.Stats().Waiting == 0 && a.Stats().Connections == 0 })
		if a.Stats().InFlightBytes != 64 {
			t.Fatal("server released a different owner's reservation")
		}
		if !shutdown && a.Stats().TimedOutRequests != 1 {
			t.Fatal("admission timeout was not counted")
		}
		held()
		if a.Stats().InFlightBytes != 0 {
			t.Fatal("reservation leaked")
		}
	}
}

func TestFramesReleaseOnReadParseAndWriteFailure(t *testing.T) {
	for _, mode := range []string{"oversized", "partial", "parse", "write"} {
		t.Run(mode, func(t *testing.T) {
			a := testAdmission(t, 1, 64, time.Second)
			server, client := net.Pipe()
			defer client.Close()
			conn := &observedWriteConn{Conn: server, writing: make(chan struct{})}
			listener := &pipeListener{conn: conn, closed: make(chan struct{})}
			limitedServer(t, a, testutil.NewMetadataManager(t), listener)
			switch mode {
			case "oversized":
				if err := binary.Write(client, binary.BigEndian, int32(65)); err != nil {
					t.Fatal(err)
				}
			case "partial":
				if _, err := client.Write(append(binary.BigEndian.AppendUint32(nil, 64), 0, 1)); err != nil {
					t.Fatal(err)
				}
				client.Close()
			case "parse":
				// JoinGroup with a complete common header but no request fields.
				frame := binary.BigEndian.AppendUint32(nil, 10)
				frame = append(frame, 0, 11, 0, 0, 0, 0, 0, 1, 0xff, 0xff)
				if _, err := client.Write(frame); err != nil {
					t.Fatal(err)
				}
			case "write":
				frame := kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 1)
				if _, err := client.Write(frame); err != nil {
					t.Fatal(err)
				}
				select {
				case <-conn.writing:
				case <-time.After(time.Second):
					t.Fatal("response was not attempted")
				}
				if a.Stats().InFlightBytes != int64(len(frame)-4) {
					t.Fatal("write did not retain the frame reservation")
				}
				client.Close()
			}
			waitLifecycle(t, func() bool { return a.Stats().Connections == 0 && a.Stats().InFlightBytes == 0 })
			if mode == "oversized" && a.Stats().PeakInFlightBytes != 0 {
				t.Fatal("oversized frame was reserved or allocated")
			}
		})
	}
}

func TestAcksZeroReleasesFrameBeforeNextRequest(t *testing.T) {
	m, _ := ackManager(t)
	body := ackProduceBody(0, 1000, "events", ackBatch(t))
	size := int64(8 + len(body))
	a := testAdmission(t, 1, size, time.Second)
	listener := admissionListener(t)
	limitedServer(t, a, m, listener)
	client := admissionClient(t, listener)
	writeProduceWithoutReading(t, client, body)
	next := kmsg.NewRequestFormatter().AppendRequest(nil, kmsg.NewPtrApiVersionsRequest(), 202)
	if _, err := client.Write(next); err != nil {
		t.Fatal(err)
	}
	readAdmissionResponse(t, client, 202)
	waitLifecycle(t, func() bool { return a.Stats().InFlightBytes == 0 })
	if a.Stats().PeakInFlightBytes != size {
		t.Fatal("acks=0 accounting did not match the declared frame")
	}
}
