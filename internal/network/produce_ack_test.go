package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/replication"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/storage"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func ackBatch(t *testing.T) []byte {
	t.Helper()
	data, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: []byte(`{"value":"record"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func ackProduceBody(acks int16, timeout int32, topic string, batch []byte) []byte {
	e := protocol.NewEncoder()
	e.WriteString("ack-test")
	e.WriteTagBuffer()
	e.WriteByte(0)
	e.WriteInt16(acks)
	e.WriteInt32(timeout)
	e.WriteUnsignedVarint(2)
	e.WriteCompactString(topic)
	e.WriteUnsignedVarint(2)
	e.WriteInt32(0)
	e.WriteCompactBytes(batch)
	e.WriteTagBuffer()
	e.WriteTagBuffer()
	e.WriteTagBuffer()
	return e.Bytes()
}

func writeProduceWithoutReading(t *testing.T, conn net.Conn, body []byte) {
	t.Helper()
	e := protocol.NewEncoder()
	e.WriteInt16(protocol.APIKeyProduce)
	e.WriteInt16(11)
	e.WriteInt32(101)
	message := append(e.Bytes(), body...)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(message)))
	frame = append(frame, message...)
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
}

func ackResponseCode(t *testing.T, body []byte) int16 {
	t.Helper()
	d := protocol.NewDecoder(body)
	if err := d.ReadTaggedFields(nil); err != nil {
		t.Fatal(err)
	}
	if n, err := d.ReadUnsignedVarint(); err != nil || n != 2 {
		t.Fatalf("topic count=%d err=%v", n, err)
	}
	if _, err := d.ReadCompactString(); err != nil {
		t.Fatal(err)
	}
	if n, err := d.ReadUnsignedVarint(); err != nil || n != 2 {
		t.Fatalf("partition count=%d err=%v", n, err)
	}
	if _, err := d.ReadInt32(); err != nil {
		t.Fatal(err)
	}
	code, err := d.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func ackManager(t *testing.T) (*metadata.Manager, *metadata.Partition) {
	t.Helper()
	m := testutil.NewMetadataManager(t)
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	return m, &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
}

func TestIntegration_AcksZeroHasNoResponseAndKeepsConnection(t *testing.T) {
	m, p := ackManager(t)
	conn := recoveryConnection(t, m)
	writeProduceWithoutReading(t, conn, ackProduceBody(0, 1000, "events", ackBatch(t)))
	// A second request is a processing barrier: any unsolicited Produce response
	// would be read first and fail sendV0Request's correlation-ID assertion.
	e := protocol.NewEncoder()
	e.WriteString("ack-test")
	e.WriteInt32(-1)
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteArrayLen(1)
	e.WriteString("events")
	e.WriteArrayLen(1)
	e.WriteInt32(0)
	e.WriteInt64(0)
	e.WriteInt32(1024)
	sendV0Request(t, conn, protocol.APIKeyFetch, 0, 202, e.Bytes())
	if p.GetLogEndOffset() != 1 {
		t.Fatal("acks=0 suppressed the write, not just its response")
	}
}

func TestIntegration_AcksZeroFailureClosesWithoutResponse(t *testing.T) {
	for _, scenario := range []string{"unknown topic", "invalid record", "negative timeout"} {
		t.Run(scenario, func(t *testing.T) {
			m, p := ackManager(t)
			conn := recoveryConnection(t, m)
			topic, batch, timeout := "events", ackBatch(t), int32(1000)
			switch scenario {
			case "unknown topic":
				topic = "missing"
			case "invalid record":
				batch = []byte{1, 2}
			case "negative timeout":
				timeout = -1
			}
			writeProduceWithoutReading(t, conn, ackProduceBody(0, timeout, topic, batch))
			if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			var b [1]byte
			if n, err := conn.Read(b[:]); n != 0 || err != io.EOF {
				t.Fatalf("expected connection close with no response bytes, got n=%d err=%v", n, err)
			}
			if p.GetLogEndOffset() != 0 {
				t.Fatal("failed acks=0 request appended data")
			}
		})
	}
}

func TestIntegration_ProduceAckErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		acks   int16
		code   int16
		writes int64
	}{
		{2, protocol.ErrInvalidRequiredAcks, 0},
		{-1, protocol.ErrNotEnoughReplicas, 0},
		{1, protocol.ErrNone, 1},
	} {
		m, p := ackManager(t)
		conn := recoveryConnection(t, m)
		response := sendV0Request(t, conn, protocol.APIKeyProduce, 11, 42,
			ackProduceBody(tc.acks, 1000, "events", ackBatch(t)))
		if got := ackResponseCode(t, response); got != tc.code {
			t.Fatalf("acks=%d code=%d want %d", tc.acks, got, tc.code)
		}
		if p.GetLogEndOffset() != tc.writes {
			t.Fatalf("acks=%d wrote %d records, want %d", tc.acks, p.GetLogEndOffset(), tc.writes)
		}
	}
}

func TestIntegration_ISRDropAfterAppendReturnsDistinctError(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1})
	m.Brokers.Register(&broker.Broker{ID: 2})
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	p := &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
	conn := recoveryConnection(t, m)
	writeProduceWithoutReading(t, conn, ackProduceBody(-1, 2000, "events", ackBatch(t)))
	deadline := time.Now().Add(time.Second)
	for p.GetLogEndOffset() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.GetLogEndOffset() != 1 {
		t.Fatal("produce did not append before waiting")
	}
	if err := p.SetReplicaOnline(2, false); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var length int32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		t.Fatal(err)
	}
	if length < 4 || length > 1024 {
		t.Fatalf("invalid response size %d", length)
	}
	response := make([]byte, length)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(response[:4]) != 101 {
		t.Fatal("wrong response correlation ID")
	}
	if code := ackResponseCode(t, response[4:]); code != protocol.ErrNotEnoughReplicasAfterAppend {
		t.Fatalf("ISR drop returned %d, want NOT_ENOUGH_REPLICAS_AFTER_APPEND", code)
	}
	if p.GetLogEndOffset() != 1 {
		t.Fatal("failed acknowledgement removed the written record")
	}
}
func TestIntegration_AcksAllWaitsForDurableFollower(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(listener.Addr().(*net.TCPAddr).Port)})
	m.Brokers.Register(&broker.Broker{ID: 2})
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	p := &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	coord := coordinator.NewCoordinator()
	go func() {
		defer close(serverDone)
		if err := Serve(ctx, listener, m, coord, 1); err != nil {
			t.Errorf("ack server: %v", err)
		}
	}()
	follower := replication.NewFollowerManager(m, 2, 5*time.Millisecond)
	if err := follower.Start(ctx); err != nil {
		cancel()
		listener.Close()
		coord.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		coord.Close()
		follower.Wait()
		select {
		case <-serverDone:
		case <-time.After(5 * time.Second):
			t.Error("acknowledgement test server did not stop")
		}
	})
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	response := sendV0Request(t, conn, protocol.APIKeyProduce, 11, 42,
		ackProduceBody(-1, 3000, "events", ackBatch(t)))
	if code := ackResponseCode(t, response); code != protocol.ErrNone {
		t.Fatalf("replicated produce returned %d", code)
	}
	if p.GetHighWatermark() != 1 || p.GetReplicaLEO(2) != 1 {
		t.Fatal("acks=-1 succeeded before follower progress")
	}
	data, err := metadata.ReadPartitionLogFrom(p, "00000000000000000000.log", 0, p.LogDirForBroker(2))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := storage.ParseRecordBatches(data)
	if err != nil || len(batches) != 1 || string(batches[0].Values[0]) != `{"value":"record"}` {
		t.Fatalf("acks=-1 did not correspond to a complete follower disk write: %v", err)
	}
}
