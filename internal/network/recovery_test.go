package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/storage"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestIntegration_ProduceRestartFetch(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	m.MinInSyncReplicas = 1 // Explicit single-broker opt-in.
	topic := testutil.CreateTopic(t, m, "events", 1)
	id, dir := topic.ID, m.GetLogDir()
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	values := []string{`{"value":"one"}`, `{"value":"two"}`, `{"value":"three"}`}
	records := make([]storage.Record, len(values))
	for i, value := range values {
		records[i].Value = []byte(value)
	}
	batch, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("produce", func(t *testing.T) {
		conn := recoveryConnection(t, m)
		e := protocol.NewEncoder()
		e.WriteString("restart-test") // standard request client ID
		e.WriteTagBuffer()
		e.WriteByte(0) // null transactional ID
		e.WriteInt16(-1)
		e.WriteInt32(1000)
		e.WriteUnsignedVarint(2)
		e.WriteCompactString("events")
		e.WriteUnsignedVarint(2)
		e.WriteInt32(0)
		e.WriteCompactBytes(batch)
		e.WriteTagBuffer()
		e.WriteTagBuffer()
		e.WriteTagBuffer()
		response := sendV0Request(t, conn, protocol.APIKeyProduce, 11, 1, e.Bytes())
		d := protocol.NewDecoder(response)
		if tag, err := d.ReadByte(); err != nil || tag != 0 {
			t.Fatalf("response header: tag=%d err=%v", tag, err)
		}
		if count, err := d.ReadUnsignedVarint(); err != nil || count != 2 {
			t.Fatalf("topic count=%d err=%v", count, err)
		}
		if name, err := d.ReadCompactString(); err != nil || name != "events" {
			t.Fatalf("topic=%s err=%v", name, err)
		}
		if count, err := d.ReadUnsignedVarint(); err != nil || count != 2 {
			t.Fatalf("partition count=%d err=%v", count, err)
		}
		if index, err := d.ReadInt32(); err != nil || index != 0 {
			t.Fatalf("partition=%d err=%v", index, err)
		}
		if code, err := d.ReadInt16(); err != nil || code != 0 {
			t.Fatalf("produce error=%d err=%v", code, err)
		}
		if offset, err := d.ReadInt64(); err != nil || offset != 0 {
			t.Fatalf("base offset=%d err=%v", offset, err)
		}
	})
	if t.Failed() {
		return
	}
	restarted := metadata.NewManager()
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	if recovered := restarted.GetTopicByID(id); recovered == nil || recovered.Name != "events" {
		t.Fatal("restart lost topic identity")
	}
	t.Run("fetch after restart", func(t *testing.T) {
		conn := recoveryConnection(t, restarted)
		e := protocol.NewEncoder()
		e.WriteString("restart-test")
		e.WriteInt32(-1) // consumer
		e.WriteInt32(0)
		e.WriteInt32(1)
		e.WriteArrayLen(1)
		e.WriteString("events")
		e.WriteArrayLen(1)
		e.WriteInt32(0)
		e.WriteInt64(0)
		e.WriteInt32(1 << 20)
		d := protocol.NewDecoder(sendV0Request(t, conn, protocol.APIKeyFetch, 0, 2, e.Bytes()))
		if count, err := d.ReadArrayLen(); err != nil || count != 1 {
			t.Fatalf("topic count=%d err=%v", count, err)
		}
		if name, err := d.ReadString(); err != nil || name != "events" {
			t.Fatalf("topic=%s err=%v", name, err)
		}
		if count, err := d.ReadArrayLen(); err != nil || count != 1 {
			t.Fatalf("partition count=%d err=%v", count, err)
		}
		if index, err := d.ReadInt32(); err != nil || index != 0 {
			t.Fatalf("partition=%d err=%v", index, err)
		}
		if code, err := d.ReadInt16(); err != nil || code != 0 {
			t.Fatalf("fetch error=%d err=%v", code, err)
		}
		if hw, err := d.ReadInt64(); err != nil || hw != 3 {
			t.Fatalf("HW=%d err=%v", hw, err)
		}
		data, err := d.ReadBytes()
		if err != nil {
			t.Fatal(err)
		}
		batches, err := storage.ParseRecordBatches(data)
		if err != nil || len(batches) != 1 || batches[0].BaseOffset != 0 || batches[0].LastOffsetDelta != 2 {
			t.Fatalf("bad fetched batch after restart: %v", err)
		}
		if len(batches[0].Values) != len(values) {
			t.Fatal("missing records after restart")
		}
		for i, value := range batches[0].Values {
			if string(value) != values[i] {
				t.Fatalf("record %d changed after restart", i)
			}
		}
	})
}

func recoveryConnection(t *testing.T, m *metadata.Manager) net.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	coord := coordinator.NewCoordinator()
	go func() {
		defer close(done)
		if err := Serve(ctx, listener, m, coord, 1); err != nil {
			t.Errorf("test server: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		coord.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("test server did not stop")
		}
	})
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
