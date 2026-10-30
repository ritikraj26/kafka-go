package network

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestIntegration_Fetch16ConsumerVisibility(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1})
	m.Brokers.Register(&broker.Broker{ID: 2})
	topic := testutil.CreateTopic(t, m, "events", 1)
	p := &topic.Partitions[0]
	for _, value := range []string{"committed", "uncommitted"} {
		batch, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: []byte(value)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.AppendRecords(batch, p.LogDirForBroker(1)); err != nil {
			t.Fatal(err)
		}
	}
	p.UpdateReplicaState(2, 1, 0)
	p.RunUnderLock(func() { p.AdvanceHighWatermark() })
	conn := recoveryConnection(t, m)
	e := protocol.NewEncoder()
	e.WriteString("real-client")
	e.WriteTagBuffer()
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteInt32(1024)
	e.WriteByte(0)
	e.WriteInt32(0)
	e.WriteInt32(-1)
	e.WriteUnsignedVarint(2)
	e.WriteUUID(topic.ID)
	e.WriteUnsignedVarint(2)
	e.WriteInt32(0)
	e.WriteInt32(-1)
	e.WriteInt64(0)
	e.WriteInt32(-1)
	e.WriteInt64(0)
	e.WriteInt32(1024)
	e.WriteTagBuffer()
	e.WriteTagBuffer()
	e.WriteUnsignedVarint(1)
	e.WriteCompactString("")
	e.WriteTagBuffer() // no ReplicaState: this is a consumer
	response := sendV0Request(t, conn, protocol.APIKeyFetch, 16, 42, e.Bytes())
	// Header tag(1), throttle(4), error(2), session(4), topics(1), UUID(16),
	// partitions(1), index(4), error(2), HW(8), LSO(8), start(8),
	// aborted transactions(1), preferred replica(4), compact record set.
	if len(response) < 65 || response[0] != 0 {
		t.Fatalf("invalid flexible response: %x", response)
	}
	if code := int16(binary.BigEndian.Uint16(response[33:35])); code != 0 {
		t.Fatalf("partition error=%d", code)
	}
	if hw := int64(binary.BigEndian.Uint64(response[35:43])); hw != 1 {
		t.Fatalf("HW=%d, want 1", hw)
	}
	d := protocol.NewDecoder(response[64:])
	records, err := d.ReadCompactBytes()
	if err != nil {
		t.Fatal(err)
	}
	batches, err := storage.ParseRecordBatches(records)
	if err != nil || len(batches) != 1 || string(batches[0].Values[0]) != "committed" {
		t.Fatalf("consumer fetched uncommitted or malformed data: %v", err)
	}
	if p.GetHighWatermark() != 1 || p.GetReplicaLEO(0) != 0 {
		t.Fatal("consumer request changed replication state")
	}
}
