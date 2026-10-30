package produce

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func replicatedProduceManager(t *testing.T, partitions int) (*metadata.Manager, *metadata.Topic) {
	t.Helper()
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1})
	m.Brokers.Register(&broker.Broker{ID: 2})
	m.Schemas.Register("orders", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	return m, testutil.CreateTopic(t, m, "orders", partitions)
}

func TestProduceInvalidAcksHaveNoSideEffects(t *testing.T) {
	for _, acks := range []int16{-2, 2, 3, 32767} {
		m, p := produceManager(t)
		req := produceRequest(produceBatch(t, `{"value":12}`))
		req.Acks = acks
		body, failed := Process(req, m, 1)
		if !failed || responseError(t, body) != protocol.ErrInvalidRequiredAcks {
			t.Fatalf("invalid acks %d did not return INVALID_REQUIRED_ACKS", acks)
		}
		if p.GetLogEndOffset() != 0 || m.GetTopic("orders.dlq") != nil {
			t.Fatal("invalid acknowledgement caused source or DLQ writes")
		}
	}
}

func TestProduceMinimumISRBlocksSourceAndDLQSideEffects(t *testing.T) {
	m, p := produceManager(t)
	req := produceRequest(produceBatch(t, `{"value":12}`))
	req.Acks = -1
	body, failed := Process(req, m, 1)
	if !failed || responseError(t, body) != protocol.ErrNotEnoughReplicas {
		t.Fatal("missing minimum did not reject before schema routing")
	}
	if p.GetLogEndOffset() != 0 || m.GetTopic("orders.dlq") != nil {
		t.Fatal("minimum-ISR rejection caused write side effects")
	}
}

func TestProduceNegativeTimeoutRejected(t *testing.T) {
	m, p := produceManager(t)
	req := produceRequest(produceBatch(t, `{"value":"valid"}`))
	req.TimeoutMs = -1
	body, failed := Process(req, m, 1)
	if !failed || responseError(t, body) != protocol.ErrInvalidRequest || p.GetLogEndOffset() != 0 {
		t.Fatal("negative timeout was accepted or appended")
	}
}

func TestDLQHonorsRequiredAcknowledgements(t *testing.T) {
	m, _ := replicatedProduceManager(t, 1)
	req := produceRequest(produceBatch(t, `{"value":12}`))
	req.Acks = -1
	req.TimeoutMs = 10
	body := BuildBody(req, m, 1)
	if code := responseError(t, body); code != protocol.ErrRequestTimedOut {
		t.Fatalf("DLQ returned %d without a durable follower, want timeout", code)
	}
	dlq := m.GetTopic("orders.dlq")
	if dlq == nil || dlq.Partitions[0].GetLogEndOffset() != 1 || dlq.Partitions[0].GetHighWatermark() != 0 {
		t.Fatal("unexpected DLQ append/replication state")
	}
}

func TestProduceSharesRequestDeadlineAcrossPartitions(t *testing.T) {
	m, topic := replicatedProduceManager(t, 3)
	// Partitions 0 and 2 have leader 1 under round-robin placement.
	req := produceRequest(produceBatch(t, `{"value":"a"}`))
	req.Acks = -1
	req.TimeoutMs = 200
	req.Topics[0].Partitions = append(req.Topics[0].Partitions,
		protocol.ProducePartition{Index: 2, Records: produceBatch(t, `{"value":"b"}`)})
	start := time.Now()
	body := BuildBody(req, m, 1)
	elapsed := time.Since(start)
	if elapsed >= 350*time.Millisecond {
		t.Fatalf("replication timeout was restarted for the next partition: %s", elapsed)
	}
	if responseError(t, body) != protocol.ErrRequestTimedOut {
		t.Fatal("first partition did not time out")
	}
	// Each v11 partition entry with null optional fields occupies 33 bytes.
	nameBytes := int(body[1]) - 1
	first := 1 + 1 + nameBytes + 1
	secondError := first + 33 + 4
	if len(body) < secondError+2 || int16(binary.BigEndian.Uint16(body[secondError:secondError+2])) != protocol.ErrRequestTimedOut {
		t.Fatal("second partition did not share the expired deadline")
	}
	if topic.Partitions[0].GetLogEndOffset() != 1 || topic.Partitions[2].GetLogEndOffset() != 1 {
		t.Fatal("timed-out appends should remain on disk")
	}
}
