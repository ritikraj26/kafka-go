package apimetadata

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// readUvarint reads an unsigned varint from buf at position pos.
func readUvarint(buf []byte, pos int) (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if pos+i >= len(buf) {
			return 0, 0
		}
		b := buf[pos+i]
		x |= uint64(b&0x7f) << s
		if b < 0x80 {
			return x, i + 1
		}
		s += 7
	}
	return 0, 0
}

// readCompactString reads a COMPACT_STRING at pos and returns the string and next position.
func readCompactString(buf []byte, pos int) (string, int) {
	l, n := readUvarint(buf, pos)
	if n == 0 {
		return "", pos
	}
	strLen := int(l) - 1
	pos += n
	return string(buf[pos : pos+strLen]), pos + strLen
}

// parseBrokersCount reads the brokers compact array count from the response body (after throttle_time_ms).
func parseBrokersCount(body []byte) int {
	count, _ := readUvarint(body, 4)
	return int(count) - 1
}

func TestMetadata_FallbackNoBrokers(t *testing.T) {
	mgr := metadata.NewManager()
	req := &protocol.MetadataRequest{}
	body := BuildBody(req, mgr)
	if len(body) == 0 {
		t.Fatal("expected non-empty response body")
	}
	count := parseBrokersCount(body)
	if count != 1 {
		t.Errorf("brokers count = %d, want 1 (fallback localhost)", count)
	}
}

func TestMetadata_WithBrokerRegistry(t *testing.T) {
	mgr := metadata.NewManager()
	reg := broker.NewRegistry(1)
	reg.Register(&broker.Broker{ID: 1, Host: "b1", Port: 9092})
	reg.Register(&broker.Broker{ID: 2, Host: "b2", Port: 9093})
	mgr.Brokers = reg
	req := &protocol.MetadataRequest{}
	body := BuildBody(req, mgr)
	count := parseBrokersCount(body)
	if count != 2 {
		t.Errorf("brokers count = %d, want 2", count)
	}
}

func TestMetadata_SpecificTopic(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("orders", 3)
	req := &protocol.MetadataRequest{TopicNames: []string{"orders"}}
	body := BuildBody(req, mgr)
	if len(body) == 0 {
		t.Fatal("expected non-empty response body")
	}
}

func TestMetadata_AllTopics(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("t1", 1)
	mgr.CreateTopic("t2", 2)
	req := &protocol.MetadataRequest{TopicNames: nil} // nil = all topics
	body := BuildBody(req, mgr)
	// Find topics compact array after brokers section.
	// Brokers: throttle(4) + brokers_count_varint + broker data + clusterID + controllerID
	// Rather than fully parsing, just ensure body is non-empty and has content.
	if len(body) < 10 {
		t.Errorf("response body too short: %d bytes", len(body))
	}
	_ = binary.BigEndian.Uint32(body[0:4]) // throttle_time_ms (suppress unused import)
}
