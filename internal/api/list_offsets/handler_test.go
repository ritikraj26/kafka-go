package listoffsets

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// parseFirstPartition parses error_code, timestamp, and offset for the first partition of the first topic.
// Response: array_len(4) + name_len(2)+name + part_array_len(4) + part_index(4) + errCode(2) + timestamp(8) + offset(8)
func parseFirstPartition(body []byte) (errCode int16, timestamp int64, offset int64) {
	pos := 4 // skip topics array length
	nameLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2 + nameLen
	pos += 4 // skip partitions array length
	pos += 4 // skip partition index
	errCode = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	timestamp = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8
	offset = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	return
}

func TestListOffsets_UnknownTopic(t *testing.T) {
	mgr := metadata.NewManager()
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{Name: "ghost", Partitions: []protocol.ListOffsetsPartition{{Index: 0, Timestamp: -1}}},
		},
	}
	body := BuildBody(req, mgr)
	ec, _, _ := parseFirstPartition(body)
	if ec != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want ErrUnknownTopicOrPartition (%d)", ec, protocol.ErrUnknownTopicOrPartition)
	}
}

func TestListOffsets_Latest(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("orders", 1)
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{Name: "orders", Partitions: []protocol.ListOffsetsPartition{{Index: 0, Timestamp: -1}}},
		},
	}
	body := BuildBody(req, mgr)
	ec, ts, _ := parseFirstPartition(body)
	if ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
	if ts != -1 {
		t.Errorf("timestamp = %d, want -1 (latest)", ts)
	}
}

func TestListOffsets_Earliest(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("orders", 1)
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{Name: "orders", Partitions: []protocol.ListOffsetsPartition{{Index: 0, Timestamp: -2}}},
		},
	}
	body := BuildBody(req, mgr)
	ec, _, off := parseFirstPartition(body)
	if ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
	if off != 0 {
		t.Errorf("offset = %d, want 0 (earliest)", off)
	}
}

func TestListOffsets_UnknownPartition(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("orders", 1)
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{Name: "orders", Partitions: []protocol.ListOffsetsPartition{{Index: 99, Timestamp: -1}}},
		},
	}
	body := BuildBody(req, mgr)
	ec, _, _ := parseFirstPartition(body)
	if ec != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want ErrUnknownTopicOrPartition (%d)", ec, protocol.ErrUnknownTopicOrPartition)
	}
}
