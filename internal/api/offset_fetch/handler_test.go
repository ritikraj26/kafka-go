package offsetfetch

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// parsePartitionOffset parses the committed offset from the first partition of the first topic.
// Response layout: array_len(4) + name_len(2) + name + part_array_len(4) + part_index(4) + offset(8) + ...
func parsePartitionOffset(body []byte) (offset int64, errCode int16) {
	pos := 4 // skip topics array length
	nameLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2 + nameLen
	pos += 4 // skip partitions array length
	pos += 4 // skip partition index
	offset = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8
	metaLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2 + metaLen
	errCode = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	return
}

func joinAndCommit(coord *coordinator.Coordinator, groupID, topic string, partition int32, offset int64) {
	res := coord.JoinGroup(groupID, "", 30000, "consumer",
		[]coordinator.MemberProtocol{{Name: "range"}})
	coord.CommitOffset(groupID, res.GenerationID, res.MemberID, topic, partition, offset)
}

func TestOffsetFetch_NoCommittedOffset(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.OffsetFetchRequest{
		GroupID: "fetch-group",
		Topics: []protocol.OffsetFetchTopic{
			{Name: "orders", Partitions: []int32{0}},
		},
	}
	body := BuildBody(req, coord)
	offset, _ := parsePartitionOffset(body)
	if offset != -1 {
		t.Errorf("offset = %d, want -1 (no committed offset)", offset)
	}
}

func TestOffsetFetch_WithCommittedOffset(t *testing.T) {
	coord := coordinator.NewCoordinator()
	joinAndCommit(coord, "fetch-group", "orders", 0, 55)
	req := &protocol.OffsetFetchRequest{
		GroupID: "fetch-group",
		Topics: []protocol.OffsetFetchTopic{
			{Name: "orders", Partitions: []int32{0}},
		},
	}
	body := BuildBody(req, coord)
	offset, ec := parsePartitionOffset(body)
	if ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
	if offset != 55 {
		t.Errorf("offset = %d, want 55", offset)
	}
}
