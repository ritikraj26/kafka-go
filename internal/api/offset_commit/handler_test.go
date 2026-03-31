package offsetcommit

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseTopicErrCode(body []byte) int16 {
	// Response: array_len(4) + topic_name_len(2) + topic_name + part_array_len(4) + part_index(4) + error_code(2)
	// Skip array length (4 bytes)
	pos := 4
	// Skip topic name: INT16 length + bytes
	nameLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2 + nameLen
	// Skip partitions array length (4 bytes)
	pos += 4
	// Skip partition index (4 bytes)
	pos += 4
	// Read error_code (2 bytes)
	return int16(binary.BigEndian.Uint16(body[pos : pos+2]))
}

func joinMember(coord *coordinator.Coordinator, groupID string) (memberID string, genID int32) {
	res := coord.JoinGroup(groupID, "", 30000, "consumer",
		[]coordinator.MemberProtocol{{Name: "range"}})
	return res.MemberID, res.GenerationID
}

func TestOffsetCommit_HappyPath(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID, genID := joinMember(coord, "commit-group")
	req := &protocol.OffsetCommitRequest{
		GroupID:      "commit-group",
		GenerationID: genID,
		MemberID:     memberID,
		Topics: []protocol.OffsetCommitTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetCommitPartition{
					{Index: 0, CommittedOffset: 42},
				},
			},
		},
	}
	body := BuildBody(req, coord)
	if ec := parseTopicErrCode(body); ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
}

func TestOffsetCommit_UnknownGroup(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.OffsetCommitRequest{
		GroupID:      "no-such-group",
		GenerationID: 1,
		MemberID:     "any-id",
		Topics: []protocol.OffsetCommitTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetCommitPartition{
					{Index: 0, CommittedOffset: 10},
				},
			},
		},
	}
	body := BuildBody(req, coord)
	if ec := parseTopicErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown group, got 0")
	}
}
