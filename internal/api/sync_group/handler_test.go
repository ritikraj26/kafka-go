package syncgroup

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseErrCode(body []byte) int16 {
	return int16(binary.BigEndian.Uint16(body[0:2]))
}

func joinMember(coord *coordinator.Coordinator, groupID string) (memberID string, genID int32) {
	res := coord.JoinGroup(groupID, "", 30000, "consumer",
		[]coordinator.MemberProtocol{{Name: "range"}})
	return res.MemberID, res.GenerationID
}

func TestSyncGroup_LeaderSendsAssignment(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID, genID := joinMember(coord, "sync-group")
	req := &protocol.SyncGroupRequest{
		GroupID:      "sync-group",
		GenerationID: genID,
		MemberID:     memberID,
		Assignments: []protocol.SyncGroupAssignment{
			{MemberID: memberID, Assignment: []byte("assignment-data")},
		},
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
}

func TestSyncGroup_WrongGeneration(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID, _ := joinMember(coord, "sync-group")
	req := &protocol.SyncGroupRequest{
		GroupID:      "sync-group",
		GenerationID: 99, // wrong
		MemberID:     memberID,
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for wrong generation, got 0")
	}
}

func TestSyncGroup_UnknownGroup(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.SyncGroupRequest{
		GroupID:      "no-such-group",
		GenerationID: 1,
		MemberID:     "any-id",
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown group, got 0")
	}
}
