package leavegroup

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseErrCode(body []byte) int16 {
	return int16(binary.BigEndian.Uint16(body[0:2]))
}

func joinMember(coord *coordinator.Coordinator, groupID string) string {
	res := coord.JoinGroup(groupID, "", 30000, "consumer",
		[]coordinator.MemberProtocol{{Name: "range"}})
	return res.MemberID
}

func TestLeaveGroup_ValidMember(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID := joinMember(coord, "leave-group")
	req := &protocol.LeaveGroupRequest{
		GroupID:  "leave-group",
		MemberID: memberID,
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
}

func TestLeaveGroup_UnknownMember(t *testing.T) {
	coord := coordinator.NewCoordinator()
	joinMember(coord, "leave-group")
	req := &protocol.LeaveGroupRequest{
		GroupID:  "leave-group",
		MemberID: "unknown-member",
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown member, got 0")
	}
}

func TestLeaveGroup_UnknownGroup(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.LeaveGroupRequest{
		GroupID:  "no-such-group",
		MemberID: "any-id",
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown group, got 0")
	}
}
