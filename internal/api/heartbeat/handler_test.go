package heartbeat

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseErrCode(body []byte) int16 {
	return int16(binary.BigEndian.Uint16(body[0:2]))
}

// joinAndSync joins a group and immediately syncs (leader assigns itself),
// transitioning the group to Stable state so heartbeats are accepted.
func joinAndSync(coord *coordinator.Coordinator, groupID string) (memberID string, genID int32) {
	res := coord.JoinGroup(groupID, "", 30000, "consumer",
		[]coordinator.MemberProtocol{{Name: "range"}})
	memberID = res.MemberID
	genID = res.GenerationID
	coord.SyncGroup(groupID, genID, memberID,
		[]coordinator.SyncGroupAssignment{{MemberID: memberID, Assignment: []byte("a")}})
	return
}

func TestHeartbeat_Stable(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID, genID := joinAndSync(coord, "hb-group")
	req := &protocol.HeartbeatRequest{
		GroupID:      "hb-group",
		GenerationID: genID,
		MemberID:     memberID,
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", ec)
	}
}

func TestHeartbeat_UnknownMember(t *testing.T) {
	coord := coordinator.NewCoordinator()
	joinAndSync(coord, "hb-group")
	req := &protocol.HeartbeatRequest{
		GroupID:      "hb-group",
		GenerationID: 1,
		MemberID:     "unknown-member-id",
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown member, got 0")
	}
}

func TestHeartbeat_WrongGeneration(t *testing.T) {
	coord := coordinator.NewCoordinator()
	memberID, _ := joinAndSync(coord, "hb-group")
	req := &protocol.HeartbeatRequest{
		GroupID:      "hb-group",
		GenerationID: 99, // wrong
		MemberID:     memberID,
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for wrong generation, got 0")
	}
}

func TestHeartbeat_UnknownGroup(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.HeartbeatRequest{
		GroupID:      "no-such-group",
		GenerationID: 1,
		MemberID:     "any-id",
	}
	body := BuildBody(req, coord)
	if ec := parseErrCode(body); ec == protocol.ErrNone {
		t.Error("expected non-zero error_code for unknown group, got 0")
	}
}
