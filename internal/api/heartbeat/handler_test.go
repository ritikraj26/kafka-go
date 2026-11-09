package heartbeat

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseErrCode(body []byte) int16 {
	return int16(binary.BigEndian.Uint16(body[0:2]))
}

// joinAndSync joins a group and immediately syncs (leader assigns itself),
// transitioning the group to Stable after publishing assignments.
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

func TestHeartbeat_AwaitingSync(t *testing.T) {
	coord := coordinator.NewCoordinator()
	t.Cleanup(coord.Close)
	joined := coord.JoinGroup("awaiting-sync", "", 30000, "consumer", []coordinator.MemberProtocol{{Name: "range"}})
	if joined.ErrorCode != protocol.ErrNone {
		t.Fatal(joined.ErrorCode)
	}
	for _, tc := range []struct {
		name       string
		member     string
		generation int32
		want       int16
	}{
		{"valid member", joined.MemberID, joined.GenerationID, protocol.ErrNone},
		{"wrong generation", joined.MemberID, joined.GenerationID + 1, protocol.ErrIllegalGeneration},
		{"unknown member", "unknown", joined.GenerationID, protocol.ErrUnknownMemberID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := BuildBody(&protocol.HeartbeatRequest{
				GroupID: "awaiting-sync", MemberID: tc.member, GenerationID: tc.generation,
			}, coord)
			if code := parseErrCode(body); code != tc.want {
				t.Fatalf("heartbeat error=%d, want %d", code, tc.want)
			}
		})
	}
}

func TestHeartbeat_JoiningStillRequiresRejoin(t *testing.T) {
	coord := coordinator.NewCoordinator()
	t.Cleanup(coord.Close)
	member, generation := joinAndSync(coord, "joining")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan *coordinator.JoinGroupResult, 1)
	go func() {
		joined <- coord.JoinGroupContext(ctx, "joining", "", 30000, "consumer", []coordinator.MemberProtocol{{Name: "range"}})
	}()
	req := &protocol.HeartbeatRequest{GroupID: "joining", GenerationID: generation, MemberID: member}
	deadline := time.Now().Add(time.Second)
	for {
		code := parseErrCode(BuildBody(req, coord))
		if code == protocol.ErrRebalanceInProgress {
			break
		}
		if code != protocol.ErrNone || time.Now().After(deadline) {
			t.Fatalf("heartbeat error=%d while waiting for joining, want rebalance in progress", code)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("new member join did not cancel")
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
