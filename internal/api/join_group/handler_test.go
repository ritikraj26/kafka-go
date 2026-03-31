package joingroup

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func parseJoinGroupResponse(t *testing.T, body []byte) (errCode int16, genID int32, leaderID, memberID string) {
	t.Helper()
	pos := 0
	errCode = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	genID = int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	// protocol_name STRING
	pnLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2 + pnLen
	// leader_id STRING
	lLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2
	leaderID = string(body[pos : pos+lLen])
	pos += lLen
	// member_id STRING
	mLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2
	memberID = string(body[pos : pos+mLen])
	return
}

func TestJoinGroup_NewMember(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.JoinGroupRequest{
		GroupID:          "grp",
		MemberID:         "",
		SessionTimeoutMs: 30000,
		ProtocolType:     "consumer",
		Protocols:        []protocol.GroupProtocol{{Name: "range", Metadata: nil}},
	}
	body := BuildBody(req, coord)
	errCode, genID, _, memberID := parseJoinGroupResponse(t, body)
	if errCode != protocol.ErrNone {
		t.Fatalf("error_code = %d, want 0", errCode)
	}
	if genID != 1 {
		t.Errorf("generation_id = %d, want 1", genID)
	}
	if memberID == "" {
		t.Error("member_id should be assigned (non-empty)")
	}
}

func TestJoinGroup_ExistingMember(t *testing.T) {
	coord := coordinator.NewCoordinator()
	req := &protocol.JoinGroupRequest{
		GroupID:          "grp",
		MemberID:         "",
		SessionTimeoutMs: 30000,
		ProtocolType:     "consumer",
		Protocols:        []protocol.GroupProtocol{{Name: "range", Metadata: nil}},
	}
	// First join to get a member ID
	body1 := BuildBody(req, coord)
	_, _, _, memberID := parseJoinGroupResponse(t, body1)

	// Rejoin with the same member ID
	req.MemberID = memberID
	body2 := BuildBody(req, coord)
	errCode, genID, _, returnedID := parseJoinGroupResponse(t, body2)
	if errCode != protocol.ErrNone {
		t.Fatalf("rejoin error_code = %d, want 0", errCode)
	}
	if returnedID != memberID {
		t.Errorf("member_id = %q, want %q (same after rejoin)", returnedID, memberID)
	}
	if genID < 1 {
		t.Errorf("generation_id = %d, want >= 1", genID)
	}
}

func TestJoinGroup_TwoMembersLeaderAssigned(t *testing.T) {
	coord := coordinator.NewCoordinator()
	// RebalanceDelay holds the join round open so both goroutines can arrive
	// before the round closes. Without it, the first member completes the
	// round immediately (solo) and the second starts a new generation alone.
	coord.RebalanceDelay = 100 * time.Millisecond
	proto := []protocol.GroupProtocol{{Name: "range", Metadata: nil}}

	type roundResult struct {
		genID    int32
		leaderID string
		memberID string
	}
	results := make(chan roundResult, 2)

	for i := 0; i < 2; i++ {
		go func() {
			body := BuildBody(&protocol.JoinGroupRequest{
				GroupID: "grp2", SessionTimeoutMs: 5000,
				ProtocolType: "consumer", Protocols: proto,
			}, coord)
			_, genID, leaderID, memberID := parseJoinGroupResponse(t, body)
			results <- roundResult{genID, leaderID, memberID}
		}()
	}

	r1 := <-results
	r2 := <-results

	if r1.leaderID == "" || r2.leaderID == "" {
		t.Errorf("leader_id should not be empty: r1=%q r2=%q", r1.leaderID, r2.leaderID)
	}
	if r1.leaderID != r2.leaderID {
		t.Errorf("both members must agree on leader: r1=%q r2=%q", r1.leaderID, r2.leaderID)
	}
	if r1.genID != r2.genID {
		t.Errorf("both members must be in the same generation: r1=%d r2=%d", r1.genID, r2.genID)
	}
}
