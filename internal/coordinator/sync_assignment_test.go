package coordinator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

type assignmentSyncResult struct {
	data []byte
	code int16
}

func newSyncAssignmentGroup(t *testing.T, limits StateLimits) (*Coordinator, int32) {
	t.Helper()
	c := NewCoordinator()
	if err := c.SetStateLimits(limits); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	first := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != protocol.ErrNone {
		t.Fatalf("initial join error=%d", first.ErrorCode)
	}
	return c, rejoinSyncAssignmentGroup(t, c)
}

func rejoinSyncAssignmentGroup(t *testing.T, c *Coordinator) int32 {
	t.Helper()
	aID, bID := syncAssignmentMemberIDs(t, c)
	joined := make(chan *JoinGroupResult, 1)
	go func(memberID string) {
		joined <- c.JoinGroup("g", memberID, 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}(bID)
	bID = waitPendingJoinMember(t, c, aID)
	a := c.JoinGroup("g", aID, 60000, "consumer", []MemberProtocol{{Name: "range"}})
	select {
	case b := <-joined:
		if a.ErrorCode != protocol.ErrNone || b.ErrorCode != protocol.ErrNone ||
			a.GenerationID != b.GenerationID || a.LeaderID != aID || b.LeaderID != aID || b.MemberID != bID {
			t.Fatalf("unexpected join results: a=%+v b=%+v", a, b)
		}
	case <-time.After(time.Second):
		t.Fatal("follower join did not complete")
	}
	return a.GenerationID
}

// The first member stays leader across these assignment fixtures. The second
// member starts with an empty ID and receives its identity from the coordinator.
func syncAssignmentMemberIDs(t *testing.T, c *Coordinator) (a, b string) {
	t.Helper()
	g := c.FindGroup("g")
	if g == nil {
		t.Fatal("assignment group is missing")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	a = g.LeaderID
	if a == "" || g.Members[a] == nil {
		t.Fatal("assignment group has no leader")
	}
	for id := range g.Members {
		if id != a {
			if b != "" {
				t.Fatal("assignment fixture has more than two members")
			}
			b = id
		}
	}
	return a, b
}

// Tables use role labels; resolve them to IDs obtained through real admission.
func assignmentMemberIDs(assignments []SyncGroupAssignment, a, b string) []SyncGroupAssignment {
	if assignments == nil {
		return nil
	}
	result := make([]SyncGroupAssignment, len(assignments))
	for i, assignment := range assignments {
		result[i] = assignment
		switch assignment.MemberID {
		case "a":
			result[i].MemberID = a
		case "b":
			result[i].MemberID = b
		}
	}
	return result
}

func waitSyncAssignment(t *testing.T, c *Coordinator, generation int32) <-chan assignmentSyncResult {
	t.Helper()
	_, bID := syncAssignmentMemberIDs(t, c)
	result := make(chan assignmentSyncResult, 1)
	go func() {
		data, code := c.SyncGroup("g", generation, bID, nil)
		result <- assignmentSyncResult{data, code}
	}()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.pendingSyncs[bID] != nil })
	return result
}

func receiveSyncAssignment(t *testing.T, result <-chan assignmentSyncResult, want []byte) {
	t.Helper()
	select {
	case got := <-result:
		if got.code != protocol.ErrNone || got.data == nil || !bytes.Equal(got.data, want) {
			t.Fatalf("follower sync=(%q, %d), want (%q, success)", got.data, got.code, want)
		}
	case <-time.After(time.Second):
		t.Fatal("follower sync did not complete")
	}
}

func assertSyncAssignmentState(t *testing.T, c *Coordinator, state GroupState, a, b []byte) {
	t.Helper()
	aID, bID := syncAssignmentMemberIDs(t, c)
	g := c.FindGroup("g")
	g.mu.Lock()
	protocols := []MemberProtocol{{Name: "range"}}
	wantRetained := groupCharge("g") + 2*int64(len("consumer"))
	for id, want := range map[string][]byte{aID: a, bID: b} {
		m := g.Members[id]
		if !bytes.Equal(m.Assignment, want) {
			t.Errorf("member %s assignment=%q, want %q", id, m.Assignment, want)
		}
		wantMember := memberCharge(id, protocols, want)
		if m.retained != wantMember {
			t.Errorf("member %s charge=%d, want %d", id, m.retained, wantMember)
		}
		wantRetained += wantMember
	}
	if g.State != state || g.retained != wantRetained {
		t.Errorf("group state/charge=(%v, %d), want (%v, %d)", g.State, g.retained, state, wantRetained)
	}
	g.mu.Unlock()
	if got := c.StateStats().GroupBytes; got != wantRetained {
		t.Errorf("global group charge=%d, want %d", got, wantRetained)
	}
}

func TestSyncGroupReplacesAssignmentsForEveryMember(t *testing.T) {
	for _, tc := range []struct {
		name        string
		assignments []SyncGroupAssignment
		a, b        []byte
	}{
		{"omitted follower", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("P0,P1")}}, []byte("P0,P1"), nil},
		{"omitted leader", []SyncGroupAssignment{{MemberID: "b", Assignment: []byte("P0,P1")}}, nil, []byte("P0,P1")},
		{"nil snapshot", nil, nil, nil},
		{"empty snapshot", []SyncGroupAssignment{}, nil, nil},
		{"nil follower assignment", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("P0,P1")}, {MemberID: "b"}}, []byte("P0,P1"), nil},
		{"empty follower assignment", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("P0,P1")}, {MemberID: "b", Assignment: []byte{}}}, []byte("P0,P1"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, generation := newSyncAssignmentGroup(t, DefaultStateLimits())
			aID, bID := syncAssignmentMemberIDs(t, c)
			if _, code := c.SyncGroup("g", generation, aID, []SyncGroupAssignment{
				{MemberID: aID, Assignment: []byte("P0")},
				{MemberID: bID, Assignment: []byte("P1")},
			}); code != protocol.ErrNone {
				t.Fatalf("initial sync error=%d", code)
			}
			generation = rejoinSyncAssignmentGroup(t, c)
			follower := waitSyncAssignment(t, c, generation)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			data, code := c.SyncGroupContext(ctx, "g", generation, aID, assignmentMemberIDs(tc.assignments, aID, bID))
			if code != protocol.ErrNone || data == nil || !bytes.Equal(data, tc.a) {
				t.Fatalf("leader sync=(%q, %d), want (%q, success)", data, code, tc.a)
			}
			receiveSyncAssignment(t, follower, tc.b)
			assertSyncAssignmentState(t, c, GroupStable, tc.a, tc.b)
			for id, want := range map[string][]byte{aID: tc.a, bID: tc.b} {
				cached, code := c.SyncGroup("g", generation, id, nil)
				if code != protocol.ErrNone || cached == nil || !bytes.Equal(cached, want) {
					t.Fatalf("cached assignment for %s=(%q, %d), want (%q, success)", id, cached, code, want)
				}
			}
		})
	}
}

func TestSyncGroupRejectedSnapshotIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name        string
		assignments []SyncGroupAssignment
		code        int16
	}{
		{"unknown member", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("new")}, {MemberID: "unknown"}}, protocol.ErrInvalidRequest},
		{"duplicate member", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("new")}, {MemberID: "a"}}, protocol.ErrInvalidRequest},
		{"quota exceeded", []SyncGroupAssignment{{MemberID: "a", Assignment: make([]byte, 5000)}}, protocol.ErrThrottlingQuotaExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := DefaultStateLimits()
			limits.PerGroupBytes, limits.GroupBytes = 4096, 4096
			c, generation := newSyncAssignmentGroup(t, limits)
			aID, bID := syncAssignmentMemberIDs(t, c)
			oldA, oldB := []byte("old-a"), []byte("old-b")
			if _, code := c.SyncGroup("g", generation, aID, []SyncGroupAssignment{
				{MemberID: aID, Assignment: oldA}, {MemberID: bID, Assignment: oldB},
			}); code != protocol.ErrNone {
				t.Fatal(code)
			}
			generation = rejoinSyncAssignmentGroup(t, c)
			follower := waitSyncAssignment(t, c, generation)
			if _, code := c.SyncGroup("g", generation, aID, assignmentMemberIDs(tc.assignments, aID, bID)); code != tc.code {
				t.Fatalf("invalid snapshot error=%d, want %d", code, tc.code)
			}
			assertSyncAssignmentState(t, c, GroupAwaitingSync, oldA, oldB)
			g := c.FindGroup("g")
			g.mu.Lock()
			pending := g.pendingSyncs[bID] != nil
			g.mu.Unlock()
			if !pending {
				t.Fatal("rejected snapshot removed the waiting follower")
			}
			select {
			case result := <-follower:
				t.Fatalf("rejected snapshot completed follower: %+v", result)
			default:
			}
			newA, newB := []byte("new-a"), []byte("new-b")
			data, code := c.SyncGroup("g", generation, aID, []SyncGroupAssignment{
				{MemberID: aID, Assignment: newA}, {MemberID: bID, Assignment: newB},
			})
			if code != protocol.ErrNone || !bytes.Equal(data, newA) {
				t.Fatalf("retry sync=(%q, %d)", data, code)
			}
			receiveSyncAssignment(t, follower, newB)
			newA[0], newB[0], data[0] = 'x', 'y', 'z'
			assertSyncAssignmentState(t, c, GroupStable, []byte("new-a"), []byte("new-b"))
		})
	}
}

func TestSyncGroupSnapshotQuotaIncludesReleasedAssignments(t *testing.T) {
	protocols := []MemberProtocol{{Name: "range"}}
	const uuidLengthID = "00000000-0000-0000-0000-000000000000"
	base := groupCharge("g") + 2*int64(len("consumer")) + 2*memberCharge(uuidLengthID, protocols, nil)
	limits := DefaultStateLimits()
	limits.PerGroupBytes = base + 2*(128+512)
	limits.GroupBytes = limits.PerGroupBytes
	c, generation := newSyncAssignmentGroup(t, limits)
	aID, bID := syncAssignmentMemberIDs(t, c)
	if _, code := c.SyncGroup("g", generation, aID, []SyncGroupAssignment{
		{MemberID: aID, Assignment: bytes.Repeat([]byte("a"), 128)},
		{MemberID: bID, Assignment: bytes.Repeat([]byte("b"), 512)},
	}); code != protocol.ErrNone {
		t.Fatal(code)
	}
	generation = rejoinSyncAssignmentGroup(t, c)
	next := bytes.Repeat([]byte("c"), 640)
	if _, code := c.SyncGroup("g", generation, aID, []SyncGroupAssignment{{MemberID: aID, Assignment: next}}); code != protocol.ErrNone {
		t.Fatalf("snapshot did not reuse bytes released by omitted member: %d", code)
	}
	assertSyncAssignmentState(t, c, GroupStable, next, nil)
	if got := c.StateStats().GroupBytes; got != limits.GroupBytes {
		t.Fatalf("snapshot charge=%d, want exact ceiling %d", got, limits.GroupBytes)
	}
}
