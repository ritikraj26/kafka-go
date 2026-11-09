package coordinator

import (
	"reflect"
	"testing"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func TestRemovedMemberCannotRecreateMembership(t *testing.T) {
	for _, state := range []GroupState{GroupEmpty, GroupJoining, GroupAwaitingSync, GroupStable} {
		t.Run(state.String(), func(t *testing.T) {
			c, _ := newSyncAssignmentGroup(t, DefaultStateLimits())
			a, b := syncAssignmentMemberIDs(t, c)
			if code := c.LeaveGroup("g", a); code != protocol.ErrNone {
				t.Fatal(code)
			}
			if state == GroupEmpty {
				if code := c.LeaveGroup("g", b); code != protocol.ErrNone {
					t.Fatal(code)
				}
			} else if state != GroupJoining {
				joined := c.JoinGroup("g", b, 60000, "consumer", []MemberProtocol{{Name: "range"}})
				if joined.ErrorCode != protocol.ErrNone {
					t.Fatal(joined.ErrorCode)
				}
				if state == GroupStable {
					if _, code := c.SyncGroup("g", joined.GenerationID, b, nil); code != protocol.ErrNone {
						t.Fatal(code)
					}
				}
			}
			before, usage := snapshotJoinProtocolGroup(c), c.StateStats()
			if before.state != state {
				t.Fatalf("fixture state=%v, want %v", before.state, state)
			}
			g := c.FindGroup("g")
			g.mu.Lock()
			phaseTimer, sequence, deadline := g.phaseTimer, g.phaseSequence, g.phaseDeadline
			g.mu.Unlock()
			result := c.JoinGroup("g", a, 60000, "consumer", []MemberProtocol{{Name: "range"}})
			if result.ErrorCode != protocol.ErrUnknownMemberID || result.MemberID != a {
				t.Fatalf("removed member join=%+v, want unknown member", result)
			}
			if !reflect.DeepEqual(before, snapshotJoinProtocolGroup(c)) || c.StateStats() != usage {
				t.Fatal("rejected stale join changed membership, timers, or accounting")
			}
			g.mu.Lock()
			phaseUnchanged := g.phaseTimer == phaseTimer && g.phaseSequence == sequence && g.phaseDeadline.Equal(deadline)
			g.mu.Unlock()
			if !phaseUnchanged {
				t.Fatal("rejected stale join changed the phase deadline")
			}
			if state == GroupEmpty {
				fresh := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
				if fresh.ErrorCode != protocol.ErrNone || fresh.MemberID == "" || fresh.MemberID == a || fresh.MemberID == b {
					t.Fatalf("fresh admission did not get a new identity: %+v", fresh)
				}
			}
		})
	}
}

func TestUnknownMemberDoesNotClaimInitializingGroup(t *testing.T) {
	c := NewCoordinator()
	t.Cleanup(c.Close)
	g := c.getOrCreateGroup("g")
	before := c.StateStats()
	result := c.JoinGroup("g", "unknown", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if result.ErrorCode != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown member error=%d", result.ErrorCode)
	}
	g.mu.Lock()
	unchanged := g.initializing && g.State == GroupEmpty && len(g.Members) == 0 && len(g.pendingJoins) == 0 && g.phaseTimer == nil
	g.mu.Unlock()
	if !unchanged || c.StateStats() != before {
		t.Fatal("unknown member interfered with an initializing group")
	}
	fresh := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if fresh.ErrorCode != protocol.ErrNone {
		t.Fatalf("fresh join after stale request failed: %+v", fresh)
	}
}

func TestOldJoinCleanupCannotRemoveReplacementInstance(t *testing.T) {
	c := NewCoordinator()
	t.Cleanup(c.Close)
	joined := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if joined.ErrorCode != protocol.ErrNone {
		t.Fatal(joined.ErrorCode)
	}
	g := c.FindGroup("g")
	g.mu.Lock()
	old := g.Members[joined.MemberID]
	attempt := old.joinAttempt
	// Model a replacement with the same ID and reset attempt count. Admission
	// now prevents this reuse; cleanup must independently reject the old object.
	replacement := *old
	g.Members[joined.MemberID] = &replacement
	g.mu.Unlock()
	before, usage := snapshotJoinProtocolGroup(c), c.StateStats()
	c.cancelJoin(g, old, attempt)
	if !reflect.DeepEqual(before, snapshotJoinProtocolGroup(c)) || c.StateStats() != usage {
		t.Fatal("delayed cleanup removed or changed a replacement membership")
	}
	g.mu.Lock()
	retained := g.Members[joined.MemberID] == &replacement
	g.mu.Unlock()
	if !retained {
		t.Fatal("replacement member instance was removed")
	}
	// Matching cleanup still removes its own member and releases its resources.
	c.cancelJoin(g, &replacement, attempt)
	g.mu.Lock()
	empty := g.State == GroupEmpty && len(g.Members) == 0 && g.phaseTimer == nil
	g.mu.Unlock()
	if !empty || c.StateStats().GroupBytes != groupCharge("g") {
		t.Fatal("matching cleanup did not release membership state")
	}
}
