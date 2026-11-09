package coordinator

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

type joinProtocolSnapshot struct {
	state                      GroupState
	generation                 int32
	protocolType, name, leader string
	retained                   int64
	members, joins, syncs      int
	member                     *Member
	pendingJoin                chan *JoinGroupResult
	rebalanceTimer             *groupTimer
	rebalanceSequence          uint64
}

func snapshotJoinProtocolGroup(c *Coordinator) joinProtocolSnapshot {
	g := c.FindGroup("g")
	g.mu.Lock()
	defer g.mu.Unlock()
	var member *Member
	if m := g.Members["a"]; m != nil {
		copyOf := *m
		member = &copyOf
	}
	return joinProtocolSnapshot{
		state: g.State, generation: g.GenerationID,
		protocolType: g.ProtocolType, name: g.ProtocolName, leader: g.LeaderID,
		retained: g.retained, members: len(g.Members), joins: len(g.pendingJoins), syncs: len(g.pendingSyncs),
		member: member, pendingJoin: g.pendingJoins["a"],
		rebalanceTimer: g.rebalanceTimer, rebalanceSequence: g.rebalanceSequence,
	}
}

func joinProtocolRequest(c *Coordinator, member, protocolType string, protocols []MemberProtocol) *JoinGroupResult {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	return c.JoinGroupContext(ctx, "g", member, 60000, protocolType, protocols)
}

func TestJoinGroupInvalidProtocolsDoNotCreateGroup(t *testing.T) {
	for _, tc := range []struct {
		name, protocolType string
		protocols          []MemberProtocol
	}{
		{"empty type", "", []MemberProtocol{{Name: "range"}}},
		{"nil protocols", "consumer", nil},
		{"empty protocols", "consumer", []MemberProtocol{}},
		{"empty name", "consumer", []MemberProtocol{{Name: ""}}},
		{"empty alternative", "consumer", []MemberProtocol{{Name: "range"}, {Name: ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCoordinator()
			t.Cleanup(c.Close)
			result := joinProtocolRequest(c, "", tc.protocolType, tc.protocols)
			if result.ErrorCode != protocol.ErrInconsistentGroupProtocol {
				t.Fatalf("join error=%d, want inconsistent group protocol", result.ErrorCode)
			}
			if stats := c.StateStats(); stats.Groups != 0 || stats.GroupBytes != 0 {
				t.Fatalf("invalid first join retained state: %+v", stats)
			}
		})
	}
}

func TestJoinGroupIncompatibleRequestPreservesStableGroup(t *testing.T) {
	for _, tc := range []struct {
		name, member, protocolType string
		protocols                  []MemberProtocol
	}{
		{"disjoint assignors", "b", "consumer", []MemberProtocol{{Name: "roundrobin"}}},
		{"different type", "b", "other", []MemberProtocol{{Name: "range"}}},
		{"existing member type change", "a", "other", []MemberProtocol{{Name: "range"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCoordinator()
			t.Cleanup(c.Close)
			joined := c.JoinGroup("g", "a", 60000, "consumer", []MemberProtocol{{Name: "range", Metadata: []byte("subscription")}})
			if joined.ErrorCode != protocol.ErrNone {
				t.Fatal(joined.ErrorCode)
			}
			if _, code := c.SyncGroup("g", joined.GenerationID, "a", []SyncGroupAssignment{{MemberID: "a", Assignment: []byte("P0,P1")}}); code != protocol.ErrNone {
				t.Fatal(code)
			}
			before, usage := snapshotJoinProtocolGroup(c), c.StateStats()
			result := joinProtocolRequest(c, tc.member, tc.protocolType, tc.protocols)
			if result.ErrorCode != protocol.ErrInconsistentGroupProtocol || result.MemberID != tc.member {
				t.Errorf("join result=%+v, want protocol rejection for %s", result, tc.member)
			}
			if after := snapshotJoinProtocolGroup(c); !reflect.DeepEqual(before, after) {
				t.Errorf("rejected join changed group: before=%+v after=%+v", before, after)
			}
			if c.StateStats() != usage {
				t.Error("rejected join changed quota usage")
			}
			if code := c.Heartbeat("g", joined.GenerationID, "a"); code != protocol.ErrNone {
				t.Errorf("healthy member heartbeat failed after rejection: %d", code)
			}
			if generation := rejoinSyncAssignmentGroup(t, c); generation != joined.GenerationID+1 {
				t.Fatalf("compatible retry generation=%d, want %d", generation, joined.GenerationID+1)
			}
		})
	}
}

func TestJoinGroupIncompatibleRequestPreservesPendingJoin(t *testing.T) {
	c := NewCoordinator()
	t.Cleanup(c.Close)
	c.RebalanceDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan *JoinGroupResult, 1)
	go func() {
		joined <- c.JoinGroupContext(ctx, "g", "a", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.pendingJoins["a"] != nil })
	before, usage := snapshotJoinProtocolGroup(c), c.StateStats()
	result := joinProtocolRequest(c, "b", "consumer", []MemberProtocol{{Name: "roundrobin"}})
	if result.ErrorCode != protocol.ErrInconsistentGroupProtocol {
		t.Errorf("join error=%d, want inconsistent group protocol", result.ErrorCode)
	}
	if after := snapshotJoinProtocolGroup(c); !reflect.DeepEqual(before, after) || c.StateStats() != usage {
		t.Error("incompatible member changed the pending join or its timers/accounting")
	}
	select {
	case result := <-joined:
		t.Fatalf("incompatible member completed the original join: %+v", result)
	default:
	}
	// Complete the original collection round without waiting for a real timer.
	g := c.FindGroup("g")
	g.mu.Lock()
	g.rebalanceTimer.Stop()
	g.rebalanceTimer = nil
	g.rebalanceSequence++
	g.completeJoinPhase()
	g.mu.Unlock()
	select {
	case result := <-joined:
		if result.ErrorCode != protocol.ErrNone || result.ProtocolName != "range" || result.GenerationID != 1 {
			t.Fatalf("original join did not complete normally: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("original join was stranded")
	}
}

func TestJoinGroupSelectsSharedAlternative(t *testing.T) {
	c := NewCoordinator()
	t.Cleanup(c.Close)
	aProtocols := []MemberProtocol{{Name: "range", Metadata: []byte("a-range")}, {Name: "roundrobin", Metadata: []byte("a-roundrobin")}}
	first := c.JoinGroup("g", "a", 60000, "consumer", aProtocols)
	if first.ErrorCode != protocol.ErrNone || first.ProtocolName != "range" {
		t.Fatalf("initial join=%+v", first)
	}
	joined := make(chan *JoinGroupResult, 1)
	go func() {
		joined <- c.JoinGroup("g", "b", 60000, "consumer", []MemberProtocol{{Name: "roundrobin", Metadata: []byte("b-roundrobin")}})
	}()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.pendingJoins["b"] != nil })
	a := c.JoinGroup("g", "a", 60000, "consumer", aProtocols)
	select {
	case b := <-joined:
		if a.ErrorCode != protocol.ErrNone || b.ErrorCode != protocol.ErrNone ||
			a.ProtocolName != "roundrobin" || b.ProtocolName != "roundrobin" || a.GenerationID != b.GenerationID {
			t.Fatalf("shared protocol not selected: a=%+v b=%+v", a, b)
		}
		want := []JoinGroupMember{{MemberID: "a", Metadata: []byte("a-roundrobin")}, {MemberID: "b", Metadata: []byte("b-roundrobin")}}
		if !reflect.DeepEqual(a.Members, want) {
			t.Fatalf("leader metadata=%+v, want selected-protocol metadata %+v", a.Members, want)
		}
	case <-time.After(time.Second):
		t.Fatal("compatible member did not join")
	}
	before, usage := snapshotJoinProtocolGroup(c), c.StateStats()
	result := joinProtocolRequest(c, "a", "consumer", []MemberProtocol{{Name: "range", Metadata: []byte("changed")}})
	if result.ErrorCode != protocol.ErrInconsistentGroupProtocol {
		t.Errorf("incompatible capability update error=%d", result.ErrorCode)
	}
	if !reflect.DeepEqual(before, snapshotJoinProtocolGroup(c)) || c.StateStats() != usage {
		t.Error("rejected capability update changed existing member or group")
	}
}

func TestJoinGroupMayReplaceOwnCapabilitiesAndReuseEmptyGroup(t *testing.T) {
	c := NewCoordinator()
	t.Cleanup(c.Close)
	first := c.JoinGroup("g", "a", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	rejoined := c.JoinGroup("g", "a", 60000, "consumer", []MemberProtocol{{Name: "custom-assignor"}})
	if rejoined.ErrorCode != protocol.ErrNone || rejoined.ProtocolName != "custom-assignor" || rejoined.GenerationID != first.GenerationID+1 {
		t.Fatalf("sole member could not replace its capabilities: %+v", rejoined)
	}
	if code := c.LeaveGroup("g", "a"); code != protocol.ErrNone {
		t.Fatal(code)
	}
	joined := c.JoinGroup("g", "b", 60000, "other", []MemberProtocol{{Name: "other-assignor"}})
	if joined.ErrorCode != protocol.ErrNone || joined.ProtocolName != "other-assignor" {
		t.Fatalf("empty group could not select a new protocol type: %+v", joined)
	}
}
