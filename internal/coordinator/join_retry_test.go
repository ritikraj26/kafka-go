package coordinator

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func retryProtocols(member string) []MemberProtocol {
	return []MemberProtocol{
		{Name: "range", Metadata: []byte(member + "-range")},
		{Name: "roundrobin", Metadata: []byte(member + "-roundrobin")},
	}
}

func newJoinRetryGroup(t *testing.T) (*Coordinator, *JoinGroupResult, *JoinGroupResult) {
	t.Helper()
	c := newDeadlineCoordinator(t)
	first := c.JoinGroup("g", "", 60000, "consumer", retryProtocols("a"))
	if first.ErrorCode != protocol.ErrNone {
		t.Fatalf("first join: %+v", first)
	}
	follower := make(chan *JoinGroupResult, 1)
	go func() { follower <- c.JoinGroup("g", "", 60000, "consumer", retryProtocols("b")) }()
	waitPendingJoinMember(t, c, first.MemberID)
	a := c.JoinGroup("g", first.MemberID, 60000, "consumer", retryProtocols("a"))
	b := receiveDeadlineJoin(t, follower)
	if a.ErrorCode != protocol.ErrNone || a.LeaderID != a.MemberID || b.LeaderID != a.MemberID || a.GenerationID != b.GenerationID {
		t.Fatalf("retry fixture: a=%+v b=%+v", a, b)
	}
	return c, a, b
}

func retryJoin(t *testing.T, c *Coordinator, member string, protocols []MemberProtocol) *JoinGroupResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.JoinGroupContext(ctx, "g", member, 60000, "consumer", protocols)
}

func TestJoinRetryPreservesAwaitingSyncRound(t *testing.T) {
	for _, role := range []string{"leader", "follower"} {
		t.Run(role, func(t *testing.T) {
			c, a, b := newJoinRetryGroup(t)
			member, expected, protocols := a.MemberID, a, retryProtocols("a")
			if role == "follower" {
				member, expected, protocols = b.MemberID, b, retryProtocols("b")
			}
			waiting := waitSyncAssignment(t, c, a.GenerationID)
			g := c.FindGroup("g")
			g.mu.Lock()
			timer, sequence, deadline := g.phaseTimer, g.phaseSequence, g.phaseDeadline
			pending := g.pendingSyncs[b.MemberID]
			original := g.Members[member]
			attempt := original.joinAttempt
			g.mu.Unlock()
			usage := c.StateStats()
			for i := 0; i < 3; i++ {
				result := retryJoin(t, c, member, protocols)
				if !reflect.DeepEqual(result, expected) {
					t.Fatalf("retry result=%+v, want current round %+v", result, expected)
				}
				g.mu.Lock()
				unchanged := g.State == GroupAwaitingSync && g.GenerationID == a.GenerationID &&
					g.phaseTimer == timer && g.phaseSequence == sequence && g.phaseDeadline.Equal(deadline) &&
					g.pendingSyncs[b.MemberID] == pending && len(g.pendingJoins) == 0 && g.Members[member].joinAttempt == attempt+uint64(i)+1
				g.mu.Unlock()
				if !unchanged || c.StateStats() != usage {
					t.Fatal("unchanged retry restarted the round, displaced its waiter, or changed retained state")
				}
				select {
				case result := <-waiting:
					t.Fatalf("retry completed the pending follower sync: %+v", result)
				default:
				}
				if role == "leader" {
					// Each response owns its member list and metadata bytes.
					result.Members[0].Metadata[0] = 'x'
					result.Members[0].MemberID = "changed by caller"
				}
			}
			// The retry supersedes cancellation from the request whose response
			// was lost, even when that cleanup resumes after the replay.
			c.cancelJoin(g, original, attempt)
			g.mu.Lock()
			retained := g.Members[member] == original && g.State == GroupAwaitingSync && g.pendingSyncs[b.MemberID] == pending
			g.mu.Unlock()
			if !retained {
				t.Fatal("old Join cancellation removed a member after a successful retry")
			}
			if _, code := c.SyncGroup("g", a.GenerationID, a.MemberID, []SyncGroupAssignment{
				{MemberID: a.MemberID, Assignment: []byte("P0")},
				{MemberID: b.MemberID, Assignment: []byte("P1")},
			}); code != protocol.ErrNone {
				t.Fatalf("leader could not complete the original round: %d", code)
			}
			receiveSyncAssignment(t, waiting, []byte("P1"))
		})
	}
}

func TestJoinRetryKeepsOriginalSessionTimeout(t *testing.T) {
	c, a, _ := newJoinRetryGroup(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := c.JoinGroupContext(ctx, "g", a.MemberID, 120000, "consumer", retryProtocols("a"))
	if result.ErrorCode != protocol.ErrNone || result.GenerationID != a.GenerationID {
		t.Fatalf("timeout-only retry restarted assignment: %+v", result)
	}
	g := c.FindGroup("g")
	g.mu.Lock()
	timeout := g.Members[a.MemberID].SessionTimeout
	g.mu.Unlock()
	if timeout != time.Minute {
		t.Fatalf("timeout-only retry changed the stored session timeout to %v", timeout)
	}
}

func TestJoinRetryDistinguishesNullAndEmptyMetadata(t *testing.T) {
	c := newDeadlineCoordinator(t)
	first := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != protocol.ErrNone {
		t.Fatal(first.ErrorCode)
	}
	changed := retryJoin(t, c, first.MemberID, []MemberProtocol{{Name: "range", Metadata: []byte{}}})
	if changed.ErrorCode != protocol.ErrNone || changed.GenerationID != first.GenerationID+1 {
		t.Fatalf("null-to-empty metadata change did not start a new round: %+v", changed)
	}
}

func TestJoinRetriesCannotExtendSyncDeadline(t *testing.T) {
	c, a, b := newJoinRetryGroup(t)
	waiting := waitSyncAssignment(t, c, a.GenerationID)
	g := c.FindGroup("g")
	g.mu.Lock()
	sequence := g.phaseSequence
	g.mu.Unlock()
	for i := 0; i < 3; i++ {
		for _, member := range []struct {
			id, label string
		}{{a.MemberID, "a"}, {b.MemberID, "b"}} {
			if result := retryJoin(t, c, member.id, retryProtocols(member.label)); result.ErrorCode != protocol.ErrNone || result.GenerationID != a.GenerationID {
				t.Fatalf("retry did not return the current generation: %+v", result)
			}
		}
	}
	// The originally scheduled callback must still expire this same round.
	g.mu.Lock()
	g.expirePhase(GroupAwaitingSync, a.GenerationID, sequence)
	recovered := g.State == GroupJoining && g.Members[a.MemberID] == nil && g.Members[b.MemberID] != nil
	g.mu.Unlock()
	if !recovered {
		t.Fatal("Join retries invalidated the original assignment deadline")
	}
	select {
	case result := <-waiting:
		if result.code != protocol.ErrRebalanceInProgress {
			t.Fatalf("expired sync error=%d", result.code)
		}
	case <-time.After(time.Second):
		t.Fatal("assignment deadline left the follower waiting")
	}
	joined := c.JoinGroup("g", b.MemberID, 60000, "consumer", retryProtocols("b"))
	if joined.ErrorCode != protocol.ErrNone || joined.GenerationID != a.GenerationID+1 || joined.LeaderID != b.MemberID {
		t.Fatalf("survivor could not begin the next generation: %+v", joined)
	}
}

func TestChangedJoinMetadataStillRebalancesAwaitingSyncGroup(t *testing.T) {
	for _, change := range []string{"metadata", "protocol order"} {
		t.Run(change, func(t *testing.T) {
			c, a, b := newJoinRetryGroup(t)
			waiting := waitSyncAssignment(t, c, a.GenerationID)
			changed := retryProtocols("a")
			if change == "metadata" {
				changed[0].Metadata = []byte("changed subscription")
			} else {
				changed[0], changed[1] = changed[1], changed[0]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan *JoinGroupResult, 1)
			go func() { result <- c.JoinGroupContext(ctx, "g", a.MemberID, 60000, "consumer", changed) }()
			waitGroupState(t, c, func(g *ConsumerGroup) bool {
				return g.State == GroupJoining && g.pendingJoins[a.MemberID] != nil
			})
			select {
			case synced := <-waiting:
				if synced.code != protocol.ErrRebalanceInProgress {
					t.Fatalf("changed join did not invalidate old sync: %+v", synced)
				}
			case <-time.After(time.Second):
				t.Fatal("changed join left the old sync waiting")
			}
			joinedB := c.JoinGroup("g", b.MemberID, 60000, "consumer", retryProtocols("b"))
			joinedA := receiveDeadlineJoin(t, result)
			if joinedA.GenerationID != a.GenerationID+1 || joinedB.GenerationID != joinedA.GenerationID {
				t.Fatalf("changed join did not advance the round: a=%+v b=%+v", joinedA, joinedB)
			}
		})
	}
}
