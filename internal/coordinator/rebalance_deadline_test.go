package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func newDeadlineCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c := NewCoordinator()
	c.JoinTimeout, c.SyncTimeout = time.Hour, time.Hour
	t.Cleanup(c.Close)
	return c
}

func deadlinePair(t *testing.T, c *Coordinator) int32 {
	t.Helper()
	first := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != protocol.ErrNone {
		t.Fatalf("initial join error=%d", first.ErrorCode)
	}
	return rejoinSyncAssignmentGroup(t, c)
}

func waitPendingJoinMember(t *testing.T, c *Coordinator, except string) string {
	t.Helper()
	var memberID string
	waitGroupState(t, c, func(g *ConsumerGroup) bool {
		for id := range g.pendingJoins {
			if id != except {
				memberID = id
				return true
			}
		}
		return false
	})
	return memberID
}

func receiveDeadlineJoin(t *testing.T, result <-chan *JoinGroupResult) *JoinGroupResult {
	t.Helper()
	select {
	case joined := <-result:
		if joined.ErrorCode != protocol.ErrNone {
			t.Fatalf("join error=%d", joined.ErrorCode)
		}
		return joined
	case <-time.After(time.Second):
		t.Fatal("join did not complete at its group deadline")
		return nil
	}
}

func TestJoinDeadlineIsNotExtendedByHeartbeats(t *testing.T) {
	c := newDeadlineCoordinator(t)
	c.JoinTimeout = 120 * time.Millisecond
	first := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != protocol.ErrNone {
		t.Fatal(first.ErrorCode)
	}
	a := first.MemberID
	if _, code := c.SyncGroup("g", first.GenerationID, a, nil); code != protocol.ErrNone {
		t.Fatal(code)
	}
	joined := make(chan *JoinGroupResult, 1)
	go func() {
		joined <- c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	b := waitPendingJoinMember(t, c, a)
	g := c.FindGroup("g")
	g.mu.Lock()
	deadline, sequence := g.phaseDeadline, g.phaseSequence
	g.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	heartbeats := 0
	for {
		select {
		case <-ticker.C:
			// A remains responsive but never participates in JoinGroup.
			c.Heartbeat("g", first.GenerationID, a)
			heartbeats++
			g.mu.Lock()
			changed := g.State == GroupJoining && (g.phaseSequence != sequence || !g.phaseDeadline.Equal(deadline))
			g.mu.Unlock()
			if changed {
				t.Fatal("a heartbeat extended the join phase")
			}
		case result := <-joined:
			if heartbeats == 0 || result.ErrorCode != protocol.ErrNone || result.LeaderID != b || result.GenerationID != first.GenerationID+1 {
				t.Fatalf("join did not recover from the missing participant: heartbeats=%d result=%+v", heartbeats, result)
			}
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.Members[a] != nil || g.Members[b] == nil || g.State != GroupAwaitingSync {
				t.Fatalf("join expiry left state=%s members=%v", g.State, g.sortedMemberIDs())
			}
			return
		case <-timeout.C:
			t.Fatal("heartbeats kept an incomplete join phase alive")
		}
	}
}

func TestSyncDeadlineFencesLeaderDespiteHeartbeats(t *testing.T) {
	c := newDeadlineCoordinator(t)
	c.SyncTimeout = 150 * time.Millisecond
	generation := deadlinePair(t, c)
	a, b := syncAssignmentMemberIDs(t, c)
	follower := waitSyncAssignment(t, c, generation)
	g := c.FindGroup("g")
	g.mu.Lock()
	deadline, sequence := g.phaseDeadline, g.phaseSequence
	g.mu.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	heartbeats := 0
	for {
		select {
		case <-ticker.C:
			// Both members heartbeat, but A never supplies an assignment snapshot.
			c.Heartbeat("g", generation, a)
			c.Heartbeat("g", generation, b)
			heartbeats++
			g.mu.Lock()
			changed := g.State == GroupAwaitingSync && (g.phaseSequence != sequence || !g.phaseDeadline.Equal(deadline))
			g.mu.Unlock()
			if changed {
				t.Fatal("a heartbeat extended the sync phase")
			}
		case result := <-follower:
			if heartbeats == 0 || result.code != protocol.ErrRebalanceInProgress {
				t.Fatalf("sync expiry result=%+v heartbeats=%d", result, heartbeats)
			}
			g.mu.Lock()
			recovered := g.State == GroupJoining && g.Members[a] == nil && g.Members[b] != nil && len(g.pendingSyncs) == 0
			g.mu.Unlock()
			if !recovered {
				t.Fatal("sync expiry did not remove the stalled leader and begin a new join")
			}
			if _, code := c.SyncGroup("g", generation, a, nil); code != protocol.ErrUnknownMemberID {
				t.Fatalf("expired leader sync error=%d", code)
			}
			joined := c.JoinGroup("g", b, 60000, "consumer", []MemberProtocol{{Name: "range"}})
			if joined.ErrorCode != protocol.ErrNone || joined.LeaderID != b || joined.GenerationID != generation+1 {
				t.Fatalf("survivor could not become leader: %+v", joined)
			}
			if data, code := c.SyncGroup("g", joined.GenerationID, b, []SyncGroupAssignment{{MemberID: b, Assignment: []byte("P0")}}); code != protocol.ErrNone || string(data) != "P0" {
				t.Fatalf("new leader sync=(%q, %d)", data, code)
			}
			return
		case <-timeout.C:
			t.Fatal("heartbeats kept the stalled sync leader alive")
		}
	}
}

func TestPhaseExpiryRejectsStaleStateGenerationAndSequence(t *testing.T) {
	c := newDeadlineCoordinator(t)
	generation := deadlinePair(t, c)
	a, _ := syncAssignmentMemberIDs(t, c)
	g := c.FindGroup("g")
	g.mu.Lock()
	sequence, deadline := g.phaseSequence, g.phaseDeadline
	for _, stale := range []struct {
		state      GroupState
		generation int32
		sequence   uint64
	}{
		{GroupJoining, generation, sequence},
		{GroupAwaitingSync, generation + 1, sequence},
		{GroupAwaitingSync, generation, sequence + 1},
	} {
		g.expirePhase(stale.state, stale.generation, stale.sequence)
		if g.State != GroupAwaitingSync || len(g.Members) != 2 || g.GenerationID != generation || g.phaseSequence != sequence || !g.phaseDeadline.Equal(deadline) {
			g.mu.Unlock()
			t.Fatalf("stale callback changed the current phase: %+v", stale)
		}
	}
	g.mu.Unlock()
	if _, code := c.SyncGroup("g", generation, a, nil); code != protocol.ErrNone {
		t.Fatal(code)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expirePhase(GroupAwaitingSync, generation, sequence)
	if g.State != GroupStable || len(g.Members) != 2 || g.phaseTimer != nil || !g.phaseDeadline.IsZero() || g.phaseSequence == sequence {
		t.Fatal("an expired sync callback changed a successfully stabilized group")
	}
}

func TestJoinDeadlineSurvivesRepeatedRebalanceTriggers(t *testing.T) {
	c := newDeadlineCoordinator(t)
	generation := deadlinePair(t, c)
	a, b := syncAssignmentMemberIDs(t, c)
	if _, code := c.SyncGroup("g", generation, a, nil); code != protocol.ErrNone {
		t.Fatal(code)
	}
	joined := make(chan *JoinGroupResult, 1)
	go func() {
		joined <- c.JoinGroup("g", b, 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.pendingJoins[b] != nil })
	g.mu.Lock()
	sequence, deadline := g.phaseSequence, g.phaseDeadline
	g.triggerRebalance()
	g.triggerRebalance()
	if g.phaseSequence != sequence || !g.phaseDeadline.Equal(deadline) {
		g.mu.Unlock()
		t.Fatal("a repeated rebalance trigger reset an in-progress join deadline")
	}
	g.expirePhase(GroupJoining, generation, sequence)
	newSequence := g.phaseSequence
	g.expirePhase(GroupJoining, generation, sequence)
	if g.State != GroupAwaitingSync || g.GenerationID != generation+1 || g.phaseSequence != newSequence || len(g.Members) != 1 || g.Members[b] == nil {
		g.mu.Unlock()
		t.Fatal("join expiry or its stale callback changed the surviving generation")
	}
	g.mu.Unlock()
	result := receiveDeadlineJoin(t, joined)
	if result.LeaderID != b || result.GenerationID != generation+1 {
		t.Fatalf("unexpected survivor join: %+v", result)
	}
}

func TestOldPhaseCallbackCannotExpireSameGenerationAfterGroupBecomesEmpty(t *testing.T) {
	c := newDeadlineCoordinator(t)
	c.RebalanceDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan *JoinGroupResult, 1)
	go func() {
		first <- c.JoinGroupContext(ctx, "g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	waitPendingJoinMember(t, c, "")
	g := c.FindGroup("g")
	g.mu.Lock()
	generation, sequence := g.GenerationID, g.phaseSequence
	g.mu.Unlock()
	cancel()
	select {
	case result := <-first:
		if result.ErrorCode != protocol.ErrRequestTimedOut {
			t.Fatalf("cancelled join error=%d", result.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled join did not complete")
	}
	g.mu.Lock()
	empty := g.State == GroupEmpty && g.phaseTimer == nil && g.phaseDeadline.IsZero()
	g.mu.Unlock()
	if !empty {
		t.Fatal("the last member's removal left a live phase deadline")
	}
	second := make(chan *JoinGroupResult, 1)
	go func() {
		second <- c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	b := waitPendingJoinMember(t, c, "")
	current := c.FindGroup("g")
	if current != g {
		t.Fatal("the existing empty group was unexpectedly replaced")
	}
	g.mu.Lock()
	newSequence, deadline := g.phaseSequence, g.phaseDeadline
	g.expirePhase(GroupJoining, generation, sequence)
	if g.State != GroupJoining || g.GenerationID != generation || g.phaseSequence != newSequence || !g.phaseDeadline.Equal(deadline) || g.pendingJoins[b] == nil || len(g.Members) != 1 {
		g.mu.Unlock()
		t.Fatal("an old callback expired a new phase with the same state and generation")
	}
	g.completeJoinPhase()
	g.mu.Unlock()
	receiveDeadlineJoin(t, second)
}

func TestJoinCollectionCallbackRequiresEveryMemberToHaveJoined(t *testing.T) {
	c := newDeadlineCoordinator(t)
	c.RebalanceDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan *JoinGroupResult, 2)
	go func() {
		joined <- c.JoinGroupContext(ctx, "g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.rebalanceTimer != nil })
	a := waitPendingJoinMember(t, c, "")
	go func() {
		joined <- c.JoinGroupContext(ctx, "g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	waitPendingJoinMember(t, c, a)
	g.mu.Lock()
	// Model a queued collection callback whose membership eligibility has changed.
	// The callback must recheck eligibility after acquiring the group lock.
	waiter := g.pendingJoins[a]
	delete(g.pendingJoins, a)
	g.rebalanceTimer.timer.Reset(time.Millisecond)
	g.mu.Unlock()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.rebalanceTimer == nil })
	g.mu.Lock()
	if g.State != GroupJoining || g.GenerationID != 0 || len(g.Members) != 2 {
		g.mu.Unlock()
		t.Fatal("a collection callback completed a generation with a missing JoinGroup")
	}
	g.pendingJoins[a] = waiter
	g.mu.Unlock()
	select {
	case result := <-joined:
		t.Fatalf("missing member unexpectedly received a completed join: %+v", result)
	default:
	}
}

func TestPendingRebalanceRPCsOutliveTheirSessionTimeout(t *testing.T) {
	t.Run("join", func(t *testing.T) {
		c := newDeadlineCoordinator(t)
		c.RebalanceDelay = time.Hour
		joined := make(chan *JoinGroupResult, 1)
		go func() {
			joined <- c.JoinGroup("g", "", 20, "consumer", []MemberProtocol{{Name: "range"}})
		}()
		a := waitPendingJoinMember(t, c, "")
		g := c.FindGroup("g")
		select {
		case result := <-joined:
			t.Fatalf("pending join used its session timeout: %+v", result)
		case <-time.After(60 * time.Millisecond):
		}
		g.mu.Lock()
		if g.Members[a] == nil || g.pendingJoins[a] == nil {
			g.mu.Unlock()
			t.Fatal("session expiry removed a participant waiting in JoinGroup")
		}
		lastHeartbeat := g.Members[a].LastHeartbeat
		// The short session has served its purpose; allow scheduling delays
		// while checking completion and the newly refreshed session.
		g.Members[a].SessionTimeout = time.Hour
		g.completeJoinPhase()
		refreshed := g.Members[a].LastHeartbeat.After(lastHeartbeat)
		g.mu.Unlock()
		if !refreshed {
			t.Fatal("join completion did not refresh the participant's session")
		}
		result := receiveDeadlineJoin(t, joined)
		if _, code := c.SyncGroup("g", result.GenerationID, a, nil); code != protocol.ErrNone {
			t.Fatal(code)
		}
	})
	t.Run("sync", func(t *testing.T) {
		c := newDeadlineCoordinator(t)
		generation := deadlinePair(t, c)
		a, b := syncAssignmentMemberIDs(t, c)
		g := c.FindGroup("g")
		g.mu.Lock()
		g.Members[b].SessionTimeout = 20 * time.Millisecond
		g.mu.Unlock()
		follower := waitSyncAssignment(t, c, generation)
		select {
		case result := <-follower:
			t.Fatalf("pending sync used its session timeout: %+v", result)
		case <-time.After(60 * time.Millisecond):
		}
		g.mu.Lock()
		waiting := g.Members[b] != nil && g.pendingSyncs[b] != nil
		var lastHeartbeat time.Time
		if waiting {
			lastHeartbeat = g.Members[b].LastHeartbeat
			g.Members[b].SessionTimeout = time.Hour
		}
		g.mu.Unlock()
		if !waiting {
			t.Fatal("session expiry removed a participant waiting in SyncGroup")
		}
		if _, code := c.SyncGroup("g", generation, a, []SyncGroupAssignment{{MemberID: b, Assignment: []byte("P1")}}); code != protocol.ErrNone {
			t.Fatal(code)
		}
		receiveSyncAssignment(t, follower, []byte("P1"))
		g.mu.Lock()
		refreshed := g.Members[b].LastHeartbeat.After(lastHeartbeat)
		g.mu.Unlock()
		if !refreshed {
			t.Fatal("sync completion did not refresh the participant's session")
		}
	})
}

func TestCloseDrainsRunningPhaseCallback(t *testing.T) {
	c := newDeadlineCoordinator(t)
	g := c.getOrCreateGroup("g")
	entered, release := make(chan struct{}), make(chan struct{})
	g.mu.Lock()
	g.State = GroupJoining
	g.phaseSequence++
	g.phaseDeadline = time.Now().Add(time.Hour)
	sequence := g.phaseSequence
	g.phaseTimer = g.after(0, func() { close(entered); <-release })
	g.mu.Unlock()
	<-entered
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
		close(release)
		t.Fatal("close returned while a phase callback was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not drain the completed phase callback")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.expirePhase(GroupJoining, 0, sequence)
	if g.State != GroupDead || g.phaseTimer != nil || !g.phaseDeadline.IsZero() || g.phaseSequence == sequence {
		t.Fatal("close left an active rebalance deadline")
	}
}

func TestPhaseExpiryWithNoSurvivorsReleasesMembershipState(t *testing.T) {
	for _, state := range []GroupState{GroupJoining, GroupAwaitingSync} {
		t.Run(state.String(), func(t *testing.T) {
			c := newDeadlineCoordinator(t)
			joined := c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
			if joined.ErrorCode != protocol.ErrNone {
				t.Fatal(joined.ErrorCode)
			}
			g := c.FindGroup("g")
			g.mu.Lock()
			if state == GroupJoining {
				g.triggerRebalance()
			}
			g.expirePhase(state, g.GenerationID, g.phaseSequence)
			if g.State != GroupEmpty || len(g.Members) != 0 || len(g.pendingJoins) != 0 || len(g.pendingSyncs) != 0 || g.ProtocolType != "" || g.retained != groupCharge("g") || g.phaseTimer != nil || g.rebalanceTimer != nil || !g.phaseDeadline.IsZero() {
				g.mu.Unlock()
				t.Fatal("expiry without survivors retained membership state, charges, or timers")
			}
			g.mu.Unlock()
			if got := c.StateStats().GroupBytes; got != groupCharge("g") {
				t.Fatalf("group bytes after expiry=%d, want %d", got, groupCharge("g"))
			}
			joined = c.JoinGroup("g", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
			if joined.ErrorCode != protocol.ErrNone || joined.LeaderID != joined.MemberID {
				t.Fatalf("empty group could not be reused: %+v", joined)
			}
			if _, code := c.SyncGroup("g", joined.GenerationID, joined.MemberID, nil); code != protocol.ErrNone {
				t.Fatalf("reused group sync error=%d", code)
			}
		})
	}
}

func TestCanceledSyncResumesOriginalExpiredSession(t *testing.T) {
	c := newDeadlineCoordinator(t)
	generation := deadlinePair(t, c)
	_, b := syncAssignmentMemberIDs(t, c)
	g := c.FindGroup("g")
	g.mu.Lock()
	member := g.Members[b]
	member.SessionTimeout = 20 * time.Millisecond
	g.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan int16, 1)
	go func() {
		_, code := c.SyncGroupContext(ctx, "g", generation, b, nil)
		result <- code
	}()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.pendingSyncs[b] != nil })
	g.mu.Lock()
	lastHeartbeat := member.LastHeartbeat
	g.mu.Unlock()
	select {
	case code := <-result:
		t.Fatalf("pending sync ended at its session deadline: %d", code)
	case <-time.After(60 * time.Millisecond):
	}
	cancel()
	select {
	case code := <-result:
		if code != protocol.ErrRequestTimedOut {
			t.Fatalf("canceled sync error=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("sync ignored cancellation")
	}
	g.mu.Lock()
	unchanged := member.LastHeartbeat.Equal(lastHeartbeat)
	g.mu.Unlock()
	if !unchanged {
		t.Fatal("canceling SyncGroup refreshed the original liveness deadline")
	}
	waitGroupState(t, c, func(g *ConsumerGroup) bool {
		return g.Members[b] == nil && g.State == GroupJoining && g.phaseTimer != nil && !g.phaseDeadline.IsZero()
	})
}
