package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func waitGroupState(t *testing.T, c *Coordinator, ready func(*ConsumerGroup) bool) *ConsumerGroup {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if g := c.FindGroup("g"); g != nil {
			g.mu.Lock()
			ok := ready(g)
			g.mu.Unlock()
			if ok {
				return g
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("group did not reach expected wait state")
	return nil
}

func TestCancelledJoinCleansMembershipAndTimers(t *testing.T) {
	c := NewCoordinator()
	defer c.Close()
	c.RebalanceDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan *JoinGroupResult, 1)
	go func() {
		result <- c.JoinGroupContext(ctx, "g", "", 3600000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return len(g.pendingJoins) == 1 })
	cancel()
	select {
	case joined := <-result:
		if joined.ErrorCode != protocol.ErrRequestTimedOut {
			t.Fatalf("cancelled join error=%d", joined.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("join ignored cancellation")
	}
	g.mu.Lock()
	empty := len(g.Members) == 0 && len(g.pendingJoins) == 0 && g.rebalanceTimer == nil
	g.mu.Unlock()
	if !empty {
		t.Fatal("cancelled join left membership, waiter or rebalance timer")
	}
}

func TestCancelledSyncCanBeRetried(t *testing.T) {
	c, generation := newSyncAssignmentGroup(t, DefaultStateLimits())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan int16, 1)
	go func() { _, code := c.SyncGroupContext(ctx, "g", generation, "b", nil); result <- code }()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return len(g.pendingSyncs) == 1 })
	cancel()
	select {
	case code := <-result:
		if code != protocol.ErrRequestTimedOut {
			t.Fatalf("sync error=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("sync ignored cancellation")
	}
	g.mu.Lock()
	pending := len(g.pendingSyncs)
	g.mu.Unlock()
	if pending != 0 {
		t.Fatal("cancelled sync remained pending")
	}
	retried := waitSyncAssignment(t, c, generation)
	data, code := c.SyncGroup("g", generation, "a", []SyncGroupAssignment{
		{MemberID: "a", Assignment: []byte("leader")},
		{MemberID: "b", Assignment: []byte("assigned")},
	})
	if code != 0 || string(data) != "leader" {
		t.Fatalf("sync retry failed: %d", code)
	}
	receiveSyncAssignment(t, retried, []byte("assigned"))
}

func TestOldJoinCancellationDoesNotRemoveNewAttempt(t *testing.T) {
	c := NewCoordinator()
	defer c.Close()
	c.RebalanceDelay = time.Hour
	first, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	second, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	a, b := make(chan *JoinGroupResult, 1), make(chan *JoinGroupResult, 1)
	go func() {
		a <- c.JoinGroupContext(first, "g", "member", 3600000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return len(g.pendingJoins) == 1 })
	go func() {
		b <- c.JoinGroupContext(second, "g", "member", 3600000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	waitGroupState(t, c, func(g *ConsumerGroup) bool { return g.Members["member"].joinAttempt == 2 })
	cancelFirst()
	select {
	case result := <-a:
		if result.ErrorCode == 0 {
			t.Fatal("superseded join succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("superseded join did not wake")
	}
	if g.MemberCount() != 1 {
		t.Fatal("old cancellation removed the replacement join")
	}
	cancelSecond()
	select {
	case <-b:
	case <-time.After(time.Second):
		t.Fatal("replacement join did not cancel")
	}
}

func TestCloseStopsTimersAndRejectsNewWork(t *testing.T) {
	c := NewCoordinator()
	c.RebalanceDelay = time.Hour
	result := make(chan *JoinGroupResult, 1)
	go func() { result <- c.JoinGroup("g", "", 3600000, "consumer", []MemberProtocol{{Name: "range"}}) }()
	g := waitGroupState(t, c, func(g *ConsumerGroup) bool { return len(g.pendingJoins) == 1 })
	done := make(chan struct{})
	go func() { c.Close(); c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close waited for long-lived timers")
	}
	select {
	case result := <-result:
		if result.ErrorCode == 0 {
			t.Fatal("closed join succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not release join")
	}
	if g.MemberCount() != 0 {
		t.Fatal("closed coordinator retained members")
	}
	if result := c.JoinGroup("new", "", 1000, "consumer", []MemberProtocol{{Name: "range"}}); result.ErrorCode != protocol.ErrCoordinatorNotAvailable {
		t.Fatal("closed coordinator accepted a new group")
	}
	if err := c.LoadOffsets(t.TempDir()); err == nil {
		t.Fatal("closed coordinator initialized storage")
	}
}

func TestCloseWaitsForRunningTimerCallback(t *testing.T) {
	c := NewCoordinator()
	g := c.getOrCreateGroup("g")
	entered, release := make(chan struct{}), make(chan struct{})
	g.mu.Lock()
	g.rebalanceTimer = g.after(0, func() { close(entered); <-release })
	g.mu.Unlock()
	<-entered
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("close returned while its timer callback was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close did not join the completed timer callback")
	}
}

func TestStaleSessionCallbackDoesNotExpireRefreshedMember(t *testing.T) {
	c := NewCoordinator()
	defer c.Close()
	result := c.JoinGroup("g", "", 3600000, "consumer", []MemberProtocol{{Name: "range"}})
	g := c.FindGroup("g")
	c.handleSessionExpired(g, result.MemberID)
	if g.MemberCount() != 1 {
		t.Fatal("a queued old timer expired a refreshed member")
	}
}
