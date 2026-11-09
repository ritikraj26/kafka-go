package coordinator

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func isolationMember(t *testing.T, c *Coordinator, group string) *JoinGroupResult {
	t.Helper()
	joined := c.JoinGroup(group, "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	if joined.ErrorCode != protocol.ErrNone {
		t.Fatalf("join %s: %d", group, joined.ErrorCode)
	}
	if _, code := c.SyncGroup(group, joined.GenerationID, joined.MemberID, nil); code != protocol.ErrNone {
		t.Fatalf("sync %s: %d", group, code)
	}
	return joined
}

// The release cleanup is registered after Close, so failures always unblock the
// writer before shutdown waits for its durable operation to finish.
func blockIsolationSync(t *testing.T, c *Coordinator) (<-chan struct{}, func(), *atomic.Int32) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	var opens atomic.Int32
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.openOffsetLog = func(path string) (offsetLogFile, error) {
		opens.Add(1)
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		return &faultyOffsetFile{File: f, syncHook: func() error {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return f.Sync()
		}}, nil
	}
	return entered, unblock, &opens
}

func isolationCode(t *testing.T, result <-chan int16, operation string, want int16) {
	t.Helper()
	select {
	case code := <-result:
		if code != want {
			t.Fatalf("%s: code=%d, want %d", operation, code, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s blocked behind an unrelated disk sync", operation)
	}
}

func startIsolationCommit(t *testing.T, c *Coordinator, member *JoinGroupResult, entered <-chan struct{}) <-chan int16 {
	t.Helper()
	result := make(chan int16, 1)
	go func() {
		result <- c.CommitOffset("a", member.GenerationID, member.MemberID, "events", 0, 7)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("offset commit did not reach disk sync")
	}
	return result
}

func newIsolationCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	dir := t.TempDir()
	c := NewCoordinator()
	t.Cleanup(c.Close)
	if err := c.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestOffsetSyncDoesNotBlockUnrelatedHeartbeats(t *testing.T) {
	c := newIsolationCoordinator(t)
	a := isolationMember(t, c, "a")
	b := isolationMember(t, c, "b")
	gb := c.FindGroup("b")
	entered, unblock, _ := blockIsolationSync(t, c)
	committed := startIsolationCommit(t, c, a, entered)

	// Model an old session callback whose original deadline has already passed.
	// Processing the newly arrived heartbeat must refresh liveness before that
	// callback checks it; no short real-time session is needed for this schedule.
	gb.mu.Lock()
	member := gb.Members[b.MemberID]
	member.LastHeartbeat = time.Now().Add(-member.SessionTimeout)
	previousHeartbeat := member.LastHeartbeat
	gb.mu.Unlock()
	heartbeat := make(chan int16, 1)
	go func() { heartbeat <- c.Heartbeat("b", b.GenerationID, b.MemberID) }()
	isolationCode(t, heartbeat, "unrelated heartbeat", protocol.ErrNone)
	c.handleSessionExpired(gb, b.MemberID)
	gb.mu.Lock()
	stillPresent := gb.Members[b.MemberID] == member
	refreshed := member.LastHeartbeat.After(previousHeartbeat)
	stable := gb.State == GroupStable && gb.GenerationID == b.GenerationID
	gb.mu.Unlock()
	if !stillPresent || !refreshed || !stable {
		t.Fatal("unrelated disk sync caused a refreshed member to expire or rebalance")
	}

	unblock()
	isolationCode(t, committed, "offset commit", protocol.ErrNone)
	if offset, code := c.FetchOffset("a", "events", 0); code != protocol.ErrNone || offset != 7 {
		t.Fatalf("completed commit: offset=%d code=%d", offset, code)
	}
}

func TestGroupAdmissionDoesNotBlockBehindOffsetSync(t *testing.T) {
	c := newIsolationCoordinator(t)
	a := isolationMember(t, c, "a")
	b := isolationMember(t, c, "b")
	entered, unblock, _ := blockIsolationSync(t, c)
	committed := startIsolationCommit(t, c, a, entered)

	joined := make(chan *JoinGroupResult, 1)
	go func() {
		joined <- c.JoinGroup("new-group", "", 60000, "consumer", []MemberProtocol{{Name: "range"}})
	}()
	select {
	case member := <-joined:
		if member.ErrorCode != protocol.ErrNone {
			t.Fatalf("new group admission: %d", member.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("group admission held the coordinator lock while waiting for a busy group")
	}
	heartbeat := make(chan int16, 1)
	go func() { heartbeat <- c.Heartbeat("b", b.GenerationID, b.MemberID) }()
	isolationCode(t, heartbeat, "heartbeat after group admission", protocol.ErrNone)

	unblock()
	isolationCode(t, committed, "offset commit", protocol.ErrNone)
}

func TestQueuedOffsetCommitDoesNotLockUnrelatedMembership(t *testing.T) {
	c := newIsolationCoordinator(t)
	a := isolationMember(t, c, "a")
	b := isolationMember(t, c, "b")
	entered, unblock, opens := blockIsolationSync(t, c)
	committed := startIsolationCommit(t, c, a, entered)

	queued, started := make(chan int16, 1), make(chan struct{})
	go func() {
		close(started)
		queued <- c.CommitOffset("b", b.GenerationID, b.MemberID, "events", 0, 99)
	}()
	<-started
	heartbeat := make(chan int16, 1)
	go func() { heartbeat <- c.Heartbeat("b", b.GenerationID, b.MemberID) }()
	isolationCode(t, heartbeat, "heartbeat while commit queued", protocol.ErrNone)
	left := make(chan int16, 1)
	go func() { left <- c.LeaveGroup("b", b.MemberID) }()
	isolationCode(t, left, "leave while commit queued", protocol.ErrNone)

	unblock()
	isolationCode(t, committed, "first offset commit", protocol.ErrNone)
	isolationCode(t, queued, "queued commit after membership removal", protocol.ErrUnknownMemberID)
	if got := opens.Load(); got != 1 {
		t.Fatalf("removed member's queued commit reached persistence: opened log %d times", got)
	}
	if offset, code := c.FetchOffset("b", "events", 0); code != protocol.ErrNone || offset != -1 {
		t.Fatalf("removed member's commit was published: offset=%d code=%d", offset, code)
	}
}

func TestCanceledQueuedOffsetCommitDoesNotPersist(t *testing.T) {
	c := newIsolationCoordinator(t)
	a := isolationMember(t, c, "a")
	b := isolationMember(t, c, "b")
	entered, unblock, opens := blockIsolationSync(t, c)
	committed := startIsolationCommit(t, c, a, entered)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queued, started := make(chan int16, 1), make(chan struct{})
	go func() {
		close(started)
		queued <- c.CommitOffsetWithMetadataContext(ctx, "b", b.GenerationID, b.MemberID, "events", 0, 99, nil)
	}()
	<-started
	cancel()
	unblock()
	isolationCode(t, committed, "first offset commit", protocol.ErrNone)
	isolationCode(t, queued, "canceled queued commit", protocol.ErrRequestTimedOut)
	if got := opens.Load(); got != 1 {
		t.Fatalf("canceled queued commit reached persistence: opened log %d times", got)
	}
	if offset, code := c.FetchOffset("b", "events", 0); code != protocol.ErrNone || offset != -1 {
		t.Fatalf("canceled commit was published: offset=%d code=%d", offset, code)
	}
}

func TestCloseRejectsQueuedOffsetCommitAndDrainsDurableWrite(t *testing.T) {
	dir := t.TempDir()
	c := NewCoordinator()
	t.Cleanup(c.Close)
	if err := c.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	a := isolationMember(t, c, "a")
	b := isolationMember(t, c, "b")
	entered, unblock, opens := blockIsolationSync(t, c)
	committed := startIsolationCommit(t, c, a, entered)

	queued, started := make(chan int16, 1), make(chan struct{})
	go func() {
		close(started)
		queued <- c.CommitOffset("b", b.GenerationID, b.MemberID, "events", 0, 99)
	}()
	<-started
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("close did not reject new work during offset sync")
	}
	select {
	case <-closed:
		t.Fatal("close returned before the durable write finished")
	default:
	}
	unblock()
	isolationCode(t, committed, "in-progress offset commit during close", protocol.ErrNone)
	isolationCode(t, queued, "queued commit during close", protocol.ErrCoordinatorNotAvailable)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not drain the durable offset write")
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("queued commit reached persistence after close: opened log %d times", got)
	}

	restarted := NewCoordinator()
	t.Cleanup(restarted.Close)
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	if offset, code := restarted.FetchOffset("a", "events", 0); code != protocol.ErrNone || offset != 7 {
		t.Fatalf("in-progress commit lost at shutdown: offset=%d code=%d", offset, code)
	}
	if offset, code := restarted.FetchOffset("b", "events", 0); code != protocol.ErrNone || offset != -1 {
		t.Fatalf("queued commit survived shutdown: offset=%d code=%d", offset, code)
	}
}
