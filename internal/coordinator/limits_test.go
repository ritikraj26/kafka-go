package coordinator

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func quotaCoordinator(t *testing.T, limits StateLimits) *Coordinator {
	t.Helper()
	c := NewCoordinator()
	if err := c.SetStateLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := c.LoadOffsets(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestEmptyGroupsReclaimedWithoutDeletingOffsets(t *testing.T) {
	limits := DefaultStateLimits()
	limits.MaxGroups = 1
	c := quotaCoordinator(t, limits)
	first := c.JoinGroup("first", "", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != 0 {
		t.Fatal(first.ErrorCode)
	}
	if code := c.CommitOffset("first", first.GenerationID, first.MemberID, "events", 0, 42); code != 0 {
		t.Fatal(code)
	}
	if result := c.JoinGroup("second", "", 30000, "consumer", []MemberProtocol{{Name: "range"}}); result.ErrorCode != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("group count limit bypassed")
	}
	c.LeaveGroup("first", first.MemberID)
	second := c.JoinGroup("second", "", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if second.ErrorCode != 0 || c.FindGroup("first") != nil || c.StateStats().Groups != 1 {
		t.Fatal("empty membership state was not reclaimed")
	}
	if offset, code := c.FetchOffset("first", "events", 0); code != 0 || offset != 42 {
		t.Fatal("membership reclamation deleted committed offsets")
	}
}

func TestCloseJoinsCallbacksForReclaimedGroups(t *testing.T) {
	limits := DefaultStateLimits()
	limits.MaxGroups = 1
	c := quotaCoordinator(t, limits)
	first := c.JoinGroup("first", "member", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if first.ErrorCode != 0 {
		t.Fatal(first.ErrorCode)
	}
	g := c.FindGroup("first")
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	g.mu.Lock()
	g.after(0, func() {
		close(started)
		<-release
	})
	g.mu.Unlock()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	if code := c.LeaveGroup("first", "member"); code != 0 {
		t.Fatal(code)
	}
	second := c.JoinGroup("second", "member", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if second.ErrorCode != 0 || c.FindGroup("first") != nil {
		t.Fatal("empty group with a running callback was not reclaimed")
	}
	closed := make(chan struct{})
	go func() {
		c.Close()
		close(closed)
	}()
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("close did not start")
	}
	select {
	case <-closed:
		t.Fatal("close returned while a reclaimed group's callback was running")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not join the released callback")
	}
	if stats := c.StateStats(); stats.Groups != 0 || stats.GroupBytes != 0 {
		t.Fatalf("close retained group state: %+v", stats)
	}
}

func TestGroupQuotaUpdatesAreAtomicAndOwned(t *testing.T) {
	limits := DefaultStateLimits()
	limits.PerGroupBytes = 4096
	limits.GroupBytes = 4096
	c := quotaCoordinator(t, limits)
	input := bytes.Repeat([]byte("x"), 128)
	member := c.JoinGroup("g", "member", 30000, "consumer", []MemberProtocol{{Name: "range", Metadata: input}})
	if member.ErrorCode != 0 {
		t.Fatal(member.ErrorCode)
	}
	before := c.StateStats().GroupBytes
	input[0] = 'z'
	if result := c.JoinGroup("g", "member", 30000, "consumer", []MemberProtocol{{Name: "range", Metadata: make([]byte, 5000)}}); result.ErrorCode != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("oversized update was accepted")
	}
	g := c.FindGroup("g")
	g.mu.Lock()
	if g.Members["member"].Protocols[0].Metadata[0] != 'x' {
		t.Error("input mutation or rejected update changed retained metadata")
	}
	g.mu.Unlock()
	if c.StateStats().GroupBytes != before {
		t.Fatal("rejected update changed quota accounting")
	}
	if _, code := c.SyncGroup("g", member.GenerationID, "member", []SyncGroupAssignment{{MemberID: "member", Assignment: make([]byte, 5000)}}); code != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("oversized assignment accepted")
	}
	g.mu.Lock()
	if g.State != GroupAwaitingSync || len(g.Members["member"].Assignment) != 0 {
		t.Error("failed assignment partially changed state")
	}
	g.mu.Unlock()
	if _, code := c.SyncGroup("g", member.GenerationID, "member", []SyncGroupAssignment{{MemberID: "member", Assignment: []byte("ok")}}); code != 0 {
		t.Fatal(code)
	}
	c.LeaveGroup("g", "member")
	if c.StateStats().GroupBytes != groupCharge("g") {
		t.Fatal("member removal did not release its charged state")
	}
	c.Close()
	if stats := c.StateStats(); stats.GroupBytes != 0 || stats.Groups != 0 {
		t.Fatalf("close retained group state: %+v", stats)
	}
}

func TestExplicitMemberIDsCannotBypassGroupSize(t *testing.T) {
	c := quotaCoordinator(t, DefaultStateLimits())
	c.MaxGroupSize = 1
	if result := c.JoinGroup("g", "one", 30000, "consumer", []MemberProtocol{{Name: "range"}}); result.ErrorCode != 0 {
		t.Fatal(result.ErrorCode)
	}
	if result := c.JoinGroup("g", "two", 30000, "consumer", []MemberProtocol{{Name: "range"}}); result.ErrorCode != protocol.ErrGroupMaxSizeReached {
		t.Fatal("explicit member ID bypassed the member count limit")
	}
}

func TestOffsetQuotaRejectsBeforePersistenceAndPermitsUpdates(t *testing.T) {
	limits := DefaultStateLimits()
	limits.MaxOffsets = 1
	c := quotaCoordinator(t, limits)
	member := c.JoinGroup("g", "", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if code := c.CommitOffset("g", member.GenerationID, member.MemberID, "events", 0, 5); code != 0 {
		t.Fatal(code)
	}
	path := filepath.Join(c.offsetLogDir, offsetLogName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if code := c.CommitOffset("g", member.GenerationID, member.MemberID, "events", 1, 9); code != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("offset entry ceiling bypassed")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected offset was persisted")
	}
	if code := c.CommitOffset("g", member.GenerationID, member.MemberID, "events", 0, 7); code != 0 {
		t.Fatalf("existing offset could not be updated: %d", code)
	}
	if stats := c.StateStats(); stats.Offsets != 1 || stats.OffsetBytes != offsetCharge("g", "events", nil) {
		t.Fatalf("offset accounting: %+v", stats)
	}
}

func TestOffsetRecoveryFailsOnQuotaWithoutEviction(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	for partition := int32(0); partition < 2; partition++ {
		if code := c.CommitOffset("group", member.GenerationID, member.MemberID, "events", partition, 3); code != 0 {
			t.Fatal(code)
		}
	}
	path := filepath.Join(c.offsetLogDir, offsetLogName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewCoordinator()
	defer restarted.Close()
	limits := DefaultStateLimits()
	limits.MaxOffsets = 1
	if err := restarted.SetStateLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := restarted.LoadOffsets(dir); err == nil {
		t.Fatal("recovery silently dropped offsets to meet quota")
	}
	if len(restarted.offsets) != 0 {
		t.Fatal("failed recovery published partial retained state")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("quota failure modified the offset log")
	}
}
