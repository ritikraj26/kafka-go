package coordinator

import (
	"sync"
	"testing"
	"time"
)

func TestJoinGroup_SingleMember(t *testing.T) {
	coord := NewCoordinator()

	var result *JoinGroupResult
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		result = coord.JoinGroup("test-group", "", 10000, "consumer",
			[]MemberProtocol{{Name: "range", Metadata: []byte("meta")}})
	}()
	wg.Wait()

	if result == nil {
		t.Fatal("JoinGroup returned nil")
	}
	if result.ErrorCode != 0 {
		t.Fatalf("error_code = %d, want 0", result.ErrorCode)
	}
	if result.GenerationID != 1 {
		t.Errorf("generation_id = %d, want 1", result.GenerationID)
	}
	if result.MemberID == "" {
		t.Error("member_id should be assigned")
	}
	// Single member should be leader
	if result.LeaderID != result.MemberID {
		t.Errorf("leader_id = %q, want %q", result.LeaderID, result.MemberID)
	}
	if len(result.Members) != 1 {
		t.Errorf("members count = %d, want 1", len(result.Members))
	}
}

func TestJoinGroup_TwoMembers(t *testing.T) {
	coord := NewCoordinator()
	coord.RebalanceDelay = 100 * time.Millisecond // allow second member to arrive

	var r1, r2 *JoinGroupResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 = coord.JoinGroup("g1", "", 10000, "consumer",
			[]MemberProtocol{{Name: "range", Metadata: []byte("m1")}})
	}()
	go func() {
		defer wg.Done()
		r2 = coord.JoinGroup("g1", "", 10000, "consumer",
			[]MemberProtocol{{Name: "range", Metadata: []byte("m2")}})
	}()
	wg.Wait()

	if r1 == nil || r2 == nil {
		t.Fatal("one or both JoinGroup returned nil")
	}
	if r1.ErrorCode != 0 || r2.ErrorCode != 0 {
		t.Fatalf("errors: r1=%d, r2=%d", r1.ErrorCode, r2.ErrorCode)
	}
	if r1.GenerationID != r2.GenerationID {
		t.Errorf("generation mismatch: %d vs %d", r1.GenerationID, r2.GenerationID)
	}
	// Exactly one should be leader
	if r1.LeaderID != r2.LeaderID {
		t.Errorf("different leaders: %q vs %q", r1.LeaderID, r2.LeaderID)
	}
}

func TestSyncGroup(t *testing.T) {
	coord := NewCoordinator()

	// Join
	result := coord.JoinGroup("sg", "", 10000, "consumer",
		[]MemberProtocol{{Name: "range", Metadata: nil}})
	if result.ErrorCode != 0 {
		t.Fatalf("JoinGroup error: %d", result.ErrorCode)
	}

	// Sync (leader provides assignment)
	assignment := []byte{0x01, 0x02, 0x03}
	data, errCode := coord.SyncGroup("sg", result.GenerationID, result.MemberID,
		[]SyncGroupAssignment{{MemberID: result.MemberID, Assignment: assignment}})
	if errCode != 0 {
		t.Fatalf("SyncGroup error: %d", errCode)
	}
	if len(data) != len(assignment) {
		t.Errorf("assignment length = %d, want %d", len(data), len(assignment))
	}
}

func TestHeartbeat_Stable(t *testing.T) {
	coord := NewCoordinator()

	// Join + Sync to get to Stable
	result := coord.JoinGroup("hb", "", 10000, "consumer",
		[]MemberProtocol{{Name: "range", Metadata: nil}})
	coord.SyncGroup("hb", result.GenerationID, result.MemberID,
		[]SyncGroupAssignment{{MemberID: result.MemberID, Assignment: []byte{1}}})

	// Heartbeat should succeed
	errCode := coord.Heartbeat("hb", result.GenerationID, result.MemberID)
	if errCode != 0 {
		t.Errorf("Heartbeat error = %d, want 0", errCode)
	}
}

func TestHeartbeat_WrongGeneration(t *testing.T) {
	coord := NewCoordinator()
	result := coord.JoinGroup("hb2", "", 10000, "consumer",
		[]MemberProtocol{{Name: "range", Metadata: nil}})
	coord.SyncGroup("hb2", result.GenerationID, result.MemberID,
		[]SyncGroupAssignment{{MemberID: result.MemberID, Assignment: []byte{1}}})

	errCode := coord.Heartbeat("hb2", 999, result.MemberID)
	if errCode != 22 { // ErrIllegalGeneration
		t.Errorf("expected ErrIllegalGeneration(22), got %d", errCode)
	}
}

func TestLeaveGroup_TriggersRebalance(t *testing.T) {
	coord := NewCoordinator()
	coord.RebalanceDelay = 100 * time.Millisecond

	// Two members join
	var r1, r2 *JoinGroupResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 = coord.JoinGroup("lg", "", 10000, "consumer",
			[]MemberProtocol{{Name: "range", Metadata: nil}})
	}()
	go func() {
		defer wg.Done()
		r2 = coord.JoinGroup("lg", "", 10000, "consumer",
			[]MemberProtocol{{Name: "range", Metadata: nil}})
	}()
	wg.Wait()

	if r1 == nil || r2 == nil {
		t.Fatal("one of join results is nil")
	}
	if r1.ErrorCode != 0 || r2.ErrorCode != 0 {
		t.Fatalf("join errors: r1=%d, r2=%d", r1.ErrorCode, r2.ErrorCode)
	}

	// Determine which is the leader
	leader, follower := r1, r2
	if r2.MemberID == r2.LeaderID {
		leader, follower = r2, r1
	}

	// SyncGroup: leader provides assignments, follower waits
	var syncWg sync.WaitGroup
	syncWg.Add(2)
	go func() {
		defer syncWg.Done()
		coord.SyncGroup("lg", leader.GenerationID, leader.MemberID,
			[]SyncGroupAssignment{
				{MemberID: leader.MemberID, Assignment: []byte{1}},
				{MemberID: follower.MemberID, Assignment: []byte{2}},
			})
	}()
	go func() {
		defer syncWg.Done()
		coord.SyncGroup("lg", follower.GenerationID, follower.MemberID, nil)
	}()
	syncWg.Wait()

	// Leave one member
	errCode := coord.LeaveGroup("lg", follower.MemberID)
	if errCode != 0 {
		t.Fatalf("LeaveGroup error: %d", errCode)
	}

	// Group should be in Joining state
	g := coord.FindGroup("lg")
	g.mu.Lock()
	state := g.State
	g.mu.Unlock()
	if state != GroupJoining {
		t.Errorf("group state = %v, want Joining", state)
	}
}

func TestOffsetCommitAndFetch(t *testing.T) {
	coord := NewCoordinator()

	// Join a single-member group
	result := coord.JoinGroup("oc", "", 10000, "consumer",
		[]MemberProtocol{{Name: "range", Metadata: nil}})

	// Commit
	errCode := coord.CommitOffset("oc", result.GenerationID, result.MemberID, "topic1", 0, 42)
	if errCode != 0 {
		t.Fatalf("CommitOffset error: %d", errCode)
	}

	// Fetch
	offset, errCode := coord.FetchOffset("oc", "topic1", 0)
	if errCode != 0 {
		t.Fatalf("FetchOffset error: %d", errCode)
	}
	if offset != 42 {
		t.Errorf("offset = %d, want 42", offset)
	}

	// Fetch unknown partition
	offset, _ = coord.FetchOffset("oc", "topic1", 99)
	if offset != -1 {
		t.Errorf("unknown partition offset = %d, want -1", offset)
	}
}

func TestOffsetPersistence(t *testing.T) {
	dir := t.TempDir()

	// First coordinator — commit some offsets
	coord1 := NewCoordinator()
	coord1.LoadOffsets(dir)

	jr := coord1.JoinGroup("pg", "", 10000, "consumer",
		[]MemberProtocol{{Name: "range", Metadata: nil}})
	if jr.ErrorCode != 0 {
		t.Fatalf("JoinGroup error: %d", jr.ErrorCode)
	}
	coord1.SyncGroup("pg", jr.GenerationID, jr.MemberID,
		[]SyncGroupAssignment{{MemberID: jr.MemberID, Assignment: []byte{1}}})

	errCode := coord1.CommitOffset("pg", jr.GenerationID, jr.MemberID, "topic1", 0, 100)
	if errCode != 0 {
		t.Fatalf("CommitOffset error: %d", errCode)
	}
	errCode = coord1.CommitOffset("pg", jr.GenerationID, jr.MemberID, "topic1", 1, 200)
	if errCode != 0 {
		t.Fatalf("CommitOffset error: %d", errCode)
	}

	// Second coordinator — should recover offsets from disk
	coord2 := NewCoordinator()
	coord2.LoadOffsets(dir)

	off, ec := coord2.FetchOffset("pg", "topic1", 0)
	if ec != 0 {
		t.Fatalf("FetchOffset error: %d", ec)
	}
	if off != 100 {
		t.Errorf("recovered offset = %d, want 100", off)
	}

	off, ec = coord2.FetchOffset("pg", "topic1", 1)
	if ec != 0 {
		t.Fatalf("FetchOffset error: %d", ec)
	}
	if off != 200 {
		t.Errorf("recovered offset = %d, want 200", off)
	}
}
