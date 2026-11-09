package coordinator

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
)

// GroupState represents the state of a consumer group.
type GroupState int

const (
	GroupEmpty        GroupState = iota // No members
	GroupJoining                        // Waiting for all members to join
	GroupAwaitingSync                   // Waiting for leader to send assignments
	GroupStable                         // All members assigned, heartbeating
	GroupDead                           // Group is being removed
)

func (s GroupState) String() string {
	switch s {
	case GroupEmpty:
		return "Empty"
	case GroupJoining:
		return "Joining"
	case GroupAwaitingSync:
		return "AwaitingSync"
	case GroupStable:
		return "Stable"
	case GroupDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// JoinGroupResult is delivered to a waiting JoinGroup caller.
type JoinGroupResult struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	LeaderID     string
	MemberID     string
	Members      []JoinGroupMember // non-empty only for leader
}

// JoinGroupMember is sent to the group leader in the JoinGroup response.
type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

// Member tracks a single consumer in a group.
type Member struct {
	MemberID       string
	SessionTimeout time.Duration
	Protocols      []MemberProtocol
	Assignment     []byte
	LastHeartbeat  time.Time
	sessionTimer   *groupTimer
	joinAttempt    uint64
	retained       int64
}

// MemberProtocol is a name+metadata pair from JoinGroup.
type MemberProtocol struct {
	Name     string
	Metadata []byte
}

// Protocol order expresses preference, and metadata is an opaque subscription.
// Both must match before treating a Join as a retry of the current round.
func (m *Member) matchesProtocols(protocols []MemberProtocol) bool {
	if len(m.Protocols) != len(protocols) {
		return false
	}
	for i, p := range protocols {
		stored := m.Protocols[i]
		if stored.Name != p.Name || (stored.Metadata == nil) != (p.Metadata == nil) || !bytes.Equal(stored.Metadata, p.Metadata) {
			return false
		}
	}
	return true
}

// ConsumerGroup manages the state machine for one group.
type ConsumerGroup struct {
	mu           sync.Mutex
	GroupID      string
	State        GroupState
	GenerationID int32
	ProtocolType string
	ProtocolName string
	LeaderID     string
	Members      map[string]*Member

	// Channels for blocking JoinGroup/SyncGroup callers
	pendingJoins map[string]chan *JoinGroupResult
	pendingSyncs map[string]chan []byte

	// Rebalance delay timer: when set, join completion is deferred
	rebalanceTimer        *groupTimer
	rebalanceSequence     uint64
	rebalanceDelay        time.Duration
	joinTimeout           time.Duration
	syncTimeout           time.Duration
	phaseTimer            *groupTimer
	phaseSequence         uint64
	phaseDeadline         time.Time
	timers                sync.WaitGroup
	allTimers             *sync.WaitGroup
	usage                 *retainedUsage
	retained, maxRetained int64
	initializing          bool

	// Called when a member's session expires
	onSessionExpired func(g *ConsumerGroup, memberID string)
}

// newConsumerGroup creates a new group in the Empty state.
func newConsumerGroup(groupID string, onExpired func(*ConsumerGroup, string)) *ConsumerGroup {
	return &ConsumerGroup{
		GroupID:          groupID,
		State:            GroupEmpty,
		Members:          make(map[string]*Member),
		pendingJoins:     make(map[string]chan *JoinGroupResult),
		pendingSyncs:     make(map[string]chan []byte),
		onSessionExpired: onExpired,
	}
}

// sortedMemberIDs returns member IDs in sorted order for deterministic iteration.
func (g *ConsumerGroup) sortedMemberIDs() []string {
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// supportsProtocols checks the proposed membership before a join changes state.
// An existing member's new capabilities replace its old list for this check.
// Caller must hold g.mu; admission preserves a nonempty common protocol.
func (g *ConsumerGroup) supportsProtocols(memberID, protocolType string, protocols []MemberProtocol) bool {
	if len(g.Members) > 0 && g.ProtocolType != protocolType {
		return false
	}
	for _, candidate := range protocols {
		supported := true
		for id, member := range g.Members {
			if id == memberID {
				continue
			}
			found := false
			for _, offered := range member.Protocols {
				if offered.Name == candidate.Name {
					found = true
					break
				}
			}
			if !found {
				supported = false
				break
			}
		}
		if supported {
			return true
		}
	}
	return false
}

// chooseProtocol picks the protocol supported by all admitted members.
// Uses sorted member iteration for deterministic selection.
func (g *ConsumerGroup) chooseProtocol() string {
	if len(g.Members) == 0 {
		return ""
	}
	// Use the first member (sorted by ID) as the candidate source
	sortedIDs := g.sortedMemberIDs()
	first := g.Members[sortedIDs[0]]
	for _, p := range first.Protocols {
		supported := true
		for _, id := range sortedIDs {
			m := g.Members[id]
			found := false
			for _, mp := range m.Protocols {
				if mp.Name == p.Name {
					found = true
					break
				}
			}
			if !found {
				supported = false
				break
			}
		}
		if supported {
			return p.Name
		}
	}
	return ""
}

// completeJoinPhase transitions from Joining → AwaitingSync,
// increments the generation, selects protocol, picks leader, and
// delivers JoinGroupResult to all waiting callers.
func (g *ConsumerGroup) completeJoinPhase() {
	if g.State != GroupJoining || len(g.Members) == 0 || len(g.pendingJoins) != len(g.Members) {
		return
	}
	g.stopJoinCollection()
	g.GenerationID++
	g.ProtocolName = g.chooseProtocol()

	// Pick leader deterministically: keep existing leader if still present,
	// otherwise pick the lowest sorted member ID.
	if _, ok := g.Members[g.LeaderID]; !ok {
		sortedIDs := g.sortedMemberIDs()
		g.LeaderID = sortedIDs[0]
	}

	g.State = GroupAwaitingSync
	g.startPhaseDeadline(GroupAwaitingSync)
	for _, member := range g.Members {
		g.startSessionTimer(member)
	}

	// Deliver results to all pending join callers
	for mid, ch := range g.pendingJoins {
		result := g.joinGroupResult(mid)
		select {
		case ch <- result:
		default:
		}
	}
	g.pendingJoins = make(map[string]chan *JoinGroupResult)
}

// joinGroupResult snapshots the current generation for an admitted member.
// Caller must hold g.mu. Only the leader receives the sorted member metadata;
// owned copies keep callers from changing the group's retained subscription.
func (g *ConsumerGroup) joinGroupResult(memberID string) *JoinGroupResult {
	result := &JoinGroupResult{
		ErrorCode:    0, // ErrNone
		GenerationID: g.GenerationID,
		ProtocolName: g.ProtocolName,
		LeaderID:     g.LeaderID,
		MemberID:     memberID,
	}
	if memberID != g.LeaderID {
		return result
	}
	result.Members = make([]JoinGroupMember, 0, len(g.Members))
	for _, id := range g.sortedMemberIDs() {
		var metadata []byte
		for _, p := range g.Members[id].Protocols {
			if p.Name == g.ProtocolName {
				metadata = p.Metadata
				break
			}
		}
		result.Members = append(result.Members, JoinGroupMember{MemberID: id, Metadata: bytes.Clone(metadata)})
	}
	return result
}

// startSessionTimer starts the session expiry timer for a member.
func (g *ConsumerGroup) startSessionTimer(m *Member) {
	m.LastHeartbeat = time.Now()
	g.scheduleSessionTimer(m, m.SessionTimeout)
}

// resumeSessionTimer restores the original liveness deadline after a blocked
// sync is canceled. It does not count cancellation as a fresh heartbeat.
func (g *ConsumerGroup) resumeSessionTimer(m *Member) {
	g.scheduleSessionTimer(m, max(0, m.SessionTimeout-time.Since(m.LastHeartbeat)))
}

func (g *ConsumerGroup) scheduleSessionTimer(m *Member, delay time.Duration) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	memberID := m.MemberID // capture for closure
	m.sessionTimer = g.after(delay, func() {
		if g.onSessionExpired != nil {
			g.onSessionExpired(g, memberID)
		}
	})
}

// resetSessionTimer resets a member's session timer.
func (g *ConsumerGroup) resetSessionTimer(memberID string) {
	m, ok := g.Members[memberID]
	if !ok {
		return
	}
	g.startSessionTimer(m)
}

// removeMember removes a member and cleans up its resources.
// Caller must hold g.mu.
func (g *ConsumerGroup) removeMember(memberID string) {
	m, ok := g.Members[memberID]
	if !ok {
		return
	}
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	delete(g.Members, memberID)
	g.publishState(g.retained - m.retained)
	if g.LeaderID == memberID {
		g.LeaderID = ""
	}
	g.ProtocolName = ""

	// Close pending channels
	if ch, ok := g.pendingJoins[memberID]; ok {
		close(ch)
		delete(g.pendingJoins, memberID)
	}
	if ch, ok := g.pendingSyncs[memberID]; ok {
		close(ch)
		delete(g.pendingSyncs, memberID)
	}

	if len(g.Members) == 0 {
		g.publishState(g.retained - 2*int64(len(g.ProtocolType)))
		g.ProtocolType = ""
		g.stopRebalanceTimers()
		g.State = GroupEmpty
		g.LeaderID = ""
	}
}

// triggerRebalance moves the group to Joining state and fails any
// pending sync callers with REBALANCE_IN_PROGRESS (27).
func (g *ConsumerGroup) triggerRebalance() {
	if g.State == GroupDead {
		return
	}
	logger.L.Info("triggering rebalance", "group", g.GroupID, "generation", g.GenerationID)
	if g.State != GroupJoining {
		g.stopJoinCollection()
		g.State = GroupJoining
		g.startPhaseDeadline(GroupJoining)
	}

	// Fail pending syncs
	for mid, ch := range g.pendingSyncs {
		if member := g.Members[mid]; member != nil {
			g.startSessionTimer(member)
		}
		select {
		case ch <- nil: // nil signals error
		default:
		}
		delete(g.pendingSyncs, mid)
	}
	g.maybeCompleteJoin()
}

// MemberCount returns the number of members.
func (g *ConsumerGroup) MemberCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.Members)
}
