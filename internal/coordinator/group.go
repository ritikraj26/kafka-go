package coordinator

import (
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
	sessionTimer   *time.Timer
}

// MemberProtocol is a name+metadata pair from JoinGroup.
type MemberProtocol struct {
	Name     string
	Metadata []byte
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
	rebalanceTimer *time.Timer

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

// chooseProtocol picks the protocol supported by all members.
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
	g.GenerationID++
	g.ProtocolName = g.chooseProtocol()

	// Pick leader deterministically: keep existing leader if still present,
	// otherwise pick the lowest sorted member ID.
	if _, ok := g.Members[g.LeaderID]; !ok {
		sortedIDs := g.sortedMemberIDs()
		g.LeaderID = sortedIDs[0]
	}

	g.State = GroupAwaitingSync

	// Build members list for the leader (sorted for deterministic response)
	sortedIDs := g.sortedMemberIDs()
	leaderMembers := make([]JoinGroupMember, 0, len(g.Members))
	for _, mid := range sortedIDs {
		m := g.Members[mid]
		var meta []byte
		for _, p := range m.Protocols {
			if p.Name == g.ProtocolName {
				meta = p.Metadata
				break
			}
		}
		leaderMembers = append(leaderMembers, JoinGroupMember{MemberID: mid, Metadata: meta})
	}

	// Deliver results to all pending join callers
	for mid, ch := range g.pendingJoins {
		result := &JoinGroupResult{
			ErrorCode:    0, // ErrNone
			GenerationID: g.GenerationID,
			ProtocolName: g.ProtocolName,
			LeaderID:     g.LeaderID,
			MemberID:     mid,
		}
		if mid == g.LeaderID {
			result.Members = leaderMembers
		}
		select {
		case ch <- result:
		default:
		}
	}
	g.pendingJoins = make(map[string]chan *JoinGroupResult)
}

// startSessionTimer starts the session expiry timer for a member.
func (g *ConsumerGroup) startSessionTimer(m *Member) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	m.LastHeartbeat = time.Now()
	memberID := m.MemberID // capture for closure
	m.sessionTimer = time.AfterFunc(m.SessionTimeout, func() {
		// The timer may fire after removeMember stopped it but before the
		// goroutine was scheduled. Re-check membership under lock.
		g.mu.Lock()
		if _, ok := g.Members[memberID]; !ok {
			g.mu.Unlock()
			return
		}
		g.mu.Unlock()
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
	m.LastHeartbeat = time.Now()
	if m.sessionTimer != nil {
		m.sessionTimer.Reset(m.SessionTimeout)
	}
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
		g.State = GroupEmpty
		g.LeaderID = ""
	}
}

// triggerRebalance moves the group to Joining state and fails any
// pending sync callers with REBALANCE_IN_PROGRESS (27).
func (g *ConsumerGroup) triggerRebalance() {
	logger.L.Info("triggering rebalance", "group", g.GroupID, "generation", g.GenerationID)
	g.State = GroupJoining

	// Fail pending syncs
	for mid, ch := range g.pendingSyncs {
		select {
		case ch <- nil: // nil signals error
		default:
		}
		delete(g.pendingSyncs, mid)
	}
}

// MemberCount returns the number of members.
func (g *ConsumerGroup) MemberCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.Members)
}
