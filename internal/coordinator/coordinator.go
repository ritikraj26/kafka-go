package coordinator

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// Coordinator manages all consumer groups and committed offsets.
type Coordinator struct {
	mu             sync.RWMutex
	groups         map[string]*ConsumerGroup
	offsets        offsetMap
	RebalanceDelay time.Duration // delay before completing a join round (allows more members to join)
	MaxGroupSize   int           // max members per group (0 = unlimited, default 256)
	offsetLogDir   string        // directory for __consumer_offsets log persistence
	offsetReady    bool
	offsetErr      error
	openOffsetLog  func(string) (offsetLogFile, error)
	closed         bool
	done           chan struct{}
	closeDone      chan struct{}
	limits         StateLimits
	groupUsage     retainedUsage
	offsetEntries  int
	offsetBytes    int64
	allTimers      sync.WaitGroup
}

// NewCoordinator creates an empty Coordinator.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		groups:         make(map[string]*ConsumerGroup),
		offsets:        make(offsetMap),
		openOffsetLog:  openOffsetLogFile,
		RebalanceDelay: 0,
		MaxGroupSize:   256,
		done:           make(chan struct{}),
		closeDone:      make(chan struct{}),
		limits:         DefaultStateLimits(),
		groupUsage:     retainedUsage{limit: DefaultStateLimits().GroupBytes},
	}
}

// getOrCreateGroup returns the group, creating it if needed.
func (c *Coordinator) getOrCreateGroup(groupID string) *ConsumerGroup {
	return c.getOrCreateGroupContext(context.Background(), groupID)
}

func (c *Coordinator) getOrCreateGroupContext(ctx context.Context, groupID string) *ConsumerGroup {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || ctx.Err() != nil {
		return nil
	}
	g, ok := c.groups[groupID]
	if !ok {
		c.reclaimEmptyGroups()
		cost := groupCharge(groupID)
		if len(c.groups) >= c.limits.MaxGroups || cost > c.limits.PerGroupBytes || !c.groupUsage.change(cost) {
			logger.L.Warn("consumer group admission rejected", "group", groupID)
			return nil
		}
		g = newConsumerGroup(groupID, c.handleSessionExpired)
		g.GroupID = strings.Clone(groupID)
		g.usage = &c.groupUsage
		g.retained = cost
		g.maxRetained = c.limits.PerGroupBytes
		g.initializing = true
		g.allTimers = &c.allTimers
		c.groups[g.GroupID] = g
	}
	return g
}

// FindGroup returns the group or nil.
func (c *Coordinator) FindGroup(groupID string) *ConsumerGroup {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return c.groups[groupID]
}

// JoinGroup implements the JoinGroup protocol. It may block until all members join.
func (c *Coordinator) JoinGroup(groupID string, memberID string, sessionTimeoutMs int32,
	protocolType string, protocols []MemberProtocol) *JoinGroupResult {
	return c.JoinGroupContext(context.Background(), groupID, memberID, sessionTimeoutMs, protocolType, protocols)
}

func (c *Coordinator) JoinGroupContext(ctx context.Context, groupID string, memberID string, sessionTimeoutMs int32,
	protocolType string, protocols []MemberProtocol) *JoinGroupResult {
	if ctx.Err() != nil {
		return &JoinGroupResult{ErrorCode: protocol.ErrRequestTimedOut}
	}
	if groupID == "" || len(groupID) > 32767 || len(memberID) > 32767 || len(protocolType) > 32767 || sessionTimeoutMs <= 0 {
		logger.L.Warn("invalid consumer group join", "group", groupID)
		return &JoinGroupResult{ErrorCode: protocol.ErrInvalidRequest}
	}

	g := c.getOrCreateGroupContext(ctx, groupID)
	if g == nil {
		if ctx.Err() != nil {
			return &JoinGroupResult{ErrorCode: protocol.ErrRequestTimedOut}
		}
		select {
		case <-c.done:
			return &JoinGroupResult{ErrorCode: protocol.ErrCoordinatorNotAvailable}
		default:
		}
		return &JoinGroupResult{ErrorCode: protocol.ErrThrottlingQuotaExceeded}
	}
	g.mu.Lock()
	g.initializing = false

	if g.State == GroupDead {
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrCoordinatorNotAvailable}
	}
	if ctx.Err() != nil {
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrRequestTimedOut}
	}

	// Enforce max group size
	_, existing := g.Members[memberID]
	if !existing && c.MaxGroupSize > 0 && len(g.Members) >= c.MaxGroupSize {
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrGroupMaxSizeReached}
	}

	// Assign member ID if empty (first join)
	if memberID == "" {
		memberID = uuid.New().String()
	}

	// Add or update member
	m, exists := g.Members[memberID]
	oldCost := int64(0)
	var assignment []byte
	if exists {
		oldCost = m.retained
		assignment = m.Assignment
	}
	cost := memberCharge(memberID, protocols, assignment)
	next := g.retained - oldCost + cost + 2*int64(len(protocolType)-len(g.ProtocolType))
	if !g.growState(next) {
		g.mu.Unlock()
		logger.L.Warn("consumer group state quota exceeded", "group", groupID)
		return &JoinGroupResult{ErrorCode: protocol.ErrThrottlingQuotaExceeded, MemberID: memberID}
	}
	ownedProtocols := cloneProtocols(protocols)
	if !exists {
		m = &Member{
			MemberID:       strings.Clone(memberID),
			SessionTimeout: time.Duration(sessionTimeoutMs) * time.Millisecond,
			Protocols:      ownedProtocols,
		}
		g.Members[m.MemberID] = m
	} else {
		m.Protocols = ownedProtocols
		m.SessionTimeout = time.Duration(sessionTimeoutMs) * time.Millisecond
	}
	m.retained = cost
	g.publishState(next)

	m.joinAttempt++
	attempt := m.joinAttempt
	g.ProtocolType = strings.Clone(protocolType)
	g.startSessionTimer(m)

	// If group is Stable or AwaitingSync, trigger rebalance
	if g.State == GroupStable || g.State == GroupAwaitingSync {
		g.triggerRebalance()
	}

	if g.State == GroupEmpty || g.State == GroupJoining {
		g.State = GroupJoining
	}

	// Create a channel to wait for the join to complete
	ch := make(chan *JoinGroupResult, 1)
	if previous := g.pendingJoins[memberID]; previous != nil {
		close(previous)
	}
	g.pendingJoins[memberID] = ch

	// Check if all members have rejoined (pendingJoins == Members count)
	allPresent := len(g.pendingJoins) == len(g.Members)

	if allPresent {
		if c.RebalanceDelay > 0 && g.rebalanceTimer == nil {
			// Start a timer — complete join after delay (allows more members to arrive)
			delay := c.RebalanceDelay
			g.rebalanceSequence++
			sequence := g.rebalanceSequence
			g.rebalanceTimer = g.after(delay, func() {
				g.mu.Lock()
				defer g.mu.Unlock()
				if g.rebalanceSequence != sequence {
					return
				}
				g.rebalanceTimer = nil
				if g.State == GroupJoining && len(g.pendingJoins) > 0 {
					g.completeJoinPhase()
				}
			})
		} else if c.RebalanceDelay <= 0 {
			// No delay — complete immediately
			g.completeJoinPhase()
		}
		// If timer is already running and a new member arrived completing the set,
		// let the timer fire naturally (it will complete with all members)
	}

	g.mu.Unlock()

	// Wait for result (with timeout)
	timeout := time.NewTimer(time.Duration(sessionTimeoutMs) * time.Millisecond)
	defer timeout.Stop()
	select {
	case <-ctx.Done():
		c.cancelJoin(g, memberID, attempt)
		return &JoinGroupResult{ErrorCode: protocol.ErrRequestTimedOut, MemberID: memberID}
	case <-c.done:
		return &JoinGroupResult{ErrorCode: protocol.ErrCoordinatorNotAvailable, MemberID: memberID}
	case result, ok := <-ch:
		if !ok || result == nil {
			return &JoinGroupResult{ErrorCode: protocol.ErrRebalanceInProgress, MemberID: memberID}
		}
		return result
	case <-timeout.C:
		// Timeout — remove member and return error
		c.cancelJoin(g, memberID, attempt)
		return &JoinGroupResult{ErrorCode: protocol.ErrRebalanceInProgress, MemberID: memberID}
	}
}

// SyncGroup implements the SyncGroup protocol for the leader and followers.
func (c *Coordinator) SyncGroup(groupID string, generationID int32, memberID string,
	assignments []SyncGroupAssignment) ([]byte, int16) {
	return c.SyncGroupContext(context.Background(), groupID, generationID, memberID, assignments)
}

func (c *Coordinator) SyncGroupContext(ctx context.Context, groupID string, generationID int32, memberID string,
	assignments []SyncGroupAssignment) ([]byte, int16) {
	if ctx.Err() != nil {
		return nil, protocol.ErrRequestTimedOut
	}

	g := c.FindGroup(groupID)
	if g == nil {
		return nil, protocol.ErrCoordinatorNotAvailable
	}

	g.mu.Lock()
	if ctx.Err() != nil {
		g.mu.Unlock()
		return nil, protocol.ErrRequestTimedOut
	}

	m, ok := g.Members[memberID]
	if !ok {
		g.mu.Unlock()
		return nil, protocol.ErrUnknownMemberID
	}

	if generationID != g.GenerationID {
		g.mu.Unlock()
		return nil, protocol.ErrIllegalGeneration
	}

	if g.State != GroupAwaitingSync {
		if g.State == GroupStable {
			// Already synced — return cached assignment
			assignment := bytes.Clone(m.Assignment)
			g.mu.Unlock()
			return assignment, protocol.ErrNone
		}
		g.mu.Unlock()
		return nil, protocol.ErrRebalanceInProgress
	}

	g.resetSessionTimer(memberID)

	// The leader publishes a complete snapshot for this generation. Members
	// omitted from the request receive empty assignments, never their old ones.
	if memberID == g.LeaderID {
		replacement := make(map[string][]byte, len(assignments))
		for _, a := range assignments {
			_, duplicate := replacement[a.MemberID]
			if g.Members[a.MemberID] == nil || duplicate {
				g.mu.Unlock()
				logger.L.Warn("invalid group assignment member", "group", groupID)
				return nil, protocol.ErrInvalidRequest
			}
			replacement[a.MemberID] = a.Assignment
		}
		next := g.retained
		for id, mem := range g.Members {
			next += 2 * int64(len(replacement[id])-len(mem.Assignment))
		}
		if !g.growState(next) {
			g.mu.Unlock()
			logger.L.Warn("group assignment quota exceeded", "group", groupID)
			return nil, protocol.ErrThrottlingQuotaExceeded
		}
		for id, mem := range g.Members {
			assignment := bytes.Clone(replacement[id])
			if assignment == nil {
				// nil on a pending sync channel signals rebalance failure.
				assignment = []byte{}
			}
			mem.retained += 2 * int64(len(assignment)-len(mem.Assignment))
			mem.Assignment = assignment
		}
		g.publishState(next)

		g.State = GroupStable

		// Deliver assignments to all pending sync followers
		for mid, ch := range g.pendingSyncs {
			if mem, ok := g.Members[mid]; ok {
				select {
				case ch <- mem.Assignment:
				default:
				}
			}
			delete(g.pendingSyncs, mid)
		}

		// Return leader's own assignment
		assignment := bytes.Clone(m.Assignment)
		g.mu.Unlock()
		return assignment, protocol.ErrNone
	}

	// Follower — wait for leader to send assignments
	// Use this member's session timeout instead of a hardcoded value.
	syncTimeout := m.SessionTimeout
	ch := make(chan []byte, 1)
	if previous := g.pendingSyncs[memberID]; previous != nil {
		close(previous)
	}
	g.pendingSyncs[memberID] = ch
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.pendingSyncs[memberID] == ch {
			delete(g.pendingSyncs, memberID)
		}
	}()

	timer := time.NewTimer(syncTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, protocol.ErrRequestTimedOut
	case <-c.done:
		return nil, protocol.ErrCoordinatorNotAvailable
	case data, ok := <-ch:
		if !ok || data == nil {
			return nil, protocol.ErrRebalanceInProgress
		}
		return bytes.Clone(data), protocol.ErrNone
	case <-timer.C:
		return nil, protocol.ErrRebalanceInProgress
	}
}

// SyncGroupAssignment pairs a member ID with its binary assignment.
type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// Heartbeat validates a member's heartbeat.
func (c *Coordinator) Heartbeat(groupID string, generationID int32, memberID string) int16 {
	g := c.FindGroup(groupID)
	if g == nil {
		return protocol.ErrCoordinatorNotAvailable
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.Members[memberID]; !ok {
		return protocol.ErrUnknownMemberID
	}
	if generationID != g.GenerationID {
		return protocol.ErrIllegalGeneration
	}

	g.resetSessionTimer(memberID)

	if g.State == GroupJoining || g.State == GroupAwaitingSync {
		return protocol.ErrRebalanceInProgress
	}

	return protocol.ErrNone
}

// LeaveGroup removes a member from the group and triggers rebalance.
func (c *Coordinator) LeaveGroup(groupID string, memberID string) int16 {
	g := c.FindGroup(groupID)
	if g == nil {
		return protocol.ErrCoordinatorNotAvailable
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.Members[memberID]; !ok {
		return protocol.ErrUnknownMemberID
	}

	g.removeMember(memberID)

	if len(g.Members) > 0 {
		g.triggerRebalance()
	}

	return protocol.ErrNone
}

// handleSessionExpired is called when a member's session timer fires.
func (c *Coordinator) handleSessionExpired(g *ConsumerGroup, memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.Members[memberID]
	if g.State == GroupDead || m == nil || time.Since(m.LastHeartbeat) < m.SessionTimeout {
		return
	}
	logger.L.Warn("session expired", "group", g.GroupID, "member", memberID)
	g.removeMember(memberID)
	if len(g.Members) > 0 {
		g.triggerRebalance()
	}
}
