package coordinator

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
	"github.com/google/uuid"
)

// offsetRecord is the JSON format for persisted offset commits.
type offsetRecord struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// Coordinator manages all consumer groups and committed offsets.
type Coordinator struct {
	mu             sync.RWMutex
	groups         map[string]*ConsumerGroup
	offsets        map[string]map[string]map[int32]int64 // group -> topic -> partition -> offset
	RebalanceDelay time.Duration                         // delay before completing a join round (allows more members to join)
	MaxGroupSize   int                                   // max members per group (0 = unlimited, default 256)
	offsetLogDir   string                                // directory for __consumer_offsets log persistence
}

// NewCoordinator creates an empty Coordinator.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		groups:         make(map[string]*ConsumerGroup),
		offsets:        make(map[string]map[string]map[int32]int64),
		RebalanceDelay: 0,
		MaxGroupSize:   256,
	}
}

// SetLogDir sets the directory used for offset log persistence.
// The offsets log will be stored at {logDir}/__consumer_offsets-0/00000000000000000000.log
func (c *Coordinator) SetLogDir(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsetLogDir = filepath.Join(dir, "__consumer_offsets-0")
}

// LoadOffsets replays the offset commit log from disk to rebuild the in-memory offset map.
func (c *Coordinator) LoadOffsets(logDir string) {
	c.SetLogDir(logDir)

	logFile := filepath.Join(c.offsetLogDir, "00000000000000000000.log")
	data, err := os.ReadFile(logFile)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.L.Warn("failed to read offset log", "err", err)
		}
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	dec := json.NewDecoder(bytes.NewReader(data))
	count := 0
	for dec.More() {
		var rec offsetRecord
		if err := dec.Decode(&rec); err != nil {
			logger.L.Warn("skipping malformed offset record", "err", err)
			continue
		}
		if c.offsets[rec.Group] == nil {
			c.offsets[rec.Group] = make(map[string]map[int32]int64)
		}
		if c.offsets[rec.Group][rec.Topic] == nil {
			c.offsets[rec.Group][rec.Topic] = make(map[int32]int64)
		}
		c.offsets[rec.Group][rec.Topic][rec.Partition] = rec.Offset
		count++
	}
	if count > 0 {
		logger.L.Info("loaded committed offsets from disk", "records", count)
	}
}

// getOrCreateGroup returns the group, creating it if needed.
func (c *Coordinator) getOrCreateGroup(groupID string) *ConsumerGroup {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		g = newConsumerGroup(groupID, c.handleSessionExpired)
		c.groups[groupID] = g
	}
	return g
}

// FindGroup returns the group or nil.
func (c *Coordinator) FindGroup(groupID string) *ConsumerGroup {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groups[groupID]
}

// JoinGroup implements the JoinGroup protocol. It may block until all members join.
func (c *Coordinator) JoinGroup(groupID string, memberID string, sessionTimeoutMs int32,
	protocolType string, protocols []MemberProtocol) *JoinGroupResult {

	g := c.getOrCreateGroup(groupID)
	g.mu.Lock()

	if g.State == GroupDead {
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrCoordinatorNotAvailable}
	}

	// Enforce max group size
	if memberID == "" && c.MaxGroupSize > 0 && len(g.Members) >= c.MaxGroupSize {
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrGroupMaxSizeReached}
	}

	// Assign member ID if empty (first join)
	if memberID == "" {
		memberID = uuid.New().String()
	}

	// Add or update member
	m, exists := g.Members[memberID]
	if !exists {
		m = &Member{
			MemberID:       memberID,
			SessionTimeout: time.Duration(sessionTimeoutMs) * time.Millisecond,
			Protocols:      protocols,
		}
		g.Members[memberID] = m
	} else {
		m.Protocols = protocols
		m.SessionTimeout = time.Duration(sessionTimeoutMs) * time.Millisecond
	}

	g.ProtocolType = protocolType
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
	g.pendingJoins[memberID] = ch

	// Check if all members have rejoined (pendingJoins == Members count)
	allPresent := len(g.pendingJoins) == len(g.Members)

	if allPresent {
		if c.RebalanceDelay > 0 && g.rebalanceTimer == nil {
			// Start a timer — complete join after delay (allows more members to arrive)
			delay := c.RebalanceDelay
			g.rebalanceTimer = time.AfterFunc(delay, func() {
				g.mu.Lock()
				defer g.mu.Unlock()
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
	timeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	select {
	case result, ok := <-ch:
		if !ok || result == nil {
			return &JoinGroupResult{ErrorCode: protocol.ErrRebalanceInProgress, MemberID: memberID}
		}
		return result
	case <-time.After(timeout):
		// Timeout — remove member and return error
		g.mu.Lock()
		g.removeMember(memberID)
		g.mu.Unlock()
		return &JoinGroupResult{ErrorCode: protocol.ErrRebalanceInProgress, MemberID: memberID}
	}
}

// SyncGroup implements the SyncGroup protocol for the leader and followers.
func (c *Coordinator) SyncGroup(groupID string, generationID int32, memberID string,
	assignments []SyncGroupAssignment) ([]byte, int16) {

	g := c.FindGroup(groupID)
	if g == nil {
		return nil, protocol.ErrCoordinatorNotAvailable
	}

	g.mu.Lock()

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
			g.mu.Unlock()
			return m.Assignment, protocol.ErrNone
		}
		g.mu.Unlock()
		return nil, protocol.ErrRebalanceInProgress
	}

	g.resetSessionTimer(memberID)

	// If this is the leader, distribute assignments
	if memberID == g.LeaderID && len(assignments) > 0 {
		for _, a := range assignments {
			if mem, ok := g.Members[a.MemberID]; ok {
				mem.Assignment = a.Assignment
			}
		}

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
		g.mu.Unlock()
		return m.Assignment, protocol.ErrNone
	}

	// Follower — wait for leader to send assignments
	// Use this member's session timeout instead of a hardcoded value.
	syncTimeout := m.SessionTimeout
	ch := make(chan []byte, 1)
	g.pendingSyncs[memberID] = ch
	g.mu.Unlock()

	select {
	case data, ok := <-ch:
		if !ok || data == nil {
			return nil, protocol.ErrRebalanceInProgress
		}
		return data, protocol.ErrNone
	case <-time.After(syncTimeout):
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

// CommitOffset stores a committed offset for a group/topic/partition.
// Lock ordering: always c.mu first, then g.mu — never reversed.
func (c *Coordinator) CommitOffset(groupID string, generationID int32, memberID string,
	topic string, partition int32, offset int64) int16 {

	g := c.FindGroup(groupID)
	if g == nil {
		return protocol.ErrCoordinatorNotAvailable
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate member and generation under g.mu while already holding c.mu.
	g.mu.Lock()
	if _, ok := g.Members[memberID]; !ok {
		g.mu.Unlock()
		return protocol.ErrUnknownMemberID
	}
	if generationID != g.GenerationID {
		g.mu.Unlock()
		return protocol.ErrIllegalGeneration
	}
	g.mu.Unlock()

	if c.offsets[groupID] == nil {
		c.offsets[groupID] = make(map[string]map[int32]int64)
	}
	if c.offsets[groupID][topic] == nil {
		c.offsets[groupID][topic] = make(map[int32]int64)
	}
	c.offsets[groupID][topic][partition] = offset

	// Persist to offset log (best-effort — don't fail the commit on I/O error)
	if c.offsetLogDir != "" {
		c.persistOffset(groupID, topic, partition, offset)
	}

	return protocol.ErrNone
}

// persistOffset appends an offset commit record to the durable log.
// Caller must hold c.mu.
func (c *Coordinator) persistOffset(group, topic string, partition int32, offset int64) {
	if err := os.MkdirAll(c.offsetLogDir, 0755); err != nil {
		logger.L.Warn("failed to create offset log dir", "err", err)
		return
	}
	logFile := filepath.Join(c.offsetLogDir, "00000000000000000000.log")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.L.Warn("failed to open offset log", "err", err)
		return
	}
	defer f.Close()
	rec := offsetRecord{Group: group, Topic: topic, Partition: partition, Offset: offset}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		logger.L.Warn("failed to write offset record", "err", err)
		return
	}
	if err := f.Sync(); err != nil {
		logger.L.Warn("failed to fsync offset log", "err", err)
	}
}

// FetchOffset returns the committed offset for a group/topic/partition.
// Returns -1 if no offset has been committed.
func (c *Coordinator) FetchOffset(groupID string, topic string, partition int32) (int64, int16) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if topics, ok := c.offsets[groupID]; ok {
		if parts, ok := topics[topic]; ok {
			if off, ok := parts[partition]; ok {
				return off, protocol.ErrNone
			}
		}
	}
	return -1, protocol.ErrNone
}

// handleSessionExpired is called when a member's session timer fires.
func (c *Coordinator) handleSessionExpired(g *ConsumerGroup, memberID string) {
	logger.L.Warn("session expired", "group", g.GroupID, "member", memberID)
	g.mu.Lock()
	defer g.mu.Unlock()

	g.removeMember(memberID)
	if len(g.Members) > 0 {
		g.triggerRebalance()
	}
}
