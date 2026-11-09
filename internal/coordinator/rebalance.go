package coordinator

import (
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
)

const defaultRebalancePhaseTimeout = 30 * time.Second

// All phase and collection helpers require g.mu.
func (g *ConsumerGroup) stopJoinCollection() {
	g.rebalanceTimer.Stop()
	g.rebalanceTimer = nil
	g.rebalanceSequence++
}

func (g *ConsumerGroup) stopPhaseDeadline() {
	g.phaseTimer.Stop()
	g.phaseTimer = nil
	g.phaseDeadline = time.Time{}
	g.phaseSequence++
}

func (g *ConsumerGroup) stopRebalanceTimers() {
	g.stopJoinCollection()
	g.stopPhaseDeadline()
}

func (g *ConsumerGroup) startPhaseDeadline(state GroupState) {
	g.stopPhaseDeadline()
	timeout := g.joinTimeout
	if state == GroupAwaitingSync {
		timeout = g.syncTimeout
	}
	if timeout <= 0 {
		timeout = defaultRebalancePhaseTimeout
	}
	sequence, generation := g.phaseSequence, g.GenerationID
	g.phaseDeadline = time.Now().Add(timeout)
	g.phaseTimer = g.after(timeout, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.expirePhase(state, generation, sequence)
	})
}

func (g *ConsumerGroup) expirePhase(state GroupState, generation int32, sequence uint64) {
	if g.State != state || g.GenerationID != generation || g.phaseSequence != sequence {
		return
	}
	logger.L.Warn("consumer group phase expired", "group", g.GroupID, "state", state, "generation", generation)
	g.stopPhaseDeadline()
	switch state {
	case GroupJoining:
		g.stopJoinCollection()
		// Heartbeats prove liveness, but only a pending Join proves that a
		// member participated in this round. Keep those callers and finish.
		for id := range g.Members {
			if g.pendingJoins[id] == nil {
				g.removeMember(id)
			}
		}
		if len(g.Members) > 0 {
			g.completeJoinPhase()
		}
	case GroupAwaitingSync:
		// The leader is responsible for publishing the assignment. Fence its
		// membership so remaining participants can elect a different leader.
		g.removeMember(g.LeaderID)
		if len(g.Members) > 0 {
			g.triggerRebalance()
		}
	}
}

func (g *ConsumerGroup) maybeCompleteJoin() {
	if g.State != GroupJoining || len(g.Members) == 0 || len(g.pendingJoins) != len(g.Members) {
		return
	}
	if g.rebalanceDelay <= 0 {
		g.completeJoinPhase()
		return
	}
	if g.rebalanceTimer != nil {
		return
	}
	g.rebalanceSequence++
	sequence := g.rebalanceSequence
	g.rebalanceTimer = g.after(g.rebalanceDelay, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.rebalanceSequence != sequence {
			return
		}
		g.rebalanceTimer = nil
		// Collection grace never authorizes members which did not join. A
		// missing member is removed only by a session or phase deadline.
		g.completeJoinPhase()
	})
}
