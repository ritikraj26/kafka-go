package coordinator

import "time"

// groupTimer accounts for callbacks even when Stop races with expiration.
type groupTimer struct {
	timer *time.Timer
	done  func()
}

func (g *ConsumerGroup) after(delay time.Duration, fn func()) *groupTimer {
	g.timers.Add(1)
	if g.allTimers != nil {
		g.allTimers.Add(1)
	}
	done := func() {
		g.timers.Done()
		if g.allTimers != nil {
			g.allTimers.Done()
		}
	}
	return &groupTimer{
		timer: time.AfterFunc(delay, func() { defer done(); fn() }),
		done:  done,
	}
}

func (timer *groupTimer) Stop() {
	if timer != nil && timer.timer.Stop() {
		timer.done()
	}
}

// Close prevents new groups, releases pending joins/syncs, stops all timers, and
// waits for already-running callbacks. It does not interrupt disk persistence.
func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.closeDone
		return
	}
	c.closed = true
	close(c.done)
	groups := make([]*ConsumerGroup, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.Unlock()
	for _, g := range groups {
		g.mu.Lock()
		g.rebalanceTimer.Stop()
		g.rebalanceTimer = nil
		for id := range g.Members {
			g.removeMember(id)
		}
		for id, ch := range g.pendingJoins {
			close(ch)
			delete(g.pendingJoins, id)
		}
		for id, ch := range g.pendingSyncs {
			close(ch)
			delete(g.pendingSyncs, id)
		}
		g.State = GroupDead
		g.publishState(0)
		g.mu.Unlock()
	}
	c.allTimers.Wait()
	c.mu.Lock()
	c.groups = make(map[string]*ConsumerGroup)
	c.offsets = make(offsetMap)
	c.offsetEntries = 0
	c.offsetBytes = 0
	c.mu.Unlock()
	close(c.closeDone)
}

func (c *Coordinator) cancelJoin(g *ConsumerGroup, memberID string, attempt uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m := g.Members[memberID]; m != nil && m.joinAttempt == attempt {
		g.removeMember(memberID)
		if len(g.Members) > 0 {
			g.triggerRebalance()
		}
	}
}
