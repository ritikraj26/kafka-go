package coordinator

import (
	"fmt"
	"strings"
	"sync"
)

type StateLimits struct {
	MaxGroups     int
	GroupBytes    int64
	PerGroupBytes int64
	MaxOffsets    int
	OffsetBytes   int64
}

func DefaultStateLimits() StateLimits {
	return StateLimits{MaxGroups: 1000, GroupBytes: 64 << 20, PerGroupBytes: 8 << 20, MaxOffsets: 100000, OffsetBytes: 64 << 20}
}

func (l StateLimits) Validate() error {
	if l.MaxGroups < 1 || l.PerGroupBytes < 1024 || l.GroupBytes < l.PerGroupBytes || l.MaxOffsets < 1 || l.OffsetBytes < 512 {
		return fmt.Errorf("invalid coordinator state limits")
	}
	return nil
}

type retainedUsage struct {
	mu          sync.Mutex
	used, limit int64
}

func (u *retainedUsage) change(delta int64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if delta > u.limit-u.used || delta < -u.used {
		return false
	}
	u.used += delta
	return true
}

func (c *Coordinator) SetStateLimits(limits StateLimits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.offsetReady || len(c.groups) > 0 {
		return fmt.Errorf("configure state limits before initialization")
	}
	c.limits = limits
	c.groupUsage.limit = limits.GroupBytes
	return nil
}

type StateStats struct {
	Groups, Offsets         int
	GroupBytes, OffsetBytes int64
}

func (c *Coordinator) StateStats() StateStats {
	c.offsetMu.RLock()
	defer c.offsetMu.RUnlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.groupUsage.mu.Lock()
	defer c.groupUsage.mu.Unlock()
	return StateStats{Groups: len(c.groups), Offsets: c.offsetEntries, GroupBytes: c.groupUsage.used, OffsetBytes: c.offsetBytes}
}

func groupCharge(id string) int64 { return 1024 + 2*int64(len(id)) }

func memberCharge(id string, protocols []MemberProtocol, assignment []byte) int64 {
	cost := int64(512) + 2*int64(len(id)+len(assignment))
	for _, p := range protocols {
		cost += 128 + 2*int64(len(p.Name)+len(p.Metadata))
	}
	return cost
}

func (g *ConsumerGroup) growState(next int64) bool {
	if next > g.maxRetained || next < 0 {
		return false
	}
	if next > g.retained {
		return g.usage.change(next - g.retained)
	}
	return true
}

func (g *ConsumerGroup) publishState(next int64) {
	if next < g.retained {
		g.usage.change(next - g.retained)
	}
	g.retained = next
}

// Called with c.mu held. Skip busy groups so admission never holds the registry
// lock while waiting behind a group's durable offset write.
func (c *Coordinator) reclaimEmptyGroups() {
	for id, g := range c.groups {
		if !g.mu.TryLock() {
			continue
		}
		if !g.initializing && g.State == GroupEmpty && len(g.Members) == 0 && len(g.pendingJoins) == 0 && len(g.pendingSyncs) == 0 {
			g.State = GroupDead
			g.stopRebalanceTimers()
			g.publishState(0)
			delete(c.groups, id)
		}
		g.mu.Unlock()
	}
}

func cloneProtocols(protocols []MemberProtocol) []MemberProtocol {
	copyOf := make([]MemberProtocol, len(protocols))
	for i, p := range protocols {
		copyOf[i] = MemberProtocol{Name: strings.Clone(p.Name), Metadata: append([]byte{}, p.Metadata...)}
		if p.Metadata == nil {
			copyOf[i].Metadata = nil
		}
	}
	return copyOf
}

func offsetCharge(group, topic string, metadata *string) int64 {
	cost := int64(512) + 2*int64(len(group)+len(topic))
	if metadata != nil {
		cost += 64 + 2*int64(len(*metadata))
	}
	return cost
}

func nextOffsetUsage(offsets offsetMap, entries int, retained int64, record offsetRecord) (int, int64) {
	old, exists := offsets[record.Group][record.Topic][record.Partition]
	if exists {
		retained -= offsetCharge(record.Group, record.Topic, old.Metadata)
	} else {
		entries++
	}
	return entries, retained + offsetCharge(record.Group, record.Topic, record.Metadata)
}

func offsetMapUsage(offsets offsetMap) (int, int64) {
	count := 0
	var retained int64
	for group, topics := range offsets {
		for topic, parts := range topics {
			for _, value := range parts {
				count++
				retained += offsetCharge(group, topic, value.Metadata)
			}
		}
	}
	return count, retained
}
