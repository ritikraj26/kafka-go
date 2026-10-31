package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
)

// ISRManager periodically evaluates ISR membership for all partitions.
type ISRManager struct {
	metaMgr      *metadata.Manager
	interval     time.Duration
	lagTimeMaxMs int64
	wg           sync.WaitGroup
	startMu      sync.Mutex
	started      bool
}

// NewISRManager creates an ISR manager with the given check interval and lag threshold.
func NewISRManager(metaMgr *metadata.Manager, interval time.Duration, lagTimeMaxMs int64) *ISRManager {
	return &ISRManager{
		metaMgr:      metaMgr,
		interval:     interval,
		lagTimeMaxMs: lagTimeMaxMs,
	}
}

// Start begins the background ISR maintenance loop.
func (im *ISRManager) Start(ctx context.Context) error {
	im.startMu.Lock()
	defer im.startMu.Unlock()
	if im.started {
		return fmt.Errorf("ISR manager already started")
	}
	if im.interval <= 0 || im.lagTimeMaxMs <= 0 {
		return fmt.Errorf("ISR timing values must be positive")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	im.started = true
	im.wg.Add(1)
	go func() {
		defer im.wg.Done()
		ticker := time.NewTicker(im.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				im.checkAll(ctx)
			}
		}
	}()
	return nil
}

// Wait blocks until the ISR manager stops.
func (im *ISRManager) Wait() {
	im.wg.Wait()
}

// checkAll evaluates ISR for every partition.
func (im *ISRManager) checkAll(ctx context.Context) {
	for _, topicName := range im.metaMgr.ListTopics() {
		if ctx.Err() != nil {
			return
		}
		topic := im.metaMgr.GetTopic(topicName)
		if topic == nil {
			continue
		}
		for i := range topic.Partitions {
			p := &topic.Partitions[i]
			p.RunUnderLock(func() {
				if ctx.Err() != nil {
					return
				}
				if err := UpdateISR(p, im.lagTimeMaxMs); err != nil {
					logger.L.Error("failed to update ISR", "topic", topicName, "partition", p.Index, "err", err)
				}
			})
		}
	}
	logger.L.Debug("ISR maintenance tick completed")
}
