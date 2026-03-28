package replication

import (
	"context"
	"sync"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// ISRManager periodically evaluates ISR membership for all partitions.
type ISRManager struct {
	metaMgr      *metadata.Manager
	interval     time.Duration
	lagTimeMaxMs int64
	wg           sync.WaitGroup
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
func (im *ISRManager) Start(ctx context.Context) {
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
				im.checkAll()
			}
		}
	}()
}

// Wait blocks until the ISR manager stops.
func (im *ISRManager) Wait() {
	im.wg.Wait()
}

// checkAll evaluates ISR for every partition.
func (im *ISRManager) checkAll() {
	for _, topicName := range im.metaMgr.ListTopics() {
		topic := im.metaMgr.GetTopic(topicName)
		if topic == nil {
			continue
		}
		for i := range topic.Partitions {
			p := &topic.Partitions[i]
			p.RunUnderLock(func() {
				UpdateISR(p, im.lagTimeMaxMs)
			})
		}
	}
	logger.L.Debug("ISR maintenance tick completed")
}
