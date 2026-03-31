package controller

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/replication"
)

// Controller monitors broker health and triggers leader election when a leader goes offline.
// It periodically probes each broker's TCP port. If a broker fails consecutive health checks,
// it is marked dead, removed from ISR, and any partitions it leads are reassigned.
type Controller struct {
	metaMgr  *metadata.Manager
	registry *broker.Registry
	interval time.Duration // health check interval

	// Number of consecutive failures before a broker is declared dead.
	failureThreshold int

	// Track consecutive failures per broker.
	mu       sync.Mutex
	failures map[int32]int  // brokerID -> consecutive failure count
	alive    map[int32]bool // brokerID -> currently considered alive

	wg sync.WaitGroup
}

// NewController creates a controller that checks broker health at the given interval.
func NewController(metaMgr *metadata.Manager, registry *broker.Registry, interval time.Duration) *Controller {
	return &Controller{
		metaMgr:          metaMgr,
		registry:         registry,
		interval:         interval,
		failureThreshold: 3,
		failures:         make(map[int32]int),
		alive:            make(map[int32]bool),
	}
}

// Start begins the background health-check and failover loop.
func (c *Controller) Start(ctx context.Context) {
	// Mark all brokers as alive initially.
	for _, b := range c.registry.All() {
		c.alive[b.ID] = true
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.healthCheck()
			}
		}
	}()
}

// Wait blocks until the controller loop exits.
func (c *Controller) Wait() {
	c.wg.Wait()
}

// healthCheck probes every broker and handles newly-dead or newly-recovered brokers.
func (c *Controller) healthCheck() {
	for _, b := range c.registry.All() {
		reachable := c.probe(b)

		c.mu.Lock()
		wasAlive := c.alive[b.ID]

		if reachable {
			c.failures[b.ID] = 0
			if !wasAlive {
				// Broker came back online.
				c.alive[b.ID] = true
				c.mu.Unlock()
				logger.L.Info("controller: broker recovered", "broker", b.ID)
				c.onBrokerRecovered(b.ID)
				continue
			}
			c.mu.Unlock()
			continue
		}

		// Not reachable — increment failure counter.
		c.failures[b.ID]++
		count := c.failures[b.ID]

		if wasAlive && count >= c.failureThreshold {
			c.alive[b.ID] = false
			c.mu.Unlock()
			logger.L.Warn("controller: broker declared dead",
				"broker", b.ID, "consecutiveFailures", count)
			c.onBrokerDead(b.ID)
		} else {
			c.mu.Unlock()
			if count < c.failureThreshold {
				logger.L.Debug("controller: broker unreachable",
					"broker", b.ID, "failures", count, "threshold", c.failureThreshold)
			}
		}
	}
}

// probe checks if a broker's TCP port is accepting connections.
func (c *Controller) probe(b *broker.Broker) bool {
	addr := fmt.Sprintf("%s:%d", b.Host, b.Port)
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// onBrokerDead handles a broker that has been declared dead:
// 1. Remove it from ISR for all partitions
// 2. Elect a new leader for any partition it was leading
func (c *Controller) onBrokerDead(deadBrokerID int32) {
	for _, topicName := range c.metaMgr.ListTopics() {
		topic := c.metaMgr.GetTopic(topicName)
		if topic == nil {
			continue
		}
		for i := range topic.Partitions {
			p := &topic.Partitions[i]

			// Remove dead broker from ISR
			p.RunUnderLock(func() {
				replication.ShrinkISR(p, deadBrokerID)
			})

			// If this broker was the leader, elect a new one
			if p.LeaderID == deadBrokerID {
				if err := c.metaMgr.ElectLeader(topicName, p.Index); err != nil {
					logger.L.Error("controller: leader election failed",
						"topic", topicName, "partition", p.Index, "err", err)
				} else {
					logger.L.Info("controller: new leader elected",
						"topic", topicName, "partition", p.Index, "newLeader", p.LeaderID)
				}
			}
		}
	}
}

// onBrokerRecovered re-adds a broker to replica sets. Its FollowerManager will
// automatically start fetching and catching up. Once caught up, the ISRManager
// will add it back to the ISR.
func (c *Controller) onBrokerRecovered(brokerID int32) {
	logger.L.Info("controller: broker back online, will rejoin ISR after catch-up",
		"broker", brokerID)
}

// IsAlive returns whether the controller considers a broker alive.
func (c *Controller) IsAlive(brokerID int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive[brokerID]
}
