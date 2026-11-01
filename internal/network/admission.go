package network

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

var (
	ErrFrameTooLarge      = errors.New("request frame exceeds admission limit")
	ErrAdmissionTimeout   = errors.New("request admission timed out")
	ErrAdmissionQueueFull = errors.New("request admission queue is full")
)

type Limits struct {
	MaxConnections   int
	MaxRequestBytes  int64
	MaxInFlightBytes int64
	AdmissionWait    time.Duration
}

func DefaultLimits() Limits {
	return Limits{MaxConnections: 256, MaxRequestBytes: protocol.MaxRequestSize, MaxInFlightBytes: 256 << 20, AdmissionWait: 2 * time.Second}
}

type AdmissionStats struct {
	Connections         int
	InFlightBytes       int64
	Waiting             int
	PeakConnections     int
	PeakInFlightBytes   int64
	RejectedConnections uint64
	RejectedRequests    uint64
	TimedOutRequests    uint64
}

type admissionWaiter struct {
	bytes   int64
	ready   chan struct{}
	granted bool
}

// Admission accounts for accepted handlers and their raw request frames, not
// decoded objects, retained group state, responses, kernel buffers or total RSS.
type Admission struct {
	mu               sync.Mutex
	limits           Limits
	stats            AdmissionStats
	waiters          []*admissionWaiter
	lastRejectionLog time.Time
	replication      bool
}

func NewAdmission(limits Limits) (*Admission, error) {
	if limits.MaxConnections < 1 || limits.MaxRequestBytes < 8 || limits.MaxRequestBytes > protocol.MaxRequestSize ||
		limits.MaxInFlightBytes < limits.MaxRequestBytes || limits.AdmissionWait <= 0 || limits.AdmissionWait > connectionReadTimeout {
		return nil, fmt.Errorf("invalid admission limits: positive connections, 8..%d request bytes, budget >= request limit, and wait <= %s required",
			protocol.MaxRequestSize, connectionReadTimeout)
	}
	return &Admission{limits: limits}, nil
}

// NewReplicationAdmission reserves enough sockets for one connection per
// follower/leader pair. Internal Fetch v0 requests contain one partition.
func NewReplicationAdmission(brokers int) (*Admission, error) {
	if brokers < 1 || brokers > 65535 {
		return nil, fmt.Errorf("invalid replication broker count")
	}
	pairs := max(int64(1), int64(brokers)*int64(brokers-1))
	if pairs > math.MaxInt {
		return nil, fmt.Errorf("replication connection count exceeds platform limit")
	}
	a, err := NewAdmission(Limits{MaxConnections: int(pairs), MaxRequestBytes: 64 << 10, MaxInFlightBytes: 1 << 20, AdmissionWait: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	a.replication = true
	return a, nil
}

func (a *Admission) Stats() AdmissionStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats
	stats.Waiting = len(a.waiters)
	return stats
}

func (a *Admission) tryConnection() (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stats.Connections >= a.limits.MaxConnections {
		a.stats.RejectedConnections++
		return nil, false
	}
	a.stats.Connections++
	a.stats.PeakConnections = max(a.stats.PeakConnections, a.stats.Connections)
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.stats.Connections--
			a.mu.Unlock()
		})
	}, true
}

// Reserve waits FIFO for a whole frame reservation. The returned release is
// idempotent and must remain held through parsing, processing and response I/O.
func (a *Admission) Reserve(ctx context.Context, bytes int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	if bytes < 8 || bytes > a.limits.MaxRequestBytes {
		a.stats.RejectedRequests++
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, bytes)
	}
	if len(a.waiters) == 0 && bytes <= a.limits.MaxInFlightBytes-a.stats.InFlightBytes {
		a.grant(bytes)
		a.mu.Unlock()
		return a.release(bytes), nil
	}
	if len(a.waiters) >= a.limits.MaxConnections {
		a.stats.RejectedRequests++
		a.mu.Unlock()
		return nil, ErrAdmissionQueueFull
	}
	waiter := &admissionWaiter{bytes: bytes, ready: make(chan struct{})}
	a.waiters = append(a.waiters, waiter)
	a.mu.Unlock()

	waitCtx, cancel := context.WithTimeout(ctx, a.limits.AdmissionWait)
	defer cancel()
	select {
	case <-waiter.ready:
	case <-waitCtx.Done():
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := waitCtx.Err(); err != nil {
		if waiter.granted {
			a.stats.InFlightBytes -= bytes
		} else {
			for i, w := range a.waiters {
				if w == waiter {
					a.waiters = slices.Delete(a.waiters, i, i+1)
					break
				}
			}
		}
		a.drain()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		a.stats.RejectedRequests++
		a.stats.TimedOutRequests++
		return nil, ErrAdmissionTimeout
	}
	return a.release(bytes), nil
}

func (a *Admission) grant(bytes int64) {
	a.stats.InFlightBytes += bytes
	a.stats.PeakInFlightBytes = max(a.stats.PeakInFlightBytes, a.stats.InFlightBytes)
}

func (a *Admission) drain() {
	for len(a.waiters) > 0 {
		next := a.waiters[0]
		if next.bytes > a.limits.MaxInFlightBytes-a.stats.InFlightBytes {
			return
		}
		a.waiters[0] = nil
		a.waiters = a.waiters[1:]
		a.grant(next.bytes)
		next.granted = true
		close(next.ready)
	}
	a.waiters = nil
}

func (a *Admission) release(bytes int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.stats.InFlightBytes -= bytes
			a.drain()
		})
	}
}

// All rejections are counted; logs are sampled to avoid a connection-flood log storm.
func (a *Admission) logRejection() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if time.Since(a.lastRejectionLog) < time.Second {
		return false
	}
	a.lastRejectionLog = time.Now()
	return true
}
