package network

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/memory"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

var (
	ErrFrameTooLarge      = errors.New("request frame exceeds admission limit")
	ErrAdmissionTimeout   = memory.ErrTimeout
	ErrAdmissionQueueFull = memory.ErrQueueFull
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
	Connections           int
	InFlightBytes         int64
	Waiting               int
	PeakConnections       int
	PeakInFlightBytes     int64
	RejectedConnections   uint64
	RejectedRequests      uint64
	TimedOutRequests      uint64
	Decoded               memory.Stats
	Fetch                 memory.Stats
	DecodeLimitRejections uint64
	Responses             memory.Stats
}

// Admission accounts for accepted handlers and their raw request frames, not
// decoded objects, retained group state, responses, kernel buffers or total RSS.
type Admission struct {
	mu               sync.Mutex
	limits           Limits
	stats            AdmissionStats
	frames           *memory.Pool
	decoded          *memory.Pool
	fetch            *memory.Pool
	responses        *memory.Pool
	memoryLimits     MemoryLimits
	lastRejectionLog time.Time
	replication      bool
}

func NewAdmission(limits Limits) (*Admission, error) {
	return NewAdmissionWithMemory(limits, DefaultMemoryLimits())
}

func NewAdmissionWithMemory(limits Limits, mem MemoryLimits) (*Admission, error) {
	if limits.MaxConnections < 1 || limits.MaxRequestBytes < 8 || limits.MaxRequestBytes > protocol.MaxRequestSize ||
		limits.MaxInFlightBytes < limits.MaxRequestBytes || limits.AdmissionWait <= 0 || limits.AdmissionWait > connectionReadTimeout {
		return nil, fmt.Errorf("invalid admission limits: positive connections, 8..%d request bytes, budget >= request limit, and wait <= %s required",
			protocol.MaxRequestSize, connectionReadTimeout)
	}
	if err := mem.Validate(); err != nil {
		return nil, err
	}
	frames, err := memory.NewPool(limits.MaxInFlightBytes, limits.MaxConnections, limits.AdmissionWait)
	if err != nil {
		return nil, err
	}
	decoded, err := memory.NewPool(mem.DecodedBudget, limits.MaxConnections, limits.AdmissionWait)
	if err != nil {
		return nil, err
	}
	fetch, err := memory.NewPool(mem.FetchBudget, limits.MaxConnections, limits.AdmissionWait)
	if err != nil {
		return nil, err
	}
	responses, err := memory.NewPool(mem.ResponseBudget, limits.MaxConnections, limits.AdmissionWait)
	if err != nil {
		return nil, err
	}
	return &Admission{limits: limits, frames: frames, decoded: decoded, fetch: fetch, responses: responses, memoryLimits: mem}, nil
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
	a, err := NewAdmissionWithMemory(Limits{MaxConnections: int(pairs), MaxRequestBytes: 64 << 10, MaxInFlightBytes: 1 << 20, AdmissionWait: 2 * time.Second},
		MemoryLimits{MaxDecodedBytes: 1 << 20, DecodedBudget: 8 << 20, MaxEntries: 1024, FetchMaxBytes: 100 << 20, FetchBudget: 256 << 20, ResponseMaxBytes: 64 << 10, ResponseBudget: 128 << 10})
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
	raw := a.frames.Stats()
	stats.Waiting = raw.Waiting
	stats.InFlightBytes, stats.PeakInFlightBytes = raw.Used, raw.Peak
	stats.RejectedRequests += raw.Rejected
	stats.TimedOutRequests = raw.TimedOut
	stats.Decoded = a.decoded.Stats()
	stats.Fetch = a.fetch.Stats()
	stats.Responses = a.responses.Stats()
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
	a.mu.Unlock()
	reservation, err := a.frames.Acquire(ctx, bytes)
	if err != nil {
		return nil, err
	}
	return reservation.Release, nil
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
