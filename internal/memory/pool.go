// Package memory provides cancellable, FIFO byte reservations for bounded buffers.
package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var (
	ErrTooLarge  = errors.New("reservation exceeds memory budget")
	ErrTimeout   = errors.New("memory admission timed out")
	ErrQueueFull = errors.New("memory admission queue is full")
)

type Stats struct {
	Used, Peak         int64
	Waiting            int
	Rejected, TimedOut uint64
}

type waiter struct {
	bytes   int64
	ready   chan struct{}
	granted bool
}

type Pool struct {
	mu         sync.Mutex
	capacity   int64
	maxWaiters int
	wait       time.Duration
	stats      Stats
	waiters    []*waiter
}

func NewPool(capacity int64, maxWaiters int, wait time.Duration) (*Pool, error) {
	if capacity <= 0 || maxWaiters < 1 || wait <= 0 {
		return nil, fmt.Errorf("memory capacity, queue bound and wait must be positive")
	}
	return &Pool{capacity: capacity, maxWaiters: maxWaiters, wait: wait}, nil
}

func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Waiting = len(p.waiters)
	return s
}

func (p *Pool) Acquire(ctx context.Context, bytes int64) (*Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if bytes < 1 || bytes > p.capacity {
		p.stats.Rejected++
		p.mu.Unlock()
		return nil, ErrTooLarge
	}
	if len(p.waiters) == 0 && bytes <= p.capacity-p.stats.Used {
		p.grant(bytes)
		p.mu.Unlock()
		return &Reservation{pool: p, bytes: bytes}, nil
	}
	if len(p.waiters) >= p.maxWaiters {
		p.stats.Rejected++
		p.mu.Unlock()
		return nil, ErrQueueFull
	}
	w := &waiter{bytes: bytes, ready: make(chan struct{})}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()
	waitCtx, cancel := context.WithTimeout(ctx, p.wait)
	defer cancel()
	select {
	case <-w.ready:
	case <-waitCtx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := waitCtx.Err(); err != nil {
		if w.granted {
			p.stats.Used -= bytes
		} else {
			for i, pending := range p.waiters {
				if pending == w {
					p.waiters = slices.Delete(p.waiters, i, i+1)
					break
				}
			}
		}
		p.drain()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		p.stats.Rejected++
		p.stats.TimedOut++
		return nil, ErrTimeout
	}
	return &Reservation{pool: p, bytes: bytes}, nil
}

func (p *Pool) grant(bytes int64) {
	p.stats.Used += bytes
	p.stats.Peak = max(p.stats.Peak, p.stats.Used)
}

func (p *Pool) drain() {
	for len(p.waiters) > 0 {
		w := p.waiters[0]
		if w.bytes > p.capacity-p.stats.Used {
			return
		}
		p.waiters[0] = nil
		p.waiters = p.waiters[1:]
		p.grant(w.bytes)
		w.granted = true
		close(w.ready)
	}
	p.waiters = nil
}

type Reservation struct {
	mu       sync.Mutex
	pool     *Pool
	bytes    int64
	released bool
}

// Resize never waits while holding capacity. Callers must discard owned buffers
// and release before queueing for a larger reservation if growth fails.
func (r *Reservation) Resize(bytes int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || bytes < 1 {
		return false
	}
	p := r.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	delta := bytes - r.bytes
	if delta > 0 && (len(p.waiters) > 0 || delta > p.capacity-p.stats.Used) {
		return false
	}
	p.stats.Used += delta
	p.stats.Peak = max(p.stats.Peak, p.stats.Used)
	r.bytes = bytes
	if delta < 0 {
		p.drain()
	}
	return true
}

func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.pool.mu.Lock()
	defer r.pool.mu.Unlock()
	r.pool.stats.Used -= r.bytes
	r.released = true
	r.pool.drain()
}
