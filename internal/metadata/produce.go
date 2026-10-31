package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/ritiraj/kafka-go/internal/storage"
)

const DefaultMinInSyncReplicas = 2

var (
	ErrInvalidRequiredAcks          = errors.New("invalid required acknowledgements")
	ErrNotEnoughReplicas            = errors.New("not enough in-sync replicas before append")
	ErrNotEnoughReplicasAfterAppend = errors.New("not enough in-sync replicas after append")
	ErrProduceTimeout               = errors.New("produce replication deadline exceeded")
)

type ProduceOptions struct {
	BrokerID int32
	Acks     int16
	MinISR   int
	Deadline time.Time
}

type pendingProduce struct {
	minISR int
	err    error
}

// CheckProduce is an early admission check before schema/DLQ work. ProduceRecords
// rechecks the same conditions under the append lock to close the admission race.
func (p *Partition) CheckProduce(options ProduceOptions) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkProduce(options)
}

func (p *Partition) checkProduce(options ProduceOptions) error {
	if options.Acks != -1 && options.Acks != 0 && options.Acks != 1 {
		return ErrInvalidRequiredAcks
	}
	if options.MinISR < 1 {
		return fmt.Errorf("minimum ISR must be positive")
	}
	if p.LeaderID != options.BrokerID || !p.ReplicaAvailable(p.LeaderID) {
		return ErrNotLeader
	}
	if p.appendErr != nil {
		return fmt.Errorf("partition unavailable after append failure: %w", p.appendErr)
	}
	if p.ReplicaLEO[p.LeaderID] < p.LogEndOffset {
		return fmt.Errorf("%w: elected replica needs log reconciliation", ErrNotLeader)
	}
	if options.Acks == -1 && (!slices.Contains(p.ISRNodes, p.LeaderID) || len(p.ISRNodes) < options.MinISR) {
		return ErrNotEnoughReplicas
	}
	return nil
}

// ProduceRecords checks leadership and minimum ISR atomically with the local
// append. A nonnegative offset with an error means the write may already exist.
func (p *Partition) ProduceRecords(records []byte, options ProduceOptions) (int64, error) {
	return p.ProduceRecordsContext(context.Background(), records, options)
}

func (p *Partition) ProduceRecordsContext(ctx context.Context, records []byte, options ProduceOptions) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	records = bytes.Clone(records)
	var batches []storage.RecordBatch
	if len(records) > 0 {
		var err error
		batches, err = storage.ParseRecordBatches(records)
		if err != nil {
			return -1, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if err := p.checkProduce(options); err != nil {
		return -1, err
	}
	if len(records) == 0 {
		return p.NextOffset, nil
	}
	offset, err := p.appendRecords(records, batches, p.LogDirForBroker(options.BrokerID))
	if err != nil {
		return offset, err
	}
	if err := ctx.Err(); err != nil {
		return offset, err
	}
	if options.Acks != -1 {
		return offset, nil
	}
	target, epoch := p.LogEndOffset, p.LeaderEpoch
	waiter := &pendingProduce{minISR: options.MinISR}
	if p.pendingProduces == nil {
		p.pendingProduces = make(map[*pendingProduce]struct{})
	}
	p.pendingProduces[waiter] = struct{}{}
	defer delete(p.pendingProduces, waiter)
	timer := time.NewTimer(max(0, time.Until(options.Deadline)))
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return offset, err
		}
		if p.LeaderID != options.BrokerID || p.LeaderEpoch != epoch || !p.ReplicaAvailable(options.BrokerID) {
			return offset, ErrNotLeader
		}
		if p.appendErr != nil {
			return offset, fmt.Errorf("partition failed during acknowledgement: %w", p.appendErr)
		}
		if waiter.err != nil {
			return offset, waiter.err
		}
		if len(p.ISRNodes) < options.MinISR || !slices.Contains(p.ISRNodes, options.BrokerID) {
			return offset, ErrNotEnoughReplicasAfterAppend
		}
		if p.HighWatermark >= target {
			return offset, nil
		}
		if !time.Now().Before(options.Deadline) {
			return offset, ErrProduceTimeout
		}
		if p.changed == nil {
			p.changed = make(chan struct{})
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-changed:
		}
		p.mu.Lock()
	}
}

func (p *Partition) signalChange() {
	p.initCond()
	p.hwCond.Broadcast()
	if p.changed != nil {
		close(p.changed)
		p.changed = nil
	}
}

// ReplaceISR validates membership, advances HW and wakes pending produce requests.
// Caller must hold the partition lock. A lost minimum is remembered even if ISR
// recovers before the waiting goroutine is scheduled.
func (p *Partition) ReplaceISR(nodes []int32) error {
	seen := make(map[int32]bool, len(nodes))
	for _, id := range nodes {
		if seen[id] || !slices.Contains(p.ReplicaNodes, id) || !p.ReplicaAvailable(id) {
			return fmt.Errorf("invalid ISR member %d", id)
		}
		seen[id] = true
	}
	if slices.Equal(nodes, p.ISRNodes) {
		return nil
	}
	if p.PartitionEpoch == math.MaxInt32 {
		return fmt.Errorf("partition epoch exhausted")
	}
	p.ISRNodes = slices.Clone(nodes)
	p.PartitionEpoch++
	for waiter := range p.pendingProduces {
		if len(nodes) < waiter.minISR && waiter.err == nil {
			waiter.err = ErrNotEnoughReplicasAfterAppend
		}
	}
	p.AdvanceHighWatermark()
	p.signalChange()
	return nil
}

// ReplicaAvailable excludes offline or fenced replicas. Caller holds p.mu.
func (p *Partition) ReplicaAvailable(id int32) bool {
	return !slices.Contains(p.OfflineReplicas, id) && p.replicaErrors[id] == nil
}

// SetReplicaOnline is called by the controller; reachability alone never adds
// a recovered follower back to ISR.
func (p *Partition) SetReplicaOnline(id int32, online bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !slices.Contains(p.ReplicaNodes, id) {
		return nil
	}
	if online {
		nodes := slices.DeleteFunc(slices.Clone(p.OfflineReplicas), func(node int32) bool { return node == id })
		p.OfflineReplicas = nodes
	} else {
		if !slices.Contains(p.OfflineReplicas, id) {
			p.OfflineReplicas = append(p.OfflineReplicas, id)
		}
		nodes := slices.DeleteFunc(slices.Clone(p.ISRNodes), func(node int32) bool { return node == id })
		if err := p.ReplaceISR(nodes); err != nil {
			return err
		}
	}
	p.signalChange()
	return nil
}
