package fetch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/memory"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
)

type partResult struct {
	index      int32
	errCode    int16
	hwm, start int64
	records    []byte
	partition  *metadata.Partition
	plan       *metadata.FetchPlan
}

type topicResult struct {
	id    [16]byte
	name  string
	parts []partResult
}

type MemoryPolicy struct {
	Pool         *memory.Pool
	MaxDataBytes int
	MaxEntries   int
}

type Payload struct {
	Body        []byte
	reservation *memory.Reservation
}

func (p *Payload) Release() {
	p.Body = nil
	p.reservation.Release()
}

func resolveTopic(topic protocol.FetchTopic, m *metadata.Manager) *metadata.Topic {
	if topic.TopicName != "" {
		return m.GetTopic(topic.TopicName)
	}
	return m.GetTopicByID(topic.TopicID)
}

func collectPlans(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, maxBytes, maxBatches int, brokerID int32, modern bool) ([]topicResult, int, int, bool, bool) {
	total, usedBatches := 0, 0
	hasError, limited := false, false
	results := make([]topicResult, 0, len(req.Topics))
	for _, requestedTopic := range req.Topics {
		tr := topicResult{id: requestedTopic.TopicID, name: requestedTopic.TopicName}
		topic := resolveTopic(requestedTopic, m)
		for _, requested := range requestedTopic.Partitions {
			if ctx.Err() != nil {
				return results, total, usedBatches, true, limited
			}
			pr := partResult{index: requested.PartitionIndex, hwm: -1, start: -1}
			if topic == nil {
				pr.errCode = protocol.ErrUnknownTopicOrPartition
				if modern {
					pr.errCode = protocol.ErrUnknownTopicID
				}
			} else {
				for i := range topic.Partitions {
					if topic.Partitions[i].Index == requested.PartitionIndex {
						pr.partition = &topic.Partitions[i]
						break
					}
				}
				if pr.partition == nil {
					pr.errCode = protocol.ErrUnknownTopicOrPartition
				}
			}
			if p := pr.partition; p != nil {
				if requested.MaxBytes < 0 || req.MaxBytes < 0 || req.MinBytes < 0 || req.MaxWaitMs < 0 {
					pr.errCode = protocol.ErrInvalidRequest
				} else {
					plan, err := p.PlanFetch(ctx, metadata.FetchOptions{
						BrokerID: brokerID, ReplicaID: req.ReplicaID, LeaderEpoch: requested.CurrentLeaderEpoch,
						Offset: requested.FetchOffset, MaxBytes: min(max(0, maxBytes-total), int(requested.MaxBytes)),
						AllowOversizedBatch: total == 0, StrictFirstBatchLimit: !modern, FirstBatchMaxBytes: int(requested.MaxBytes),
						MaxBatches: max(0, maxBatches-usedBatches),
					})
					if plan != nil {
						pr.hwm, pr.start = plan.HighWatermark, plan.LogStart
						usedBatches += plan.Batches
						limited = limited || plan.Limited
					}
					if err != nil {
						pr.errCode = fetchErrorCode(err)
						if pr.errCode == protocol.ErrUnknownServerError {
							logger.L.Error("failed to plan fetch", "topic", topic.Name, "partition", requested.PartitionIndex, "err", err)
						}
					} else {
						pr.plan = plan
						total += plan.Bytes
					}
				}
			}
			hasError = hasError || pr.errCode != protocol.ErrNone
			tr.parts = append(tr.parts, pr)
		}
		results = append(results, tr)
	}
	return results, total, usedBatches, hasError, limited
}

func materialize(ctx context.Context, topics []topicResult) (int, error) {
	total := 0
	for i := range topics {
		for j := range topics[i].parts {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			p := &topics[i].parts[j]
			if p.plan == nil {
				continue
			}
			var err error
			p.records, err = p.partition.ReadFetchPlan(ctx, p.plan)
			if err != nil {
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				p.errCode = fetchErrorCode(err)
				logger.L.Warn("failed to read planned fetch", "partition", p.index, "err", err)
			}
			total += len(p.records)
		}
	}
	return total, nil
}

// collectData remains useful to direct callers testing a single read. Network
// responses use BuildWithMemory so allocation cannot precede admission.
func collectData(req *protocol.FetchRequest, m *metadata.Manager, maxBytes int, brokerID int32, modern bool) ([]topicResult, int, bool) {
	topics, _, _, failed, _ := collectPlans(context.Background(), req, m, maxBytes, 10000, brokerID, modern)
	total, err := materialize(context.Background(), topics)
	for _, topic := range topics {
		for _, part := range topic.parts {
			failed = failed || part.errCode != 0
		}
	}
	return topics, total, failed || err != nil
}

func fetchErrorCode(err error) int16 {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return protocol.ErrRequestTimedOut
	case errors.Is(err, metadata.ErrOffsetOutOfRange):
		return protocol.ErrOffsetOutOfRange
	case errors.Is(err, metadata.ErrNotLeader):
		return protocol.ErrNotLeaderOrFollower
	case errors.Is(err, metadata.ErrFencedEpoch):
		return protocol.ErrFencedLeaderEpoch
	case errors.Is(err, metadata.ErrUnknownEpoch):
		return protocol.ErrUnknownLeaderEpoch
	case errors.Is(err, metadata.ErrInvalidFetch):
		return protocol.ErrInvalidRequest
	default:
		return protocol.ErrUnknownServerError
	}
}

// BuildWithMemory returns an owned response whose reservation must be held until
// network delivery finishes. Growth is nonblocking; failed growth drops the plan
// and releases capacity before queueing for a full worst-case reservation.
func BuildWithMemory(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, brokerID int32, modern bool, policy MemoryPolicy) (*Payload, error) {
	if policy.Pool == nil || policy.MaxDataBytes < 1 || policy.MaxDataBytes > storage.MaxRecordBatchBytes || policy.MaxEntries < 1 {
		return nil, fmt.Errorf("invalid fetch memory policy")
	}
	entries := len(req.Topics)
	overhead := int64(1024)
	for _, topic := range req.Topics {
		entries += len(topic.Partitions)
		overhead += 64 + int64(len(topic.TopicName)) + 64*int64(len(topic.Partitions))
	}
	if entries > policy.MaxEntries {
		return nil, protocol.ErrDecodeLimit
	}
	maxData := policy.MaxDataBytes
	if modern {
		maxData = min(maxData, max(0, int(req.MaxBytes)))
	}
	planning := int64(1024) + 512*(int64(entries)+int64(policy.MaxEntries))
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)
	errorCode := protocol.ErrNone
	if modern && req.SessionID != 0 {
		errorCode = protocol.ErrFetchSessionIDNotFound
	} else if req.MinBytes < 0 || req.MaxBytes < 0 || req.MaxWaitMs < 0 || req.ReplicaID < -1 {
		errorCode = protocol.ErrInvalidRequest
	}
	for {
		lease, err := policy.Pool.Acquire(ctx, planning+overhead)
		if err != nil {
			return nil, err
		}
		payload, wait, err := buildAttempt(ctx, req, m, brokerID, modern, policy, lease, maxData, entries, planning, overhead, deadline, errorCode)
		if err != nil || !wait {
			return payload, err
		}
		timer := time.NewTimer(min(5*time.Millisecond, max(0, time.Until(deadline))))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func buildAttempt(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, brokerID int32, modern bool, policy MemoryPolicy,
	lease *memory.Reservation, maxData, entries int, planning, overhead int64, deadline time.Time, errorCode int16) (*Payload, bool, error) {
	defer func() {
		if lease != nil {
			lease.Release()
		}
	}()
	var topics []topicResult
	total, batches := 0, 0
	if errorCode == protocol.ErrNone {
		var failed, limited bool
		topics, total, batches, failed, limited = collectPlans(ctx, req, m, maxData, policy.MaxEntries, brokerID, modern)
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if !failed && !limited && total < int(req.MinBytes) && total < maxData && time.Now().Before(deadline) {
			return nil, true, nil
		}
	}
	needed := int64(1024) + 512*(int64(entries)+int64(batches)) + overhead + 2*int64(total)
	if !lease.Resize(needed) {
		topics = nil
		lease.Release()
		lease = nil
		var err error
		lease, err = policy.Pool.Acquire(ctx, planning+overhead+2*int64(storage.MaxRecordBatchBytes))
		if err != nil {
			return nil, false, err
		}
		if errorCode == protocol.ErrNone {
			topics, total, batches, _, _ = collectPlans(ctx, req, m, maxData, policy.MaxEntries, brokerID, modern)
		}
		needed = int64(1024) + 512*(int64(entries)+int64(batches)) + overhead + 2*int64(total)
		if !lease.Resize(needed) {
			return nil, false, fmt.Errorf("fetch plan exceeded its admitted bound")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	dataBytes, err := materialize(ctx, topics)
	if err != nil {
		return nil, false, err
	}
	body, err := encode(topics, modern, errorCode, int(overhead)+dataBytes)
	if err != nil {
		return nil, false, err
	}
	payload := &Payload{Body: body, reservation: lease}
	lease = nil
	return payload, false, nil
}

func encode(topics []topicResult, modern bool, code int16, capacity int) ([]byte, error) {
	e := protocol.NewFixedEncoder(capacity)
	if modern {
		e.WriteInt32(0)
		e.WriteInt16(code)
		e.WriteInt32(0)
		e.WriteUnsignedVarint(uint64(len(topics) + 1))
	} else {
		e.WriteArrayLen(len(topics))
	}
	for _, topic := range topics {
		if modern {
			e.WriteUUID(topic.id)
			e.WriteUnsignedVarint(uint64(len(topic.parts) + 1))
		} else {
			e.WriteString(topic.name)
			e.WriteArrayLen(len(topic.parts))
		}
		for _, part := range topic.parts {
			e.WriteInt32(part.index)
			e.WriteInt16(part.errCode)
			e.WriteInt64(part.hwm)
			if modern {
				if part.errCode == 0 {
					e.WriteInt64(part.hwm)
					e.WriteInt64(part.start)
				} else {
					e.WriteInt64(-1)
					e.WriteInt64(-1)
				}
				e.WriteByte(0)
				e.WriteInt32(-1)
				if len(part.records) == 0 {
					e.WriteByte(0)
				} else {
					e.WriteCompactBytes(part.records)
				}
				e.WriteTagBuffer()
			} else {
				e.WriteBytes(part.records)
			}
		}
		if modern {
			e.WriteTagBuffer()
		}
	}
	if modern {
		e.WriteTagBuffer()
	}
	if err := e.Err(); err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

func directBody(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, modern bool, ids []int32) ([]byte, error) {
	pool, err := memory.NewPool(512<<20, 1, 2*time.Second)
	if err != nil {
		return nil, err
	}
	brokerID := int32(1)
	if len(ids) > 0 {
		brokerID = ids[0]
	}
	policy := MemoryPolicy{Pool: pool, MaxDataBytes: 16 << 20, MaxEntries: 10000}
	if req.ReplicaID >= 0 {
		policy.MaxDataBytes = storage.MaxRecordBatchBytes
	}
	payload, err := BuildWithMemory(ctx, req, m, brokerID, modern, policy)
	if err != nil {
		return nil, err
	}
	body := payload.Body
	payload.Release()
	return body, nil
}

func BuildBodyContext(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, ids ...int32) ([]byte, error) {
	return directBody(ctx, req, m, true, ids)
}
func BuildBodyV0Context(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, ids ...int32) ([]byte, error) {
	return directBody(ctx, req, m, false, ids)
}
func BuildBody(req *protocol.FetchRequest, m *metadata.Manager, ids ...int32) []byte {
	body, err := BuildBodyContext(context.Background(), req, m, ids...)
	if err != nil {
		logger.L.Error("fetch response failed", "err", err)
	}
	return body
}
func BuildBodyV0(req *protocol.FetchRequest, m *metadata.Manager, ids ...int32) []byte {
	body, err := BuildBodyV0Context(context.Background(), req, m, ids...)
	if err != nil {
		logger.L.Error("fetch response failed", "err", err)
	}
	return body
}
