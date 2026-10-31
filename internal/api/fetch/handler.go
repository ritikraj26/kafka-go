package fetch

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

type partResult struct {
	index   int32
	errCode int16
	hwm     int64
	start   int64
	records []byte
}

type topicResult struct {
	id    [16]byte
	name  string
	parts []partResult
}

func resolveTopic(topic protocol.FetchTopic, m *metadata.Manager) *metadata.Topic {
	if topic.TopicName != "" {
		return m.GetTopic(topic.TopicName)
	}
	return m.GetTopicByID(topic.TopicID)
}

func collectData(req *protocol.FetchRequest, m *metadata.Manager, maxBytes int, brokerID int32, modern bool) ([]topicResult, int, bool) {
	return collectDataContext(context.Background(), req, m, maxBytes, brokerID, modern)
}

func collectDataContext(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, maxBytes int, brokerID int32, modern bool) ([]topicResult, int, bool) {
	total := 0
	hasError := false
	results := make([]topicResult, 0, len(req.Topics))
	for _, requestedTopic := range req.Topics {
		tr := topicResult{id: requestedTopic.TopicID, name: requestedTopic.TopicName}
		topic := resolveTopic(requestedTopic, m)
		for _, requested := range requestedTopic.Partitions {
			if ctx.Err() != nil {
				return results, total, true
			}
			pr := partResult{index: requested.PartitionIndex, hwm: -1, start: -1}
			var p *metadata.Partition
			if topic == nil {
				pr.errCode = protocol.ErrUnknownTopicOrPartition
				if modern {
					pr.errCode = protocol.ErrUnknownTopicID
				}
			} else {
				for i := range topic.Partitions {
					if topic.Partitions[i].Index == requested.PartitionIndex {
						p = &topic.Partitions[i]
						break
					}
				}
				if p == nil {
					pr.errCode = protocol.ErrUnknownTopicOrPartition
				}
			}
			if p != nil {
				if requested.MaxBytes < 0 || req.MaxBytes < 0 || req.MinBytes < 0 || req.MaxWaitMs < 0 {
					pr.errCode = protocol.ErrInvalidRequest
				} else {
					budget := min(max(0, maxBytes-total), int(requested.MaxBytes))
					result, err := p.FetchRecordsContext(ctx, metadata.FetchOptions{
						BrokerID: brokerID, ReplicaID: req.ReplicaID, LeaderEpoch: requested.CurrentLeaderEpoch,
						Offset: requested.FetchOffset, MaxBytes: budget, AllowOversizedBatch: modern && total == 0,
					})
					pr.hwm, pr.start = result.HighWatermark, result.LogStart
					if err != nil {
						pr.errCode = fetchErrorCode(err)
						if pr.errCode == protocol.ErrUnknownServerError {
							logger.L.Error("failed to fetch partition", "topic", topic.Name, "partition", requested.PartitionIndex, "err", err)
						}
					} else {
						pr.records = result.Records
						total += len(pr.records)
					}
				}
			}
			hasError = hasError || pr.errCode != protocol.ErrNone
			tr.parts = append(tr.parts, pr)
		}
		results = append(results, tr)
	}
	return results, total, hasError
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

func fetchData(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, brokerID int32, modern bool) ([]topicResult, error) {
	maxBytes := int(req.MaxBytes)
	if !modern {
		maxBytes = math.MaxInt32 // Fetch v0 has only per-partition byte limits.
	}
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		topics, total, hasError := collectDataContext(ctx, req, m, maxBytes, brokerID, modern)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if hasError || total >= int(req.MinBytes) || total >= maxBytes || !time.Now().Before(deadline) {
			return topics, nil
		}
		timer := time.NewTimer(min(5*time.Millisecond, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// BuildBody encodes Fetch v16. The first nonempty batch may exceed the byte
// budgets to allow progress; later batches and partitions obey the remaining budget.
func BuildBody(req *protocol.FetchRequest, m *metadata.Manager, localBrokerID ...int32) []byte {
	body, _ := BuildBodyContext(context.Background(), req, m, localBrokerID...)
	return body
}

func BuildBodyContext(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, localBrokerID ...int32) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	brokerID := int32(1)
	if len(localBrokerID) > 0 {
		brokerID = localBrokerID[0]
	}
	var topics []topicResult
	errorCode := protocol.ErrNone
	if req.SessionID != 0 {
		errorCode = protocol.ErrFetchSessionIDNotFound
	} else if req.MinBytes < 0 || req.MaxBytes < 0 || req.MaxWaitMs < 0 || req.ReplicaID < -1 {
		errorCode = protocol.ErrInvalidRequest
	} else {
		var err error
		topics, err = fetchData(ctx, req, m, brokerID, true)
		if err != nil {
			return nil, err
		}
	}
	e := protocol.NewEncoder()
	e.WriteInt32(0)
	e.WriteInt16(errorCode)
	e.WriteInt32(0) // Stateless fetch: do not claim an incremental session.
	e.WriteUnsignedVarint(uint64(len(topics) + 1))
	for _, topic := range topics {
		e.WriteUUID(topic.id)
		e.WriteUnsignedVarint(uint64(len(topic.parts) + 1))
		for _, part := range topic.parts {
			e.WriteInt32(part.index)
			e.WriteInt16(part.errCode)
			e.WriteInt64(part.hwm)
			if part.errCode == protocol.ErrNone {
				e.WriteInt64(part.hwm) // No transactional batches are accepted.
				e.WriteInt64(part.start)
			} else {
				e.WriteInt64(-1)
				e.WriteInt64(-1)
			}
			e.WriteByte(0) // aborted transactions
			e.WriteInt32(-1)
			if len(part.records) == 0 {
				e.WriteByte(0)
			} else {
				e.WriteCompactBytes(part.records)
			}
			e.WriteTagBuffer()
		}
		e.WriteTagBuffer()
	}
	e.WriteTagBuffer()
	return e.Bytes(), nil
}

// BuildBodyV0 returns complete batches within strict per-partition byte limits.
func BuildBodyV0(req *protocol.FetchRequest, m *metadata.Manager, localBrokerID ...int32) []byte {
	body, _ := BuildBodyV0Context(context.Background(), req, m, localBrokerID...)
	return body
}

func BuildBodyV0Context(ctx context.Context, req *protocol.FetchRequest, m *metadata.Manager, localBrokerID ...int32) ([]byte, error) {
	brokerID := int32(1)
	if len(localBrokerID) > 0 {
		brokerID = localBrokerID[0]
	}
	topics, err := fetchData(ctx, req, m, brokerID, false)
	if err != nil {
		return nil, err
	}
	e := protocol.NewEncoder()
	e.WriteArrayLen(len(topics))
	for _, topic := range topics {
		e.WriteString(topic.name)
		e.WriteArrayLen(len(topic.parts))
		for _, part := range topic.parts {
			e.WriteInt32(part.index)
			e.WriteInt16(part.errCode)
			e.WriteInt64(part.hwm)
			e.WriteBytes(part.records)
		}
	}
	return e.Bytes(), nil
}
