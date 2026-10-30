package produce

import (
	"errors"
	"fmt"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
)

// BuildBody builds a Produce v11 response.
func BuildBody(req *protocol.ProduceRequest, metaMgr *metadata.Manager, localBrokerID int32) []byte {
	body, _ := Process(req, metaMgr, localBrokerID)
	return body
}

// Process also reports whether any partition failed, so acks=0 callers can
// close the connection on error without sending an unsolicited response.
func Process(req *protocol.ProduceRequest, metaMgr *metadata.Manager, localBrokerID int32) ([]byte, bool) {
	requestError := protocol.ErrNone
	if req.Acks != -1 && req.Acks != 0 && req.Acks != 1 {
		requestError = protocol.ErrInvalidRequiredAcks
	} else if req.TimeoutMs < 0 {
		requestError = protocol.ErrInvalidRequest
	}
	if requestError != protocol.ErrNone {
		logger.L.Warn("rejecting invalid produce settings", "acks", req.Acks, "timeout_ms", req.TimeoutMs, "error_code", requestError)
	}
	options := metadata.ProduceOptions{
		BrokerID: localBrokerID, Acks: req.Acks, MinISR: metaMgr.MinInSyncReplicas,
		Deadline: time.Now().Add(time.Duration(req.TimeoutMs) * time.Millisecond),
	}
	failed := requestError != protocol.ErrNone
	encoder := protocol.NewEncoder()
	encoder.WriteUnsignedVarint(uint64(len(req.Topics) + 1))
	for _, reqTopic := range req.Topics {
		encoder.WriteCompactString(reqTopic.Name)
		encoder.WriteUnsignedVarint(uint64(len(reqTopic.Partitions) + 1))
		topic := metaMgr.GetTopic(reqTopic.Name)
		for _, reqPartition := range reqTopic.Partitions {
			encoder.WriteInt32(reqPartition.Index)
			errorCode := protocol.ErrUnknownTopicOrPartition
			baseOffset, logAppendTime, logStartOffset := int64(-1), int64(-1), int64(-1)
			if requestError != protocol.ErrNone {
				errorCode = requestError
			} else if topic != nil {
				for i := range topic.Partitions {
					p := &topic.Partitions[i]
					if p.Index != reqPartition.Index {
						continue
					}
					baseOffset, logAppendTime, errorCode = appendPartition(reqPartition.Records, reqTopic.Name, metaMgr, p, options)
					if baseOffset >= 0 {
						logStartOffset = 0
					}
					break
				}
			}
			if errorCode != protocol.ErrNone {
				failed = true
				logger.L.Warn("produce partition failed", "topic", reqTopic.Name, "partition", reqPartition.Index, "acks", req.Acks, "error_code", errorCode)
			}
			encoder.WriteInt16(errorCode)
			encoder.WriteInt64(baseOffset)
			encoder.WriteInt64(logAppendTime)
			encoder.WriteInt64(logStartOffset)
			encoder.WriteByte(0) // record_errors
			encoder.WriteByte(0) // error_message
			encoder.WriteTagBuffer()
		}
		encoder.WriteTagBuffer()
	}
	encoder.WriteInt32(0) // throttle_time_ms
	encoder.WriteTagBuffer()
	return encoder.Bytes(), failed
}

func appendPartition(records []byte, topic string, metaMgr *metadata.Manager, p *metadata.Partition, options metadata.ProduceOptions) (int64, int64, int16) {
	if err := p.CheckProduce(options); err != nil {
		return -1, -1, recordErrorCode(err)
	}
	if len(records) == 0 {
		offset, err := p.ProduceRecords(nil, options)
		if err != nil {
			return -1, -1, recordErrorCode(err)
		}
		return offset, -1, protocol.ErrNone
	}
	batches, err := storage.ParseRecordBatches(records)
	if err != nil {
		logger.L.Warn("rejecting invalid record set", "topic", topic, "partition", p.Index, "err", err)
		return -1, -1, recordErrorCode(err)
	}
	failed, allFailed := validateRecords(batches, topic, metaMgr)
	if len(failed) > 0 {
		if err := routeToDLQ(topic, failed, metaMgr, options); err != nil {
			logger.L.Error("failed to write to DLQ", "topic", topic, "err", err)
			return -1, -1, recordErrorCode(err)
		}
	}
	if allFailed {
		return p.GetLogEndOffset(), -1, protocol.ErrNone
	}
	if len(failed) > 0 {
		logger.L.Warn("partial schema violation: writing full batch; failed records also in DLQ", "topic", topic, "failed", len(failed))
	}

	offset, err := p.ProduceRecords(records, options)
	if err != nil {
		logger.L.Error("failed to append records", "topic", topic, "partition", p.Index, "err", err)
		return offset, -1, recordErrorCode(err)
	}
	appendTime := time.Now().UnixMilli()
	return offset, appendTime, protocol.ErrNone
}

func recordErrorCode(err error) int16 {
	switch {
	case errors.Is(err, metadata.ErrNotLeader):
		return protocol.ErrNotLeaderOrFollower
	case errors.Is(err, metadata.ErrInvalidRequiredAcks):
		return protocol.ErrInvalidRequiredAcks
	case errors.Is(err, metadata.ErrNotEnoughReplicas):
		return protocol.ErrNotEnoughReplicas
	case errors.Is(err, metadata.ErrNotEnoughReplicasAfterAppend):
		return protocol.ErrNotEnoughReplicasAfterAppend
	case errors.Is(err, metadata.ErrProduceTimeout):
		return protocol.ErrRequestTimedOut
	case errors.Is(err, storage.ErrCorruptRecord):
		return protocol.ErrCorruptMessage
	case errors.Is(err, storage.ErrUnsupportedCompression):
		return protocol.ErrUnsupportedCompressionType
	case errors.Is(err, storage.ErrUnsupportedFormat):
		return protocol.ErrUnsupportedForMessageFormat
	default:
		return protocol.ErrUnknownServerError
	}
}

func validateRecords(batches []storage.RecordBatch, topic string, metaMgr *metadata.Manager) ([][]byte, bool) {
	reg := metaMgr.Schemas
	var values [][]byte
	for _, batch := range batches {
		values = append(values, batch.Values...)
	}
	if len(values) == 0 {
		return nil, false
	}
	if reg.Get(topic) == nil {
		reg.InferAsync(topic, values[0])
		return nil, false
	}
	var failed [][]byte
	for _, value := range values {
		if err := reg.Validate(topic, value); err != nil {
			logger.L.Warn("schema violation: routing to DLQ", "topic", topic, "err", err)
			failed = append(failed, value)
		}
	}
	return failed, len(failed) == len(values)
}

func routeToDLQ(sourceTopic string, values [][]byte, metaMgr *metadata.Manager, options metadata.ProduceOptions) error {
	dlq, err := metaMgr.GetOrCreateDLQTopic(sourceTopic)
	if err != nil {
		return err
	}
	if len(dlq.Partitions) == 0 {
		return fmt.Errorf("DLQ %s has no partitions", dlq.Name)
	}
	records := make([]storage.Record, len(values))
	for i, value := range values {
		records[i].Value = value
	}
	batch, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		return err
	}
	p := &dlq.Partitions[0]
	leader, _, _, _ := p.ReplicaPosition(0)
	options.BrokerID = leader
	_, err = p.ProduceRecords(batch, options)
	return err
}
