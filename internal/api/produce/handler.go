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
			if topic != nil {
				for i := range topic.Partitions {
					p := &topic.Partitions[i]
					if p.Index != reqPartition.Index {
						continue
					}
					if p.LeaderID != localBrokerID {
						errorCode = protocol.ErrNotLeaderOrFollower
					} else {
						baseOffset, logAppendTime, errorCode = appendPartition(req, reqPartition.Records, reqTopic.Name, metaMgr, p, localBrokerID)
						if baseOffset >= 0 {
							logStartOffset = 0
						}
					}
					break
				}
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
	return encoder.Bytes()
}

func appendPartition(req *protocol.ProduceRequest, records []byte, topic string, metaMgr *metadata.Manager, p *metadata.Partition, brokerID int32) (int64, int64, int16) {
	if len(records) == 0 {
		return p.GetLogEndOffset(), -1, protocol.ErrNone
	}
	batches, err := storage.ParseRecordBatches(records)
	if err != nil {
		logger.L.Warn("rejecting invalid record set", "topic", topic, "partition", p.Index, "err", err)
		return -1, -1, recordErrorCode(err)
	}
	var recordCount int64
	for _, batch := range batches {
		recordCount += int64(batch.RecordsCount)
	}

	failed, allFailed := validateRecords(batches, topic, metaMgr)
	if len(failed) > 0 {
		if err := routeToDLQ(topic, failed, metaMgr); err != nil {
			logger.L.Error("failed to write to DLQ", "topic", topic, "err", err)
			return -1, -1, protocol.ErrUnknownServerError
		}
	}
	if allFailed {
		return p.GetLogEndOffset(), -1, protocol.ErrNone
	}
	if len(failed) > 0 {
		logger.L.Warn("partial schema violation: writing full batch; failed records also in DLQ", "topic", topic, "failed", len(failed))
	}

	offset, err := p.AppendRecords(records, p.LogDirForBroker(brokerID))
	if err != nil {
		logger.L.Error("failed to append records", "topic", topic, "partition", p.Index, "err", err)
		return -1, -1, recordErrorCode(err)
	}
	appendTime := time.Now().UnixMilli()
	if req.Acks == -1 && !p.WaitForHighWatermark(offset+recordCount, time.Duration(req.TimeoutMs)*time.Millisecond) {
		return offset, appendTime, protocol.ErrRequestTimedOut
	}
	return offset, appendTime, protocol.ErrNone
}

func recordErrorCode(err error) int16 {
	switch {
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

func routeToDLQ(sourceTopic string, values [][]byte, metaMgr *metadata.Manager) error {
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
	_, err = p.AppendRecords(batch, p.LogDirForBroker(p.LeaderID))
	return err
}
