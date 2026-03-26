package produce

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
	"github.com/codecrafters-io/kafka-starter-go/internal/schema"
	"github.com/codecrafters-io/kafka-starter-go/internal/storage"
)

// BuildBody builds a Produce v11 response
func BuildBody(req *protocol.ProduceRequest, metaMgr *metadata.Manager) []byte {
	encoder := protocol.NewEncoder()

	// responses (COMPACT_ARRAY) - comes FIRST in Produce v11
	topicCount := len(req.Topics)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic response
	for _, reqTopic := range req.Topics {
		// name (COMPACT_STRING) - echo back the topic name
		encoder.WriteCompactString(reqTopic.Name)

		// partitions (COMPACT_ARRAY)
		partitionCount := len(reqTopic.Partitions)
		encoder.WriteUnsignedVarint(uint64(partitionCount + 1))

		// Look up the topic in metadata
		topic := metaMgr.GetTopic(reqTopic.Name)

		// Write each partition response
		for _, reqPartition := range reqTopic.Partitions {
			// index (INT32) - echo back the partition index
			encoder.WriteInt32(reqPartition.Index)

			// Validate topic and partition exist
			var errorCode int16 = protocol.ErrUnknownTopicOrPartition
			var baseOffset int64 = -1
			var logAppendTime int64 = -1
			var logStartOffset int64 = -1

			if topic != nil {
				// Topic exists, check if partition exists
				var matchedPartition *metadata.Partition
				for i := range topic.Partitions {
					if topic.Partitions[i].Index == reqPartition.Index {
						matchedPartition = &topic.Partitions[i]
						break
					}
				}

				if matchedPartition != nil {
					if len(reqPartition.Records) > 0 {
						// --- AI Schema Validation ---
						// Extract raw message values from the RecordBatch and validate
						// each one against the topic's inferred schema.
						// If no schema exists yet, infer it from the first value.
						// Any value that fails validation is routed to {topic}.dlq instead.
						targetRecords, dlqRecords := validateRecords(
							reqPartition.Records,
							reqTopic.Name,
							metaMgr,
						)

						// Route DLQ records (fire-and-forget, non-fatal)
						if len(dlqRecords) > 0 {
							routeToDLQ(reqTopic.Name, dlqRecords, metaMgr)
						}

						// If all records were invalid, still return success to the
						// producer — DLQ routing is transparent.
						recordsToWrite := targetRecords
						if len(recordsToWrite) == 0 {
							recordsToWrite = reqPartition.Records // fallback: write original
						}

						// AppendRecords locks the partition, writes to disk, advances NextOffset
						offset, err := matchedPartition.AppendRecords(recordsToWrite, matchedPartition.LogDir)
						if err != nil {
							logger.L.Error("failed to write records to disk", "topic", reqTopic.Name, "partition", reqPartition.Index, "err", err)
							errorCode = protocol.ErrUnknownTopicOrPartition
						} else {
							errorCode = protocol.ErrNone
							baseOffset = offset
							logAppendTime = -1
							logStartOffset = 0
						}
					} else {
						// No records — still valid, return current offset
						errorCode = protocol.ErrNone
						baseOffset = matchedPartition.NextOffset
						logAppendTime = -1
						logStartOffset = 0
					}
				}
			}

			// error_code (INT16)
			encoder.WriteInt16(errorCode)

			// base_offset (INT64)
			encoder.WriteInt64(baseOffset)

			// log_append_time_ms (INT64)
			encoder.WriteInt64(logAppendTime)

			// log_start_offset (INT64)
			encoder.WriteInt64(logStartOffset)

			// record_errors (COMPACT_ARRAY): null
			encoder.WriteByte(0x00)

			// error_message (COMPACT_NULLABLE_STRING): null
			encoder.WriteByte(0x00)

			// TAG_BUFFER for partition
			encoder.WriteTagBuffer()
		}

		// TAG_BUFFER for topic
		encoder.WriteTagBuffer()
	}

	// throttle_time_ms (INT32): 0 - comes AFTER responses in Produce v11
	encoder.WriteInt32(0)

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}

// validateRecords inspects each message value in the RecordBatch:
//   - If no schema exists for the topic yet, calls Ollama to infer one from the
//     first value, then registers it. The full original batch is returned as-is.
//   - If a schema exists, values that pass go to targetRecords (original batch),
//     values that fail go to dlqRecords (raw value bytes for DLQ logging).
//
// Returns (originalBatch, nil) when all messages are valid or schema is new.
// Returns (originalBatch, failedValues) when some messages violate the schema.
func validateRecords(records []byte, topic string, metaMgr *metadata.Manager) ([]byte, [][]byte) {
	reg := metaMgr.Schemas
	values := storage.ParseRecordValues(records)
	if len(values) == 0 {
		return records, nil
	}

	// No schema yet — infer from first value, register, allow the whole batch through.
	if reg.Get(topic) == nil {
		s, err := schema.InferWithOllama(values[0])
		if err != nil || s == nil {
			// Ollama unavailable — fall back to local inference
			s, err = schema.InferFromJSON(values[0])
		}
		if s != nil && err == nil {
			reg.Register(topic, s)
			logger.L.Info("schema inferred for topic", "topic", topic, "fields", s.Fields)
		} else {
			logger.L.Warn("could not infer schema for topic", "topic", topic)
		}
		return records, nil
	}

	// Schema exists — validate every value.
	var failed [][]byte
	for _, v := range values {
		if err := reg.Validate(topic, v); err != nil {
			logger.L.Warn("schema violation — routing to DLQ", "topic", topic, "err", err)
			failed = append(failed, v)
		}
	}
	return records, failed
}

// routeToDLQ appends raw failed message values to the {topic}.dlq partition.
// Each value is wrapped in a minimal raw byte slice for the DLQ log entry.
func routeToDLQ(sourceTopic string, values [][]byte, metaMgr *metadata.Manager) {
	dlq := metaMgr.GetOrCreateDLQTopic(sourceTopic)
	if len(dlq.Partitions) == 0 {
		return
	}
	p := &dlq.Partitions[0]
	for _, v := range values {
		if _, err := p.AppendRecords(v, p.LogDir); err != nil {
			logger.L.Error("failed to write to DLQ", "topic", sourceTopic+".dlq", "err", err)
		}
	}
}
