package produce

import (
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
	"github.com/codecrafters-io/kafka-starter-go/internal/storage"
)

// BuildBody builds a Produce v11 response
func BuildBody(req *protocol.ProduceRequest, metaMgr *metadata.Manager, localBrokerID int32) []byte {
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

				if matchedPartition != nil && matchedPartition.LeaderID != localBrokerID {
					// This broker is not the leader for this partition
					errorCode = protocol.ErrNotLeaderOrFollower
				} else if matchedPartition != nil {
					if len(reqPartition.Records) > 0 {
						// --- AI Schema Validation ---
						// Extract raw message values from the RecordBatch and validate
						// each one against the topic's inferred schema.
						// If no schema exists yet, infer it from the first value.
						// Any value that fails validation is routed to {topic}.dlq instead.
						dlqRecords, allFailed := validateRecords(
							reqPartition.Records,
							reqTopic.Name,
							metaMgr,
						)

						// Route DLQ records (fire-and-forget, non-fatal)
						if len(dlqRecords) > 0 {
							routeToDLQ(reqTopic.Name, dlqRecords, metaMgr)
						}

						if allFailed {
							// All records were invalid — they've been routed to DLQ.
							// Return success to the producer (DLQ routing is transparent)
							// but do NOT write invalid data to the main topic.
							errorCode = protocol.ErrNone
							baseOffset = matchedPartition.NextOffset
							logAppendTime = -1
							logStartOffset = 0
						} else {
							if len(dlqRecords) > 0 {
								// Some records failed — individual record filtering from a
								// binary RecordBatch is not yet supported, so the full batch
								// is written. Failed values are also in the DLQ.
								logger.L.Warn("partial schema violation — writing full batch; failed records also in DLQ",
									"topic", reqTopic.Name, "failed", len(dlqRecords))
							}

							// AppendRecords locks the partition, writes to disk, advances NextOffset
							offset, err := matchedPartition.AppendRecords(reqPartition.Records, matchedPartition.LogDir)
							if err != nil {
								logger.L.Error("failed to write records to disk", "topic", reqTopic.Name, "partition", reqPartition.Index, "err", err)
								errorCode = protocol.ErrUnknownTopicOrPartition
							} else {
								errorCode = protocol.ErrNone
								baseOffset = offset
								logAppendTime = time.Now().UnixMilli()
								logStartOffset = 0

								// acks=-1: wait for all ISR replicas to catch up (HW >= baseOffset+1)
								if req.Acks == -1 {
									waitForHW(matchedPartition, baseOffset+1, time.Duration(req.TimeoutMs)*time.Millisecond)
								}
							}
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
//     first value, then registers it. All records are allowed through.
//   - If a schema exists, values that fail are collected for DLQ routing.
//
// Returns (nil, false) when all messages are valid or schema is new.
// Returns (failedValues, true) when every message violates the schema.
// Returns (failedValues, false) when only some messages violate the schema.
func validateRecords(records []byte, topic string, metaMgr *metadata.Manager) ([][]byte, bool) {
	reg := metaMgr.Schemas
	values := storage.ParseRecordValues(records)
	if len(values) == 0 {
		return nil, false
	}

	// No schema yet — kick off async inference and allow the whole batch through.
	if reg.Get(topic) == nil {
		reg.InferAsync(topic, values[0])
		return nil, false
	}

	// Schema exists — validate every value.
	var failed [][]byte
	for _, v := range values {
		if err := reg.Validate(topic, v); err != nil {
			logger.L.Warn("schema violation — routing to DLQ", "topic", topic, "err", err)
			failed = append(failed, v)
		}
	}
	return failed, len(failed) == len(values)
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

// waitForHW uses sync.Cond-based waiting for efficient HW advancement.
// Blocks until the partition's HighWatermark >= targetHW or timeout expires.
func waitForHW(p *metadata.Partition, targetHW int64, timeout time.Duration) {
	p.WaitForHighWatermark(targetHW, timeout)
}
