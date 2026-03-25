package fetch

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a Fetch v16 response
func BuildBody(req *protocol.FetchRequest, metaMgr *metadata.Manager) []byte {
	encoder := protocol.NewEncoder()

	// throttle_time_ms (INT32): 0
	encoder.WriteInt32(0)

	// error_code (INT16): 0 (NO_ERROR)
	encoder.WriteInt16(protocol.ErrNone)

	// session_id (INT32): echo back from request
	encoder.WriteInt32(req.SessionID)

	// responses (COMPACT_ARRAY)
	topicCount := len(req.Topics)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic response
	for _, reqTopic := range req.Topics {
		// topic_id (UUID - 16 bytes)
		encoder.WriteUUID(reqTopic.TopicID)

		// Check if topic exists
		topic := metaMgr.GetTopicByID(reqTopic.TopicID)

		if topic == nil {
			// Unknown topic - return error in partition response
			// partitions (COMPACT_ARRAY) - 1 partition with error
			encoder.WriteUnsignedVarint(2) // 1 partition + 1

			// partition_index (INT32): 0
			encoder.WriteInt32(0)

			// error_code (INT16): 100 (UNKNOWN_TOPIC_ID)
			encoder.WriteInt16(protocol.ErrUnknownTopicID)

			// high_watermark (INT64): -1
			encoder.WriteInt64(-1)

			// last_stable_offset (INT64): -1
			encoder.WriteInt64(-1)

			// log_start_offset (INT64): -1
			encoder.WriteInt64(-1)

			// aborted_transactions (COMPACT_ARRAY): null
			encoder.WriteByte(0x00)

			// preferred_read_replica (INT32): -1
			encoder.WriteInt32(-1)

			// records (COMPACT_BYTES): null
			encoder.WriteByte(0x00)

			// TAG_BUFFER for partition
			encoder.WriteTagBuffer()
		} else {
			// Known topic - read records from disk
			// partitions (COMPACT_ARRAY) - 1 partition
			encoder.WriteUnsignedVarint(2) // 1 partition + 1

			// partition_index (INT32): 0
			encoder.WriteInt32(0)

			// error_code (INT16): 0 (NO_ERROR)
			encoder.WriteInt16(protocol.ErrNone)

			// Read records from the log file
			var recordBatches []byte
			if len(topic.Partitions) > 0 {
				partition := &topic.Partitions[0]
				var err error
				recordBatches, err = metadata.ReadPartitionLog(partition)
				if err != nil {
					// If we can't read the log, treat as empty
					recordBatches = []byte{}
				}
			}

			// Calculate high_watermark (number of messages)
			// For simplicity, if we have data, high_watermark is 1, otherwise 0
			highWatermark := int64(0)
			if len(recordBatches) > 0 {
				highWatermark = 1
			}

			// high_watermark (INT64)
			encoder.WriteInt64(highWatermark)

			// last_stable_offset (INT64)
			encoder.WriteInt64(highWatermark)

			// log_start_offset (INT64): 0
			encoder.WriteInt64(0)

			// aborted_transactions (COMPACT_ARRAY): null
			encoder.WriteByte(0x00)

			// preferred_read_replica (INT32): -1
			encoder.WriteInt32(-1)

			// records (COMPACT_BYTES): raw record batch data from log file
			if len(recordBatches) == 0 {
				// null (no records)
				encoder.WriteByte(0x00)
			} else {
				// Write the record batches as COMPACT_BYTES
				encoder.WriteCompactBytes(recordBatches)
			}

			// TAG_BUFFER for partition
			encoder.WriteTagBuffer()
		}

		// TAG_BUFFER for topic
		encoder.WriteTagBuffer()
	}

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}
