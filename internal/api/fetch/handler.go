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
			// Known topic with no messages
			// partitions (COMPACT_ARRAY) - 1 partition
			encoder.WriteUnsignedVarint(2) // 1 partition + 1

			// partition_index (INT32): 0
			encoder.WriteInt32(0)

			// error_code (INT16): 0 (NO_ERROR)
			encoder.WriteInt16(protocol.ErrNone)

			// high_watermark (INT64): 0 (no messages yet)
			encoder.WriteInt64(0)

			// last_stable_offset (INT64): 0
			encoder.WriteInt64(0)

			// log_start_offset (INT64): 0
			encoder.WriteInt64(0)

			// aborted_transactions (COMPACT_ARRAY): null
			encoder.WriteByte(0x00)

			// preferred_read_replica (INT32): -1
			encoder.WriteInt32(-1)

			// records (COMPACT_BYTES): null
			encoder.WriteByte(0x00)

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
