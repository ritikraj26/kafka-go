package describetopics

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a DescribeTopicPartitions response for unknown topics.
// This stage treats all requested topics as unknown and returns error code 3.
func BuildBody(req *protocol.DescribeTopicPartitionsRequest) []byte {
	encoder := protocol.NewEncoder()

	// throttle_time_ms: 0
	encoder.WriteInt32(0)

	// topics array (COMPACT_ARRAY: length+1)
	topicCount := len(req.TopicNames)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic as unknown
	for _, topicName := range req.TopicNames {
		// error_code: 3 (UNKNOWN_TOPIC_OR_PARTITION)
		encoder.WriteInt16(protocol.ErrUnknownTopicOrPartition)

		// topic_name: echo back from request (COMPACT_STRING)
		encoder.WriteCompactString(topicName)

		// topic_id: 00000000-0000-0000-0000-000000000000 (16 zero bytes)
		var zeroUUID [16]byte
		encoder.WriteUUID(zeroUUID)

		// is_internal: false
		encoder.WriteByte(0x00)

		// partitions array: empty (COMPACT_ARRAY with 0 elements = 0x01)
		encoder.WriteByte(0x01)

		// topic_authorized_operations: 0
		encoder.WriteInt32(0)

		// TAG_BUFFER for topic entry
		encoder.WriteTagBuffer()
	}

	// next_cursor: -1 (null for NULLABLE_INT8 is 0xff)
	encoder.WriteByte(0xff)

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}
