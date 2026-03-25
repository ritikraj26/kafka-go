package produce

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a Produce v11 response
// For now, hardcode error responses for all topics/partitions
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

		// Write each partition response
		for _, reqPartition := range reqTopic.Partitions {
			// index (INT32) - echo back the partition index
			encoder.WriteInt32(reqPartition.Index)

			// error_code (INT16): 3 (UNKNOWN_TOPIC_OR_PARTITION)
			encoder.WriteInt16(protocol.ErrUnknownTopicOrPartition)

			// base_offset (INT64): -1
			encoder.WriteInt64(-1)

			// log_append_time_ms (INT64): -1
			encoder.WriteInt64(-1)

			// log_start_offset (INT64): -1
			encoder.WriteInt64(-1)

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
