package produce

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
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
						// AppendRecords locks the partition, writes to disk, advances NextOffset
						offset, err := matchedPartition.AppendRecords(reqPartition.Records, matchedPartition.LogDir)
						if err != nil {
							fmt.Printf("Error writing records to disk: %v\n", err)
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
