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

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1<<31 - 1
	}
	bytesWritten := 0

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
			// Known topic — respond per requested partition
			encoder.WriteUnsignedVarint(uint64(len(reqTopic.Partitions) + 1))

			for _, reqPart := range reqTopic.Partitions {
				// Find the partition in metadata
				var partition *metadata.Partition
				for i := range topic.Partitions {
					if topic.Partitions[i].Index == reqPart.PartitionIndex {
						partition = &topic.Partitions[i]
						break
					}
				}

				// partition_index (INT32)
				encoder.WriteInt32(reqPart.PartitionIndex)

				if partition == nil {
					// Partition not found
					encoder.WriteInt16(protocol.ErrUnknownTopicOrPartition)
					encoder.WriteInt64(-1)  // high_watermark
					encoder.WriteInt64(-1)  // last_stable_offset
					encoder.WriteInt64(-1)  // log_start_offset
					encoder.WriteByte(0x00) // aborted_transactions null
					encoder.WriteInt32(-1)  // preferred_read_replica
					encoder.WriteByte(0x00) // records null
					encoder.WriteTagBuffer()
					continue
				}

				highWatermark := partition.NextOffset

				encoder.WriteInt16(protocol.ErrNone)
				encoder.WriteInt64(highWatermark) // high_watermark
				encoder.WriteInt64(highWatermark) // last_stable_offset
				encoder.WriteInt64(0)             // log_start_offset
				encoder.WriteByte(0x00)           // aborted_transactions null
				encoder.WriteInt32(-1)            // preferred_read_replica

				// Seek to the requested offset via index, then read from that byte position
				var recordBatches []byte
				if reqPart.FetchOffset < highWatermark && bytesWritten < maxBytes {
					bytePos, _ := partition.SeekToOffset(reqPart.FetchOffset)
					recordBatches, _ = metadata.ReadPartitionLogFrom(partition, bytePos)
					remaining := maxBytes - bytesWritten
					if len(recordBatches) > remaining {
						recordBatches = recordBatches[:remaining]
					}
					bytesWritten += len(recordBatches)
				}

				if len(recordBatches) == 0 {
					encoder.WriteByte(0x00) // null records
				} else {
					encoder.WriteCompactBytes(recordBatches)
				}

				encoder.WriteTagBuffer()
			}
		}

		// TAG_BUFFER for topic
		encoder.WriteTagBuffer()
	}

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}
