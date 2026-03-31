package describetopics

import (
	"sort"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a DescribeTopicPartitions response
func BuildBody(req *protocol.DescribeTopicPartitionsRequest, metaMgr *metadata.Manager) []byte {
	encoder := protocol.NewEncoder()

	// Sort topic names alphabetically (required by Kafka protocol)
	sortedTopics := make([]string, len(req.TopicNames))
	copy(sortedTopics, req.TopicNames)
	sort.Strings(sortedTopics)

	// throttle_time_ms: 0
	encoder.WriteInt32(0)

	// topics array (COMPACT_ARRAY: length+1)
	topicCount := len(sortedTopics)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic
	for _, topicName := range sortedTopics {
		topic := metaMgr.GetTopic(topicName)

		if topic == nil {
			// Unknown topic
			writeUnknownTopic(encoder, topicName)
		} else {
			// Known topic with actual metadata
			writeKnownTopic(encoder, topic)
		}
	}

	// next_cursor: -1 (null for NULLABLE_INT8 is 0xff)
	encoder.WriteByte(0xff)

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}

// writeUnknownTopic writes response for an unknown topic
func writeUnknownTopic(encoder *protocol.Encoder, topicName string) {
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

// writeKnownTopic writes response for a known topic with real metadata
func writeKnownTopic(encoder *protocol.Encoder, topic *metadata.Topic) {
	// error_code: 0 (NO_ERROR)
	encoder.WriteInt16(protocol.ErrNone)

	// topic_name
	encoder.WriteCompactString(topic.Name)

	// topic_id: actual UUID
	topicIDBytes := topic.ID[:]
	var uuidArray [16]byte
	copy(uuidArray[:], topicIDBytes)
	encoder.WriteUUID(uuidArray)

	// is_internal
	if topic.IsInternal {
		encoder.WriteByte(0x01)
	} else {
		encoder.WriteByte(0x00)
	}

	// partitions array (COMPACT_ARRAY)
	encoder.WriteUnsignedVarint(uint64(len(topic.Partitions) + 1))

	for i := range topic.Partitions {
		partition := &topic.Partitions[i]
		// error_code for partition: 0 (NO_ERROR)
		encoder.WriteInt16(protocol.ErrNone)

		// partition_index
		encoder.WriteInt32(partition.Index)

		// leader_id
		encoder.WriteInt32(partition.LeaderID)

		// leader_epoch
		encoder.WriteInt32(partition.LeaderEpoch)

		// replica_nodes (COMPACT_ARRAY)
		encoder.WriteUnsignedVarint(uint64(len(partition.ReplicaNodes) + 1))
		for _, replica := range partition.ReplicaNodes {
			encoder.WriteInt32(replica)
		}

		// isr_nodes (COMPACT_ARRAY)
		encoder.WriteUnsignedVarint(uint64(len(partition.ISRNodes) + 1))
		for _, isr := range partition.ISRNodes {
			encoder.WriteInt32(isr)
		}

		// eligible_leader_replicas (COMPACT_ARRAY) - empty for this stage
		encoder.WriteByte(0x01) // Empty array (length 0 + 1)

		// last_known_elr (COMPACT_ARRAY) - null
		encoder.WriteByte(0x00) // Null array

		// offline_replicas (COMPACT_ARRAY) - null
		encoder.WriteByte(0x00) // Null array

		// TAG_BUFFER for partition
		encoder.WriteTagBuffer()
	}

	// topic_authorized_operations: 0
	encoder.WriteInt32(0)

	// TAG_BUFFER for topic
	encoder.WriteTagBuffer()
}
