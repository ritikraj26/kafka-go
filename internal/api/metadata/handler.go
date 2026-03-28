package apimetadata

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

const (
	brokerID   int32 = 1
	brokerHost       = "localhost"
	brokerPort int32 = 9092
	clusterID        = "kafka-go-cluster"
)

// BuildBody builds a Metadata v12 response body.
func BuildBody(req *protocol.MetadataRequest, metaMgr *metadata.Manager) []byte {
	enc := protocol.NewEncoder()

	enc.WriteInt32(0)          // throttle_time_ms
	enc.WriteUnsignedVarint(2) // brokers compact array (1 element)
	enc.WriteInt32(brokerID)
	enc.WriteCompactString(brokerHost)
	enc.WriteInt32(brokerPort)
	enc.WriteCompactString("") // rack
	enc.WriteTagBuffer()

	enc.WriteCompactString(clusterID) // cluster_id
	enc.WriteInt32(brokerID)          // controller_id

	topics := selectTopics(req, metaMgr)
	enc.WriteUnsignedVarint(uint64(len(topics) + 1))

	for _, topic := range topics {
		enc.WriteInt16(protocol.ErrNone)
		enc.WriteCompactString(topic.Name)

		var uuidBytes [16]byte
		copy(uuidBytes[:], topic.ID[:])
		enc.WriteUUID(uuidBytes)

		if topic.IsInternal {
			enc.WriteByte(0x01)
		} else {
			enc.WriteByte(0x00)
		}

		enc.WriteUnsignedVarint(uint64(len(topic.Partitions) + 1))
		for i := range topic.Partitions {
			part := &topic.Partitions[i]
			enc.WriteInt16(protocol.ErrNone)
			enc.WriteInt32(part.Index)
			enc.WriteInt32(part.LeaderID)
			enc.WriteInt32(part.LeaderEpoch)

			enc.WriteUnsignedVarint(uint64(len(part.ReplicaNodes) + 1))
			for _, r := range part.ReplicaNodes {
				enc.WriteInt32(r)
			}
			enc.WriteUnsignedVarint(uint64(len(part.ISRNodes) + 1))
			for _, isr := range part.ISRNodes {
				enc.WriteInt32(isr)
			}

			enc.WriteByte(0x01) // eligible_leader_replicas (empty array)
			enc.WriteByte(0x00) // last_known_elr (null)
			enc.WriteByte(0x00) // offline_replicas (null)
			enc.WriteTagBuffer()
		}

		enc.WriteInt32(0x00000df8) // topic_authorized_operations
		enc.WriteTagBuffer()
	}

	enc.WriteTagBuffer()
	return enc.Bytes()
}

func selectTopics(req *protocol.MetadataRequest, metaMgr *metadata.Manager) []*metadata.Topic {
	if req.TopicNames == nil {
		names := metaMgr.ListTopics()
		topics := make([]*metadata.Topic, 0, len(names))
		for _, name := range names {
			if t := metaMgr.GetTopic(name); t != nil {
				topics = append(topics, t)
			}
		}
		return topics
	}
	topics := make([]*metadata.Topic, 0, len(req.TopicNames))
	for _, name := range req.TopicNames {
		if t := metaMgr.GetTopic(name); t != nil {
			topics = append(topics, t)
		}
	}
	return topics
}
