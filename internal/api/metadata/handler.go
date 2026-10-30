package apimetadata

import (
	"sort"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

const clusterID = "kafka-go-cluster"

// BuildBody encodes Metadata v12, including topic-by-ID and missing-topic results.
func BuildBody(req *protocol.MetadataRequest, m *metadata.Manager) []byte {
	e := protocol.NewEncoder()
	e.WriteInt32(0)
	if m.Brokers != nil {
		brokers := m.Brokers.All()
		sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
		e.WriteUnsignedVarint(uint64(len(brokers) + 1))
		for _, b := range brokers {
			e.WriteInt32(b.ID)
			e.WriteCompactString(b.Host)
			e.WriteInt32(b.Port)
			e.WriteByte(0) // nullable rack
			e.WriteTagBuffer()
		}
		e.WriteCompactString(clusterID)
		e.WriteInt32(m.Brokers.LocalID)
	} else {
		e.WriteUnsignedVarint(2)
		e.WriteInt32(1)
		e.WriteCompactString("localhost")
		e.WriteInt32(9092)
		e.WriteByte(0)
		e.WriteTagBuffer()
		e.WriteCompactString(clusterID)
		e.WriteInt32(1)
	}
	requested := req.Topics
	if requested == nil {
		names := req.TopicNames
		if names == nil {
			names = m.ListTopics()
			sort.Strings(names)
		}
		requested = make([]protocol.MetadataTopic, len(names))
		for i, name := range names {
			requested[i].Name = &name
		}
	}
	e.WriteUnsignedVarint(uint64(len(requested) + 1))
	for _, query := range requested {
		var topic *metadata.Topic
		if query.ID != [16]byte{} {
			topic = m.GetTopicByID(query.ID)
			if topic != nil && query.Name != nil && *query.Name != topic.Name {
				topic = nil
			}
		} else if query.Name != nil {
			topic = m.GetTopic(*query.Name)
		}
		if topic == nil {
			code := protocol.ErrUnknownTopicOrPartition
			if query.ID != [16]byte{} {
				code = protocol.ErrUnknownTopicID
			}
			e.WriteInt16(code)
			if query.Name == nil {
				e.WriteByte(0)
			} else {
				e.WriteCompactString(*query.Name)
			}
			e.WriteUUID(query.ID)
			e.WriteByte(0)
			e.WriteUnsignedVarint(1)
		} else {
			e.WriteInt16(0)
			e.WriteCompactString(topic.Name)
			e.WriteUUID(topic.ID)
			if topic.IsInternal {
				e.WriteByte(1)
			} else {
				e.WriteByte(0)
			}
			e.WriteUnsignedVarint(uint64(len(topic.Partitions) + 1))
			for i := range topic.Partitions {
				p := topic.Partitions[i].Snapshot()
				e.WriteInt16(0)
				e.WriteInt32(p.Index)
				e.WriteInt32(p.LeaderID)
				e.WriteInt32(p.LeaderEpoch)
				writeNodes(e, p.Replicas)
				writeNodes(e, p.ISR)
				writeNodes(e, p.Offline)
				e.WriteTagBuffer()
			}
		}
		e.WriteInt32(-2147483648) // authorized operations were not calculated
		e.WriteTagBuffer()
	}
	e.WriteTagBuffer()
	return e.Bytes()
}

func writeNodes(e *protocol.Encoder, nodes []int32) {
	e.WriteUnsignedVarint(uint64(len(nodes) + 1))
	for _, id := range nodes {
		e.WriteInt32(id)
	}
}
