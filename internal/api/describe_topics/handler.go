package describetopics

import (
	"slices"
	"sort"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func BuildBody(req *protocol.DescribeTopicPartitionsRequest, m *metadata.Manager, encoders ...*protocol.Encoder) []byte {
	names := slices.Clone(req.TopicNames)
	if len(names) == 0 {
		names = m.ListTopics()
	}
	sort.Strings(names)
	names = slices.Compact(names)
	limit := int(req.ResponsePartitionLimit)
	if limit <= 0 {
		limit = 2000
	} // Direct callers use the protocol default.
	type page struct {
		name       string
		topic      *metadata.Topic
		start, end int
	}
	var pages []page
	var next *protocol.TopicCursor
	for _, name := range names {
		if req.Cursor != nil && name < req.Cursor.TopicName {
			continue
		}
		start := 0
		if req.Cursor != nil && name == req.Cursor.TopicName {
			start = int(req.Cursor.PartitionIndex)
		}
		if limit == 0 {
			next = &protocol.TopicCursor{TopicName: name, PartitionIndex: int32(start)}
			break
		}
		topic := m.GetTopic(name)
		if topic == nil {
			pages = append(pages, page{name: name})
			continue
		}
		if start >= len(topic.Partitions) {
			continue
		}
		end := min(len(topic.Partitions), start+limit)
		pages = append(pages, page{name: name, topic: topic, start: start, end: end})
		limit -= end - start
		if end < len(topic.Partitions) {
			next = &protocol.TopicCursor{TopicName: name, PartitionIndex: int32(end)}
			break
		}
	}
	e := protocol.ResponseEncoder(encoders)
	e.WriteInt32(0)
	e.WriteUnsignedVarint(uint64(len(pages) + 1))
	for _, page := range pages {
		if e.Err() != nil {
			return e.Bytes()
		}
		if page.topic == nil {
			e.WriteInt16(protocol.ErrUnknownTopicOrPartition)
			e.WriteCompactString(page.name)
			e.WriteUUID([16]byte{})
			e.WriteByte(0)
			e.WriteUnsignedVarint(1)
		} else {
			e.WriteInt16(0)
			e.WriteCompactString(page.name)
			e.WriteUUID(page.topic.ID)
			if page.topic.IsInternal {
				e.WriteByte(1)
			} else {
				e.WriteByte(0)
			}
			e.WriteUnsignedVarint(uint64(page.end - page.start + 1))
			for i := page.start; i < page.end; i++ {
				p := page.topic.Partitions[i].Snapshot()
				e.WriteInt16(0)
				e.WriteInt32(p.Index)
				e.WriteInt32(p.LeaderID)
				e.WriteInt32(p.LeaderEpoch)
				nodes(e, p.Replicas)
				nodes(e, p.ISR)
				e.WriteByte(0) // eligible leader replicas: null
				e.WriteByte(0) // last known ELR: null
				nodes(e, p.Offline)
				e.WriteTagBuffer()
			}
		}
		e.WriteInt32(-2147483648)
		e.WriteTagBuffer()
	}
	if next == nil {
		e.WriteByte(0xff)
	} else {
		e.WriteByte(1)
		e.WriteCompactString(next.TopicName)
		e.WriteInt32(next.PartitionIndex)
		e.WriteTagBuffer()
	}
	e.WriteTagBuffer()
	return e.Bytes()
}

func nodes(e *protocol.Encoder, ids []int32) {
	e.WriteUnsignedVarint(uint64(len(ids) + 1))
	for _, id := range ids {
		e.WriteInt32(id)
	}
}
