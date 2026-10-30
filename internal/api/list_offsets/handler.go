package listoffsets

import (
	"errors"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a ListOffsets v1 response.
func BuildBody(req *protocol.ListOffsetsRequest, metaMgr *metadata.Manager, localBrokerID ...int32) []byte {
	brokerID := int32(1)
	if len(localBrokerID) > 0 {
		brokerID = localBrokerID[0]
	}
	enc := protocol.NewEncoder()
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)              // topic name
		enc.WriteArrayLen(len(t.Partitions)) // partitions array
		for _, p := range t.Partitions {
			topic := metaMgr.GetTopic(t.Name)
			enc.WriteInt32(p.Index) // partition index
			if topic == nil {
				enc.WriteInt16(protocol.ErrUnknownTopicOrPartition) // error_code
				enc.WriteInt64(-1)                                  // timestamp
				enc.WriteInt64(-1)                                  // offset
				continue
			}
			var partition *metadata.Partition
			for i := range topic.Partitions {
				if topic.Partitions[i].Index == p.Index {
					partition = &topic.Partitions[i]
					break
				}
			}
			if partition == nil {
				enc.WriteInt16(protocol.ErrUnknownTopicOrPartition)
				enc.WriteInt64(-1)
				enc.WriteInt64(-1)
				continue
			}
			start, end, err := partition.OffsetBounds(brokerID, req.ReplicaID >= 0)
			if err != nil {
				code := protocol.ErrUnknownServerError
				if errors.Is(err, metadata.ErrNotLeader) {
					code = protocol.ErrNotLeaderOrFollower
				} else {
					logger.L.Error("list offsets failed", "topic", t.Name, "partition", p.Index, "err", err)
				}
				enc.WriteInt16(code)
				enc.WriteInt64(-1)
				enc.WriteInt64(-1)
				continue
			}
			if p.Timestamp != -2 && p.Timestamp != -1 {
				// Timestamp lookup is not implemented; do not silently return latest.
				enc.WriteInt16(protocol.ErrUnsupportedForMessageFormat)
				enc.WriteInt64(-1)
				enc.WriteInt64(-1)
				continue
			}
			enc.WriteInt16(protocol.ErrNone)
			// -2 = earliest, -1 = latest
			if p.Timestamp == -2 {
				enc.WriteInt64(-1) // timestamp unknown for sentinel queries
				enc.WriteInt64(start)
			} else {
				enc.WriteInt64(-1) // timestamp
				enc.WriteInt64(end)
			}
		}
	}
	return enc.Bytes()
}
