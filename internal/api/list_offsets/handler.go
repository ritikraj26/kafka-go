package listoffsets

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a ListOffsets v1 response.
func BuildBody(req *protocol.ListOffsetsRequest, metaMgr *metadata.Manager) []byte {
	enc := protocol.NewEncoder()
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)                 // topic name
		enc.WriteArrayLen(len(t.Partitions))    // partitions array
		for _, p := range t.Partitions {
			topic := metaMgr.GetTopic(t.Name)
			enc.WriteInt32(p.Index) // partition index
			if topic == nil {
				enc.WriteInt16(protocol.ErrUnknownTopicOrPartition) // error_code
				enc.WriteInt64(-1) // timestamp
				enc.WriteInt64(-1) // offset
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
			enc.WriteInt16(protocol.ErrNone) // error_code
			// -2 = earliest, -1 = latest
			if p.Timestamp == -2 {
				enc.WriteInt64(-2) // timestamp
				enc.WriteInt64(0)  // offset (earliest)
			} else {
				enc.WriteInt64(-1)                  // timestamp
				enc.WriteInt64(partition.NextOffset) // offset (latest)
			}
		}
	}
	return enc.Bytes()
}
