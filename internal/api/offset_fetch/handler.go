package offsetfetch

import (
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds an OffsetFetch v1 response.
func BuildBody(req *protocol.OffsetFetchRequest, coord *coordinator.Coordinator, encoders ...*protocol.Encoder) []byte {
	enc := protocol.ResponseEncoder(encoders)
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)              // topic name
		enc.WriteArrayLen(len(t.Partitions)) // partitions array
		for _, pIdx := range t.Partitions {
			if enc.Err() != nil {
				return enc.Bytes()
			}
			enc.WriteInt32(pIdx) // partition index
			offset, metadata, errCode := coord.FetchOffsetWithMetadata(req.GroupID, t.Name, pIdx)
			enc.WriteInt64(offset) // committed_offset
			enc.WriteNullableString(metadata)
			enc.WriteInt16(errCode) // error_code
		}
	}
	return enc.Bytes()
}
