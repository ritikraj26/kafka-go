package offsetfetch

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds an OffsetFetch v1 response.
func BuildBody(req *protocol.OffsetFetchRequest, coord *coordinator.Coordinator) []byte {
	enc := protocol.NewEncoder()
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)                    // topic name
		enc.WriteArrayLen(len(t.Partitions))       // partitions array
		for _, pIdx := range t.Partitions {
			enc.WriteInt32(pIdx) // partition index
			offset, errCode := coord.FetchOffset(req.GroupID, t.Name, pIdx)
			enc.WriteInt64(offset)       // committed_offset
			enc.WriteString("")          // metadata (empty)
			enc.WriteInt16(errCode)      // error_code
		}
	}
	return enc.Bytes()
}
