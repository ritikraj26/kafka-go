package offsetcommit

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds an OffsetCommit v2 response.
func BuildBody(req *protocol.OffsetCommitRequest, coord *coordinator.Coordinator) []byte {
	enc := protocol.NewEncoder()
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)                  // topic name
		enc.WriteArrayLen(len(t.Partitions))     // partitions array
		for _, p := range t.Partitions {
			enc.WriteInt32(p.Index) // partition index
			errCode := coord.CommitOffset(req.GroupID, req.GenerationID, req.MemberID,
				t.Name, p.Index, p.CommittedOffset)
			enc.WriteInt16(errCode) // error_code
		}
	}
	return enc.Bytes()
}
