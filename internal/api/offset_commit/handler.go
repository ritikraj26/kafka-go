package offsetcommit

import (
	"context"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds an OffsetCommit v2 response.
func BuildBody(req *protocol.OffsetCommitRequest, coord *coordinator.Coordinator) []byte {
	return BuildBodyContext(context.Background(), req, coord)
}

func BuildBodyContext(ctx context.Context, req *protocol.OffsetCommitRequest, coord *coordinator.Coordinator, encoders ...*protocol.Encoder) []byte {
	enc := protocol.ResponseEncoder(encoders)
	enc.WriteArrayLen(len(req.Topics)) // topics array
	for _, t := range req.Topics {
		enc.WriteString(t.Name)              // topic name
		enc.WriteArrayLen(len(t.Partitions)) // partitions array
		for _, p := range t.Partitions {
			if enc.Err() != nil {
				return enc.Bytes()
			}
			enc.WriteInt32(p.Index) // partition index
			errCode := coord.CommitOffsetWithMetadataContext(ctx, req.GroupID, req.GenerationID, req.MemberID,
				t.Name, p.Index, p.CommittedOffset, p.Metadata)
			enc.WriteInt16(errCode) // error_code
		}
	}
	return enc.Bytes()
}
