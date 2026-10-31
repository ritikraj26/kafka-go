package syncgroup

import (
	"context"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a SyncGroup v0 response. May block for followers.
func BuildBody(req *protocol.SyncGroupRequest, coord *coordinator.Coordinator) []byte {
	return BuildBodyContext(context.Background(), req, coord)
}

func BuildBodyContext(ctx context.Context, req *protocol.SyncGroupRequest, coord *coordinator.Coordinator) []byte {
	// Convert assignments
	assignments := make([]coordinator.SyncGroupAssignment, len(req.Assignments))
	for i, a := range req.Assignments {
		assignments[i] = coordinator.SyncGroupAssignment{
			MemberID:   a.MemberID,
			Assignment: a.Assignment,
		}
	}

	data, errCode := coord.SyncGroupContext(ctx, req.GroupID, req.GenerationID, req.MemberID, assignments)

	enc := protocol.NewEncoder()
	enc.WriteInt16(errCode) // error_code
	enc.WriteBytes(data)    // member_assignment
	return enc.Bytes()
}
