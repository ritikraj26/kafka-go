package syncgroup

import (
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a SyncGroup v0 response. May block for followers.
func BuildBody(req *protocol.SyncGroupRequest, coord *coordinator.Coordinator) []byte {
	// Convert assignments
	assignments := make([]coordinator.SyncGroupAssignment, len(req.Assignments))
	for i, a := range req.Assignments {
		assignments[i] = coordinator.SyncGroupAssignment{
			MemberID:   a.MemberID,
			Assignment: a.Assignment,
		}
	}

	data, errCode := coord.SyncGroup(req.GroupID, req.GenerationID, req.MemberID, assignments)

	enc := protocol.NewEncoder()
	enc.WriteInt16(errCode)   // error_code
	enc.WriteBytes(data)      // member_assignment
	return enc.Bytes()
}
