package leavegroup

import (
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a LeaveGroup v0 response.
func BuildBody(req *protocol.LeaveGroupRequest, coord *coordinator.Coordinator) []byte {
	errCode := coord.LeaveGroup(req.GroupID, req.MemberID)
	enc := protocol.NewEncoder()
	enc.WriteInt16(errCode) // error_code
	return enc.Bytes()
}
