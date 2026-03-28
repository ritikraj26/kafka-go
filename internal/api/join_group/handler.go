package joingroup

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a JoinGroup v0 response. May block until the join round completes.
func BuildBody(req *protocol.JoinGroupRequest, coord *coordinator.Coordinator) []byte {
	// Convert protocol types
	protocols := make([]coordinator.MemberProtocol, len(req.Protocols))
	for i, p := range req.Protocols {
		protocols[i] = coordinator.MemberProtocol{Name: p.Name, Metadata: p.Metadata}
	}

	result := coord.JoinGroup(req.GroupID, req.MemberID, req.SessionTimeoutMs,
		req.ProtocolType, protocols)

	enc := protocol.NewEncoder()
	enc.WriteInt16(result.ErrorCode)          // error_code
	enc.WriteInt32(result.GenerationID)       // generation_id
	enc.WriteString(result.ProtocolName)      // group_protocol
	enc.WriteString(result.LeaderID)          // leader_id
	enc.WriteString(result.MemberID)          // member_id
	enc.WriteArrayLen(len(result.Members))    // members array
	for _, m := range result.Members {
		enc.WriteString(m.MemberID)           // member_id
		enc.WriteBytes(m.Metadata)            // member_metadata
	}
	return enc.Bytes()
}
