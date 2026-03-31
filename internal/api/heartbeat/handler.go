package heartbeat

import (
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// BuildBody builds a Heartbeat v0 response.
func BuildBody(req *protocol.HeartbeatRequest, coord *coordinator.Coordinator) []byte {
	errCode := coord.Heartbeat(req.GroupID, req.GenerationID, req.MemberID)
	enc := protocol.NewEncoder()
	enc.WriteInt16(errCode) // error_code
	return enc.Bytes()
}
