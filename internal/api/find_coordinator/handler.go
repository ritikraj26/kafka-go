package findcoordinator

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/broker"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a FindCoordinator v0 response.
// Returns the local broker as the coordinator for every group.
func BuildBody(req *protocol.FindCoordinatorRequest, reg *broker.Registry) []byte {
	enc := protocol.NewEncoder()
	enc.WriteInt16(protocol.ErrNone) // error_code

	if reg != nil {
		local := reg.Local()
		if local != nil {
			enc.WriteInt32(local.ID)
			enc.WriteString(local.Host)
			enc.WriteInt32(local.Port)
			return enc.Bytes()
		}
	}
	// Fallback to hardcoded defaults
	enc.WriteInt32(1)            // coordinator node_id
	enc.WriteString("localhost") // coordinator host
	enc.WriteInt32(9092)         // coordinator port
	return enc.Bytes()
}
