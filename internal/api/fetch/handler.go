package fetch

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a Fetch v16 response
func BuildBody(req *protocol.FetchRequest, metaMgr *metadata.Manager) []byte {
	encoder := protocol.NewEncoder()

	// throttle_time_ms (INT32): 0
	encoder.WriteInt32(0)

	// error_code (INT16): 0 (NO_ERROR)
	encoder.WriteInt16(protocol.ErrNone)

	// session_id (INT32): echo back from request
	encoder.WriteInt32(req.SessionID)

	// responses (COMPACT_ARRAY)
	topicCount := len(req.Topics)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic response (empty array for this stage)
	for range req.Topics {
		// For now, we'll handle this in later stages
		// This stage expects 0 topics, so this loop won't execute
	}

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}
