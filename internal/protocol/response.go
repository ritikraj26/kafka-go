package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type response struct {
	message_size   int32
	correlation_id int32
	body           []byte
	headerVersion  int // 0 or 1
}

// NewResponse builds a response with header v0 (no TAG_BUFFER).
// message_size is computed as 4 (correlation_id) + len(body).
func NewResponse(correlationID int32, body []byte) *response {
	return &response{
		message_size:   int32(4 + len(body)),
		correlation_id: correlationID,
		body:           body,
		headerVersion:  0,
	}
}

// NewResponseV1 builds a response with header v1 (includes TAG_BUFFER after correlation_id).
// message_size is computed as 4 (correlation_id) + 1 (TAG_BUFFER) + len(body).
func NewResponseV1(correlationID int32, body []byte) *response {
	return &response{
		message_size:   int32(4 + 1 + len(body)),
		correlation_id: correlationID,
		body:           body,
		headerVersion:  1,
	}
}

func (r *response) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, r.message_size); err != nil {
		return nil, fmt.Errorf("failed to write message_size: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, r.correlation_id); err != nil {
		return nil, fmt.Errorf("failed to write correlation_id: %w", err)
	}

	// For header v1, add TAG_BUFFER after correlation_id
	if r.headerVersion == 1 {
		buf.WriteByte(0x00)
	}

	if _, err := buf.Write(r.body); err != nil {
		return nil, fmt.Errorf("failed to write body: %w", err)
	}

	return buf.Bytes(), nil
}
