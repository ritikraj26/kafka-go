package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Response holds a serializable Kafka response frame.
type Response struct {
	messageSize   int32
	correlationID int32
	body          []byte
	headerVersion int // 0 or 1
}

// NewResponse builds a response with header v0 (no TAG_BUFFER).
// messageSize is computed as 4 (correlationID) + len(body).
func NewResponse(correlationID int32, body []byte) *Response {
	return &Response{
		messageSize:   int32(4 + len(body)),
		correlationID: correlationID,
		body:          body,
		headerVersion: 0,
	}
}

// NewResponseV1 builds a response with header v1 (includes TAG_BUFFER after correlationID).
// messageSize is computed as 4 (correlationID) + 1 (TAG_BUFFER) + len(body).
func NewResponseV1(correlationID int32, body []byte) *Response {
	return &Response{
		messageSize:   int32(4 + 1 + len(body)),
		correlationID: correlationID,
		body:          body,
		headerVersion: 1,
	}
}

// Serialize encodes the response into its wire-format byte representation.
func (r *Response) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, r.messageSize); err != nil {
		return nil, fmt.Errorf("failed to write message_size: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, r.correlationID); err != nil {
		return nil, fmt.Errorf("failed to write correlation_id: %w", err)
	}

	// For header v1, add TAG_BUFFER after correlationID
	if r.headerVersion == 1 {
		buf.WriteByte(0x00)
	}

	if _, err := buf.Write(r.body); err != nil {
		return nil, fmt.Errorf("failed to write body: %w", err)
	}

	return buf.Bytes(), nil
}
