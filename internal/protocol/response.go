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
}

// NewResponse builds a response. message_size is computed as 4 (correlation_id) + len(body).
func NewResponse(correlationID int32, body []byte) *response {
	return &response{
		message_size:   int32(4 + len(body)),
		correlation_id: correlationID,
		body:           body,
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

	if _, err := buf.Write(r.body); err != nil {
		return nil, fmt.Errorf("failed to write body: %w", err)
	}

	return buf.Bytes(), nil
}
