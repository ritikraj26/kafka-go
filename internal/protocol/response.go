package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type response struct {
	message_size   int32
	correlation_id int32
}

func NewResponse(correlationID int32) *response {
	return &response{
		message_size:   4,
		correlation_id: correlationID,
	}
}

func (r *response) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, r.message_size); err != nil {
		return nil, fmt.Errorf("Failed to write message_size: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, r.correlation_id); err != nil {
		return nil, fmt.Errorf("Failed to write correlation_id: %w", err)
	}

	return buf.Bytes(), nil
}
