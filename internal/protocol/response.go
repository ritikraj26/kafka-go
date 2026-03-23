package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type response struct {
	message_size   int32
	correlation_id int32
	error_code     int16
}

func NewResponse(messageSize int32, correlationID int32, errorCode int16) *response {
	return &response{
		message_size:   messageSize,
		correlation_id: correlationID,
		error_code:     errorCode,
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

	if err := binary.Write(buf, binary.BigEndian, r.error_code); err != nil {
		return nil, fmt.Errorf("Failed to write error_code: %w", err)
	}

	return buf.Bytes(), nil
}
