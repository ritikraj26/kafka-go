package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
)

type RequestHeader struct {
	message_size   int32
	api_key        int16
	api_version    int16
	correlation_id int32
}

func NewRequestHeader() *RequestHeader {
	return &RequestHeader{}
}

func (r *RequestHeader) CorrelationID() int32 {
	return r.correlation_id
}

func (r *RequestHeader) ReadFrom(conn net.Conn) error {
	if err := binary.Read(conn, binary.BigEndian, &r.message_size); err != nil {
		return fmt.Errorf("failed to read message_size: %w", err)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.api_key); err != nil {
		return fmt.Errorf("failed to read api_key: %w", err)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.api_version); err != nil {
		return fmt.Errorf("failed to read api_version: %w", err)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.correlation_id); err != nil {
		return fmt.Errorf("failed to read correlation_id: %w", err)
	}

	return nil
}
