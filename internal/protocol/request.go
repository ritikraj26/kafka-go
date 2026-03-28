package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// maxRequestSize caps the message_size a client may declare to prevent OOM.
const maxRequestSize = 100 * 1024 * 1024 // 100 MB

type RequestHeader struct {
	messageSize   int32
	apiKey        int16
	apiVersion    int16
	correlationID int32
	body          []byte
}

func NewRequestHeader() *RequestHeader {
	return &RequestHeader{}
}

func (r *RequestHeader) GetCorrelationID() int32 {
	return r.correlationID
}

func (r *RequestHeader) GetAPIKey() int16 {
	return r.apiKey
}

func (r *RequestHeader) GetAPIVersion() int16 {
	return r.apiVersion
}

func (r *RequestHeader) GetMessageSize() int32 {
	return r.messageSize
}

func (r *RequestHeader) GetBody() []byte {
	return r.body
}

func (r *RequestHeader) GetErrorCode() int16 {
	if r.apiVersion < 0 || r.apiVersion > 4 {
		return ErrUnsupportedVersion
	}
	return ErrNone
}

func (r *RequestHeader) ReadFrom(conn net.Conn) error {
	if err := binary.Read(conn, binary.BigEndian, &r.messageSize); err != nil {
		return fmt.Errorf("failed to read message_size: %w", err)
	}

	if r.messageSize < 0 || r.messageSize > maxRequestSize {
		return fmt.Errorf("message_size %d exceeds maximum allowed %d", r.messageSize, maxRequestSize)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.apiKey); err != nil {
		return fmt.Errorf("failed to read api_key: %w", err)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.apiVersion); err != nil {
		return fmt.Errorf("failed to read api_version: %w", err)
	}

	if err := binary.Read(conn, binary.BigEndian, &r.correlationID); err != nil {
		return fmt.Errorf("failed to read correlation_id: %w", err)
	}

	// Read the remaining bytes of the request body
	bytesRead := int32(8) // apiKey (2) + apiVersion (2) + correlationID (4)
	remainingBytes := r.messageSize - bytesRead
	if remainingBytes > 0 {
		r.body = make([]byte, remainingBytes)
		if _, err := io.ReadFull(conn, r.body); err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
	}

	return nil
}
