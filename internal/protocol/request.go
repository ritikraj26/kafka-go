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
	clientID      *string
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
	if !SupportsVersion(r.apiKey, r.apiVersion) {
		return ErrUnsupportedVersion
	}
	return ErrNone
}

func (r *RequestHeader) ReadFrom(conn net.Conn) error {
	if err := binary.Read(conn, binary.BigEndian, &r.messageSize); err != nil {
		return fmt.Errorf("failed to read message_size: %w", err)
	}

	if r.messageSize < 8 || r.messageSize > maxRequestSize {
		return fmt.Errorf("message_size %d must be between 8 and %d", r.messageSize, maxRequestSize)
	}
	frame := make([]byte, r.messageSize)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return fmt.Errorf("failed to read request frame: %w", err)
	}
	r.apiKey = int16(binary.BigEndian.Uint16(frame[:2]))
	r.apiVersion = int16(binary.BigEndian.Uint16(frame[2:4]))
	r.correlationID = int32(binary.BigEndian.Uint32(frame[4:8]))
	r.body = frame[8:]
	r.clientID = nil
	return nil
}
