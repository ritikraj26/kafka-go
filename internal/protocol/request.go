package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// MaxRequestSize is the protocol implementation's upper bound for a raw frame.
const MaxRequestSize = 100 * 1024 * 1024

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
	if _, err := r.ReadSize(conn); err != nil {
		return err
	}
	return r.ReadFrame(conn)
}

// ReadSize consumes only the fixed four-byte prefix, before any frame allocation.
func (r *RequestHeader) ReadSize(conn io.Reader) (int64, error) {
	r.body = nil
	r.clientID = nil
	if err := binary.Read(conn, binary.BigEndian, &r.messageSize); err != nil {
		return 0, fmt.Errorf("failed to read message_size: %w", err)
	}
	if r.messageSize < 8 || r.messageSize > MaxRequestSize {
		return 0, fmt.Errorf("message_size %d must be between 8 and %d", r.messageSize, MaxRequestSize)
	}
	return int64(r.messageSize), nil
}

// ReadFrame is called after the network layer has reserved the declared bytes.
func (r *RequestHeader) ReadFrame(conn io.Reader) error {
	if r.messageSize < 8 || r.messageSize > MaxRequestSize {
		return fmt.Errorf("invalid frame size")
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

func (r *RequestHeader) ReleaseFrame() { r.body = nil; r.clientID = nil }
