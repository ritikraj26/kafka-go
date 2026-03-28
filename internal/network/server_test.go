package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// TestIntegration_ApiVersionsRoundtrip starts the broker on a random port,
// sends a raw ApiVersions v4 request, and validates the wire-format response.
func TestIntegration_ApiVersionsRoundtrip(t *testing.T) {
	metaMgr := metadata.NewManager()

	// Bind to a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		Serve(ctx, listener, metaMgr)
		close(done)
	}()

	// Give server a moment to start accepting
	time.Sleep(50 * time.Millisecond)

	// Connect to broker
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Build an ApiVersions v4 request:
	//   message_size (4) + api_key (2) + api_version (2) + correlation_id (4) + body
	// Body: client_id compact string "test" (varint 5, then 4 bytes) + TAG_BUFFER (0x00)
	var body []byte
	body = append(body, 0x05)          // compact string length: 4+1 = 5
	body = append(body, "test"...)     // client_id
	body = append(body, 0x00)          // TAG_BUFFER

	// Header: api_key=18, api_version=4, correlation_id=99
	headerSize := 2 + 2 + 4 // api_key + api_version + correlation_id
	messageSize := int32(headerSize + len(body))

	var request []byte
	buf4 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf4, uint32(messageSize))
	request = append(request, buf4...)

	buf2 := make([]byte, 2)
	binary.BigEndian.PutUint16(buf2, 18) // api_key = ApiVersions
	request = append(request, buf2...)

	binary.BigEndian.PutUint16(buf2, 4) // api_version = 4
	request = append(request, buf2...)

	binary.BigEndian.PutUint32(buf4, 99) // correlation_id = 99
	request = append(request, buf4...)

	request = append(request, body...)

	// Send request
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Read message_size (4 bytes)
	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respSizeBuf); err != nil {
		t.Fatalf("failed to read response size: %v", err)
	}
	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	if respSize <= 0 || respSize > 1024 {
		t.Fatalf("unexpected response size: %d", respSize)
	}

	// Read response body
	respBody := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	// Validate correlation_id (first 4 bytes of response body)
	correlationID := int32(binary.BigEndian.Uint32(respBody[0:4]))
	if correlationID != 99 {
		t.Errorf("correlation_id = %d, want 99", correlationID)
	}

	// ApiVersions uses v0 header (no TAG_BUFFER), so response body starts at byte 4
	// Byte 4-5: error_code
	errorCode := int16(binary.BigEndian.Uint16(respBody[4:6]))
	if errorCode != 0 {
		t.Errorf("error_code = %d, want 0", errorCode)
	}

	// Byte 6: compact array length (N+1) for the supported APIs
	numAPIs := int(respBody[6]) - 1
	if numAPIs != 5 {
		t.Errorf("number of APIs = %d, want 5", numAPIs)
	}

	// Close client connection so the handler's read loop exits
	conn.Close()

	// Clean shutdown
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("server did not shut down within 3 seconds")
	}
}
