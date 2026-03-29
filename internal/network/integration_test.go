package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// TestIntegration_ConsumerGroupLifecycle tests FindCoordinator → JoinGroup →
// SyncGroup → Heartbeat → OffsetCommit → OffsetFetch → LeaveGroup over the wire.
func TestIntegration_ConsumerGroupLifecycle(t *testing.T) {
	metaMgr := metadata.NewManager()
	coord := coordinator.NewCoordinator()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		Serve(ctx, listener, metaMgr, coord, 1)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	// Helper: send a v0 non-flexible request and read the response
	correlationID := int32(1)
	sendRecv := func(apiKey int16, body []byte) []byte {
		correlationID++
		return sendV0Request(t, conn, apiKey, 0, correlationID, body)
	}
	sendRecvV := func(apiKey int16, version int16, body []byte) []byte {
		correlationID++
		return sendV0Request(t, conn, apiKey, version, correlationID, body)
	}

	// 1. FindCoordinator (key="test-group")
	{
		body := encodeString("test-group")
		resp := sendRecv(10, body)
		errCode := int16(binary.BigEndian.Uint16(resp[0:2]))
		if errCode != 0 {
			t.Fatalf("FindCoordinator error_code = %d", errCode)
		}
		nodeID := int32(binary.BigEndian.Uint32(resp[2:6]))
		if nodeID != 1 {
			t.Errorf("coordinator node_id = %d, want 1", nodeID)
		}
	}

	// 2. JoinGroup (group="test-group", member_id="", protocol_type="consumer")
	var memberID string
	var generationID int32
	{
		var body []byte
		body = append(body, encodeString("test-group")...)      // group_id
		body = append(body, encodeInt32(10000)...)              // session_timeout_ms
		body = append(body, encodeString("")...)                // member_id (empty)
		body = append(body, encodeString("consumer")...)        // protocol_type
		body = append(body, encodeInt32(1)...)                  // protocols array count
		body = append(body, encodeString("range")...)           // protocol name
		body = append(body, encodeBytes([]byte("metadata"))...) // protocol metadata

		resp := sendRecv(11, body)
		errCode := int16(binary.BigEndian.Uint16(resp[0:2]))
		if errCode != 0 {
			t.Fatalf("JoinGroup error_code = %d", errCode)
		}
		generationID = int32(binary.BigEndian.Uint32(resp[2:6]))
		if generationID != 1 {
			t.Errorf("generation_id = %d, want 1", generationID)
		}
		// Skip protocol name
		protoLen := int16(binary.BigEndian.Uint16(resp[6:8]))
		pos := 8 + int(protoLen)
		// leader_id
		leaderLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2 + int(leaderLen)
		// member_id
		midLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		memberID = string(resp[pos+2 : pos+2+int(midLen)])
		if memberID == "" {
			t.Fatal("member_id should be non-empty")
		}
	}

	// 3. SyncGroup (provide assignment)
	{
		assignment := []byte{0x01, 0x02}
		var body []byte
		body = append(body, encodeString("test-group")...) // group_id
		body = append(body, encodeInt32(generationID)...)  // generation_id
		body = append(body, encodeString(memberID)...)     // member_id
		body = append(body, encodeInt32(1)...)             // assignments array count
		body = append(body, encodeString(memberID)...)     // assignment member_id
		body = append(body, encodeBytes(assignment)...)    // assignment data

		resp := sendRecv(14, body)
		errCode := int16(binary.BigEndian.Uint16(resp[0:2]))
		if errCode != 0 {
			t.Fatalf("SyncGroup error_code = %d", errCode)
		}
	}

	// 4. Heartbeat
	{
		var body []byte
		body = append(body, encodeString("test-group")...)
		body = append(body, encodeInt32(generationID)...)
		body = append(body, encodeString(memberID)...)

		resp := sendRecv(12, body)
		errCode := int16(binary.BigEndian.Uint16(resp[0:2]))
		if errCode != 0 {
			t.Fatalf("Heartbeat error_code = %d", errCode)
		}
	}

	// 5. OffsetCommit (group="test-group", topic="t1", partition=0, offset=42)
	{
		var body []byte
		body = append(body, encodeString("test-group")...) // group_id
		body = append(body, encodeInt32(generationID)...)  // generation_id
		body = append(body, encodeString(memberID)...)     // member_id
		body = append(body, encodeInt64(-1)...)            // retention_time_ms
		body = append(body, encodeInt32(1)...)             // topics count
		body = append(body, encodeString("t1")...)         // topic name
		body = append(body, encodeInt32(1)...)             // partitions count
		body = append(body, encodeInt32(0)...)             // partition index
		body = append(body, encodeInt64(42)...)            // committed_offset
		body = append(body, encodeString("")...)           // metadata

		resp := sendRecvV(8, 2, body)
		// topics[0].partitions[0].error_code
		// Skip: topics array(4) + topic name string(2+len) + partitions array(4) + partition index(4) → error_code
		pos := 0
		pos += 4 // topics array count
		topicNameLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2 + int(topicNameLen)
		pos += 4 // partitions array count
		pos += 4 // partition index
		errCode := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		if errCode != 0 {
			t.Fatalf("OffsetCommit error_code = %d", errCode)
		}
	}

	// 6. OffsetFetch (group="test-group", topic="t1", partition=0)
	{
		var body []byte
		body = append(body, encodeString("test-group")...) // group_id
		body = append(body, encodeInt32(1)...)             // topics count
		body = append(body, encodeString("t1")...)         // topic name
		body = append(body, encodeInt32(1)...)             // partitions count
		body = append(body, encodeInt32(0)...)             // partition index

		resp := sendRecvV(9, 1, body)
		// Parse: topics array(4) + topic name(2+len) + parts(4) + part_index(4) + offset(8) + meta(2+len) + error(2)
		pos := 0
		pos += 4 // topics count
		tnLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2 + int(tnLen)
		pos += 4 // partitions count
		pos += 4 // partition index
		offset := int64(binary.BigEndian.Uint64(resp[pos : pos+8]))
		if offset != 42 {
			t.Errorf("OffsetFetch offset = %d, want 42", offset)
		}
	}

	// 7. LeaveGroup
	{
		var body []byte
		body = append(body, encodeString("test-group")...)
		body = append(body, encodeString(memberID)...)

		resp := sendRecv(13, body)
		errCode := int16(binary.BigEndian.Uint16(resp[0:2]))
		if errCode != 0 {
			t.Fatalf("LeaveGroup error_code = %d", errCode)
		}
	}

	// Close connection and shutdown
	conn.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("server shutdown timeout")
	}
}

// --- Wire-format helpers ---

func sendV0Request(t *testing.T, conn net.Conn, apiKey int16, apiVersion int16, corrID int32, body []byte) []byte {
	t.Helper()
	headerSize := 2 + 2 + 4 // api_key + api_version + correlation_id
	messageSize := int32(headerSize + len(body))

	var req []byte
	req = append(req, encodeInt32(messageSize)...)
	buf2 := make([]byte, 2)
	binary.BigEndian.PutUint16(buf2, uint16(apiKey))
	req = append(req, buf2...)
	binary.BigEndian.PutUint16(buf2, uint16(apiVersion))
	req = append(req, buf2...)
	req = append(req, encodeInt32(corrID)...)
	req = append(req, body...)

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		t.Fatalf("read response size: %v", err)
	}
	respSize := int32(binary.BigEndian.Uint32(sizeBuf))
	resp := make([]byte, respSize)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("read response body: %v", err)
	}

	// First 4 bytes: correlation_id
	gotCorrID := int32(binary.BigEndian.Uint32(resp[0:4]))
	if gotCorrID != corrID {
		t.Fatalf("correlation_id = %d, want %d", gotCorrID, corrID)
	}
	return resp[4:] // skip correlation_id
}

func encodeString(s string) []byte {
	buf := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(s)))
	copy(buf[2:], s)
	return buf
}

func encodeBytes(b []byte) []byte {
	buf := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(b)))
	copy(buf[4:], b)
	return buf
}

func encodeInt32(v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf
}

func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}
