package findcoordinator

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// parseResponse parses a FindCoordinator response body.
func parseResponse(t *testing.T, body []byte) (errCode int16, nodeID int32, host string, port int32) {
	t.Helper()
	pos := 0
	errCode = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	nodeID = int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	// host: STRING (INT16 length + bytes)
	hostLen := int(int16(binary.BigEndian.Uint16(body[pos : pos+2])))
	pos += 2
	host = string(body[pos : pos+hostLen])
	pos += hostLen
	port = int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	return
}

func TestFindCoordinator_WithRegistry(t *testing.T) {
	reg := broker.NewRegistry(2)
	reg.Register(&broker.Broker{ID: 1, Host: "node1", Port: 9092})
	reg.Register(&broker.Broker{ID: 2, Host: "node2", Port: 9093})
	req := &protocol.FindCoordinatorRequest{Key: "my-group"}
	body := BuildBody(req, reg)
	errCode, nodeID, host, port := parseResponse(t, body)
	if errCode != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0", errCode)
	}
	// Local broker is ID=2
	if nodeID != 2 {
		t.Errorf("nodeID = %d, want 2", nodeID)
	}
	if host != "node2" {
		t.Errorf("host = %q, want \"node2\"", host)
	}
	if port != 9093 {
		t.Errorf("port = %d, want 9093", port)
	}
}

func TestFindCoordinator_NilRegistry(t *testing.T) {
	req := &protocol.FindCoordinatorRequest{Key: "any-group"}
	body := BuildBody(req, nil)
	errCode, nodeID, host, port := parseResponse(t, body)
	if errCode != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0", errCode)
	}
	// Fallback values
	if nodeID != 1 {
		t.Errorf("nodeID = %d, want 1 (fallback)", nodeID)
	}
	if host != "localhost" {
		t.Errorf("host = %q, want \"localhost\" (fallback)", host)
	}
	if port != 9092 {
		t.Errorf("port = %d, want 9092 (fallback)", port)
	}
}
