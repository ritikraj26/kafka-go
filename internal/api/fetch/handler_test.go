package fetch

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func TestBuildBody_UnknownTopic(t *testing.T) {
	metaMgr := metadata.NewManager()

	topicID := [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00}

	req := &protocol.FetchRequest{
		MaxWaitMs:    0,
		MinBytes:     1,
		MaxBytes:     1 << 20,
		SessionID:    0,
		SessionEpoch: -1,
		Topics: []protocol.FetchTopic{
			{
				TopicID:    topicID,
				Partitions: []protocol.FetchPartition{{PartitionIndex: 0, FetchOffset: 0}},
			},
		},
	}

	body := BuildBody(req, metaMgr)
	pos := 0

	throttle := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if throttle != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", throttle)
	}

	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if errCode != 0 {
		t.Errorf("error_code = %d, want 0", errCode)
	}

	pos += 4 // session_id

	topicsLen := int(body[pos]) - 1
	pos++
	if topicsLen != 1 {
		t.Fatalf("topics count = %d, want 1", topicsLen)
	}

	var gotID [16]byte
	copy(gotID[:], body[pos:pos+16])
	pos += 16
	if gotID != topicID {
		t.Errorf("topic_id mismatch")
	}

	partsLen := int(body[pos]) - 1
	pos++
	if partsLen != 1 {
		t.Fatalf("partitions count = %d, want 1", partsLen)
	}

	partIdx := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if partIdx != 0 {
		t.Errorf("partition_index = %d, want 0", partIdx)
	}

	partErr := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if partErr != protocol.ErrUnknownTopicID {
		t.Errorf("partition error_code = %d, want %d (ErrUnknownTopicID)", partErr, protocol.ErrUnknownTopicID)
	}
}

func TestBuildBody_KnownTopicWithRecords(t *testing.T) {
	metaMgr := metadata.NewManager()
	dir := t.TempDir()

	topic := metaMgr.CreateTopic("events", 1)

	topicID := topic.ID
	var topicIDBytes [16]byte
	copy(topicIDBytes[:], topicID[:])

	partDir := filepath.Join(dir, "events-0")
	os.MkdirAll(partDir, 0755)
	logData := []byte{0xCA, 0xFE, 0xBA, 0xBE}
	os.WriteFile(filepath.Join(partDir, "00000000000000000000.log"), logData, 0644)

	topic.Partitions[0].LogDir = partDir
	topic.Partitions[0].NextOffset = 1

	req := &protocol.FetchRequest{
		MaxWaitMs:    0,
		MinBytes:     1,
		MaxBytes:     1 << 20,
		SessionID:    0,
		SessionEpoch: -1,
		Topics: []protocol.FetchTopic{
			{
				TopicID:    topicIDBytes,
				Partitions: []protocol.FetchPartition{{PartitionIndex: 0, FetchOffset: 0}},
			},
		},
	}

	body := BuildBody(req, metaMgr)

	if len(body) < 10 {
		t.Fatalf("response body too short: %d bytes", len(body))
	}

	throttle := int32(binary.BigEndian.Uint32(body[0:4]))
	if throttle != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", throttle)
	}
	topLevelErr := int16(binary.BigEndian.Uint16(body[4:6]))
	if topLevelErr != 0 {
		t.Errorf("error_code = %d, want 0", topLevelErr)
	}
}
