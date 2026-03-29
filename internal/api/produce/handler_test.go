package produce

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func TestBuildBody_UnknownTopic(t *testing.T) {
	metaMgr := metadata.NewManager()

	req := &protocol.ProduceRequest{
		Acks: -1,
		Topics: []protocol.ProduceTopic{
			{
				Name: "nonexistent",
				Partitions: []protocol.ProducePartition{
					{Index: 0, Records: []byte{0x01, 0x02}},
				},
			},
		},
	}

	body := BuildBody(req, metaMgr, 1)
	pos := 0

	topicsArrayLen := int(body[pos]) - 1
	pos++
	if topicsArrayLen != 1 {
		t.Fatalf("topics array len = %d, want 1", topicsArrayLen)
	}

	nameLen := int(body[pos]) - 1
	pos++
	topicName := string(body[pos : pos+nameLen])
	pos += nameLen
	if topicName != "nonexistent" {
		t.Errorf("topic name = %q, want %q", topicName, "nonexistent")
	}

	partArrayLen := int(body[pos]) - 1
	pos++
	if partArrayLen != 1 {
		t.Fatalf("partitions array len = %d, want 1", partArrayLen)
	}

	partIdx := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if partIdx != 0 {
		t.Errorf("partition index = %d, want 0", partIdx)
	}

	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if errCode != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want %d", errCode, protocol.ErrUnknownTopicOrPartition)
	}
}

func TestBuildBody_ValidTopic(t *testing.T) {
	metaMgr := metadata.NewManager()
	dir := t.TempDir()
	metaMgr.SetLogDir(dir)

	topic := metaMgr.CreateTopic("orders", 1)
	topic.Partitions[0].LogDir = dir + "/orders-0"

	req := &protocol.ProduceRequest{
		Acks: -1,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{Index: 0, Records: []byte{0xDE, 0xAD}},
				},
			},
		},
	}

	os.Setenv("OLLAMA_URL", "http://localhost:1")
	defer os.Unsetenv("OLLAMA_URL")

	body := BuildBody(req, metaMgr, 1)
	pos := 0

	topicsArrayLen := int(body[pos]) - 1
	pos++
	if topicsArrayLen != 1 {
		t.Fatalf("topics array len = %d, want 1", topicsArrayLen)
	}

	nameLen := int(body[pos]) - 1
	pos++
	topicName := string(body[pos : pos+nameLen])
	pos += nameLen
	if topicName != "orders" {
		t.Errorf("topic name = %q, want %q", topicName, "orders")
	}

	partArrayLen := int(body[pos]) - 1
	pos++
	if partArrayLen != 1 {
		t.Fatalf("partitions array len = %d, want 1", partArrayLen)
	}

	partIdx := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if partIdx != 0 {
		t.Errorf("partition index = %d, want 0", partIdx)
	}

	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if errCode != protocol.ErrNone {
		t.Errorf("error_code = %d, want %d (ErrNone)", errCode, protocol.ErrNone)
	}

	baseOffset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	if baseOffset != 0 {
		t.Errorf("base_offset = %d, want 0", baseOffset)
	}
}
