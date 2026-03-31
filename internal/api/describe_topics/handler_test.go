package describetopics

import (
	"encoding/binary"
	"testing"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// readCompactString reads a COMPACT_STRING (unsigned varint length, then bytes).
func readCompactString(body []byte, pos int) (string, int) {
	l, n := readUvarint(body, pos)
	if n == 0 {
		return "", 0
	}
	strLen := int(l) - 1 // compact string: length is N+1
	pos += n
	return string(body[pos : pos+strLen]), pos + strLen
}

func readUvarint(buf []byte, pos int) (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if pos+i >= len(buf) {
			return 0, 0
		}
		b := buf[pos+i]
		x |= uint64(b&0x7f) << s
		if b < 0x80 {
			return x, i + 1
		}
		s += 7
	}
	return 0, 0
}

func TestDescribeTopics_UnknownTopic(t *testing.T) {
	mgr := metadata.NewManager()
	req := &protocol.DescribeTopicPartitionsRequest{
		TopicNames: []string{"ghost"},
	}
	pos := 0
	// throttle_time_ms (INT32)
	pos += 4
	body := BuildBody(req, mgr)
	// topics compact array length
	topicsLen, n := readUvarint(body, pos)
	pos += n
	if int(topicsLen)-1 != 1 {
		t.Fatalf("topics count = %d, want 1", int(topicsLen)-1)
	}
	// error_code (INT16) should be ErrUnknownTopicOrPartition
	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if errCode != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want ErrUnknownTopicOrPartition (%d)", errCode, protocol.ErrUnknownTopicOrPartition)
	}
}

func TestDescribeTopics_KnownTopic(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("orders", 1)
	req := &protocol.DescribeTopicPartitionsRequest{
		TopicNames: []string{"orders"},
	}
	pos := 0
	body := BuildBody(req, mgr)
	pos += 4 // throttle_time_ms
	topicsLen, n := readUvarint(body, pos)
	pos += n
	if int(topicsLen)-1 != 1 {
		t.Fatalf("topics count = %d, want 1", int(topicsLen)-1)
	}
	// error_code should be 0 (ErrNone)
	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if errCode != protocol.ErrNone {
		t.Errorf("error_code = %d, want 0 (ErrNone)", errCode)
	}
}

func TestDescribeTopics_SortedAlphabetically(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("zebra", 1)
	mgr.CreateTopic("apple", 1)
	req := &protocol.DescribeTopicPartitionsRequest{
		TopicNames: []string{"zebra", "apple"},
	}
	body := BuildBody(req, mgr)
	pos := 4                       // skip throttle_time_ms
	_, n := readUvarint(body, pos) // topics compact array length
	pos += n
	pos += 2 // error_code
	// First topic entry: skip error_code, then read compact string name
	name, _ := readCompactString(body, pos)
	if name != "apple" {
		t.Errorf("first topic = %q, want \"apple\" (alphabetical order)", name)
	}
}

func TestDescribeTopics_MultipleTopicsMixed(t *testing.T) {
	mgr := metadata.NewManager()
	mgr.CreateTopic("exists", 1)
	req := &protocol.DescribeTopicPartitionsRequest{
		TopicNames: []string{"exists", "missing"},
	}
	body := BuildBody(req, mgr)
	pos := 4 // throttle_time_ms
	topicsLen, n := readUvarint(body, pos)
	pos += n
	if int(topicsLen)-1 != 2 {
		t.Fatalf("topics count = %d, want 2", int(topicsLen)-1)
	}
	// First ("exists" sorted after "missing") — "exists" < "missing" alphabetically
	// so "exists" should come first
	errCode1 := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if errCode1 != protocol.ErrNone {
		t.Errorf("first topic error_code = %d, want 0", errCode1)
	}
}
