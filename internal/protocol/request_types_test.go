package protocol

import (
	"encoding/binary"
	"testing"
)

// buildFlexHeader builds the common flexible request header prefix used by most
// APIs: cursor(1) + client_software_name(length-prefixed) + TAG_BUFFER(1).
func buildFlexHeader() []byte {
	var buf []byte
	buf = append(buf, 0x00)       // cursor (null)
	buf = append(buf, 0x05)       // client software name length = 5
	buf = append(buf, "kafka"...) // client software name
	buf = append(buf, 0x00)       // TAG_BUFFER
	return buf
}

func makeRequestHeader(apiKey int16, apiVersion int16, body []byte) *RequestHeader {
	return &RequestHeader{
		messageSize:   int32(8 + len(body)),
		apiKey:        apiKey,
		apiVersion:    apiVersion,
		correlationID: 42,
		body:          body,
	}
}

func TestParseApiVersionsRequest(t *testing.T) {
	// Body: compact_string "test-client" + TAG_BUFFER
	enc := NewEncoder()
	enc.WriteCompactString("test-client")
	enc.WriteByte(0x00) // TAG_BUFFER

	header := makeRequestHeader(APIKeyApiVersions, 4, enc.Bytes())
	req, err := ParseApiVersionsRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.ClientID != "test-client" {
		t.Errorf("ClientID = %q, want %q", req.ClientID, "test-client")
	}
	if req.GetAPIKey() != APIKeyApiVersions {
		t.Errorf("APIKey = %d, want %d", req.GetAPIKey(), APIKeyApiVersions)
	}
}

func TestParseProduceRequest(t *testing.T) {
	// Build body: flex header + transactional_id(null) + acks + timeout +
	//   topics compact array (1 topic "orders" with 1 partition 0 with small records) + TAG_BUFFER
	var body []byte
	body = append(body, buildFlexHeader()...)

	// transactional_id: null (compact nullable string: 0 = null)
	body = append(body, 0x00)

	// acks: -1 (all)
	acks := make([]byte, 2)
	binary.BigEndian.PutUint16(acks, uint16(0xFFFF)) // -1
	body = append(body, acks...)

	// timeout_ms: 30000
	timeout := make([]byte, 4)
	binary.BigEndian.PutUint32(timeout, 30000)
	body = append(body, timeout...)

	// topics compact array: 1 element → varint 2
	body = append(body, 0x02)

	// topic name: "orders" (compact string: len+1 = 7)
	body = append(body, 0x07)
	body = append(body, "orders"...)

	// partitions compact array: 1 element → varint 2
	body = append(body, 0x02)

	// partition index: 0
	partIdx := make([]byte, 4)
	binary.BigEndian.PutUint32(partIdx, 0)
	body = append(body, partIdx...)

	// records: compact bytes with 4 bytes of data (len+1 = 5)
	body = append(body, 0x05)
	body = append(body, 0xDE, 0xAD, 0xBE, 0xEF)

	// TAG_BUFFER for partition
	body = append(body, 0x00)
	// TAG_BUFFER for topic
	body = append(body, 0x00)
	// TAG_BUFFER for request body
	body = append(body, 0x00)

	header := makeRequestHeader(APIKeyProduce, 11, body)
	req, err := ParseProduceRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Acks != -1 {
		t.Errorf("Acks = %d, want -1", req.Acks)
	}
	if req.TimeoutMs != 30000 {
		t.Errorf("TimeoutMs = %d, want 30000", req.TimeoutMs)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("len(Topics) = %d, want 1", len(req.Topics))
	}
	if req.Topics[0].Name != "orders" {
		t.Errorf("Topics[0].Name = %q, want %q", req.Topics[0].Name, "orders")
	}
	if len(req.Topics[0].Partitions) != 1 {
		t.Fatalf("len(Partitions) = %d, want 1", len(req.Topics[0].Partitions))
	}
	if req.Topics[0].Partitions[0].Index != 0 {
		t.Errorf("Partitions[0].Index = %d, want 0", req.Topics[0].Partitions[0].Index)
	}
	if len(req.Topics[0].Partitions[0].Records) != 4 {
		t.Errorf("len(Records) = %d, want 4", len(req.Topics[0].Partitions[0].Records))
	}
}

func TestParseFetchRequest(t *testing.T) {
	var body []byte
	body = append(body, buildFlexHeader()...)

	// max_wait_ms: 500
	tmp4 := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp4, 500)
	body = append(body, tmp4...)

	// min_bytes: 1
	binary.BigEndian.PutUint32(tmp4, 1)
	body = append(body, tmp4...)

	// max_bytes: 1048576
	binary.BigEndian.PutUint32(tmp4, 1048576)
	body = append(body, tmp4...)

	// isolation_level: 0
	body = append(body, 0x00)

	// session_id: 0
	binary.BigEndian.PutUint32(tmp4, 0)
	body = append(body, tmp4...)

	// session_epoch: -1
	binary.BigEndian.PutUint32(tmp4, 0xFFFFFFFF)
	body = append(body, tmp4...)

	// topics compact array: 1 element → varint 2
	body = append(body, 0x02)

	// topic_id: 16 bytes UUID (all 0x11 for identification)
	topicID := [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00}
	body = append(body, topicID[:]...)

	// partitions compact array: 1 element → varint 2
	body = append(body, 0x02)

	// partition_index: 0
	binary.BigEndian.PutUint32(tmp4, 0)
	body = append(body, tmp4...)

	// current_leader_epoch: -1
	binary.BigEndian.PutUint32(tmp4, 0xFFFFFFFF)
	body = append(body, tmp4...)

	// fetch_offset: 0
	tmp8 := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8, 0)
	body = append(body, tmp8...)

	// last_fetched_epoch: -1
	binary.BigEndian.PutUint32(tmp4, 0xFFFFFFFF)
	body = append(body, tmp4...)

	// log_start_offset: -1
	binary.BigEndian.PutUint64(tmp8, 0xFFFFFFFFFFFFFFFF)
	body = append(body, tmp8...)

	// partition_max_bytes: 1048576
	binary.BigEndian.PutUint32(tmp4, 1048576)
	body = append(body, tmp4...)

	// TAG_BUFFER for partition
	body = append(body, 0x00)
	// TAG_BUFFER for topic
	body = append(body, 0x00)

	// forgotten_topics_data: empty compact array → varint 1
	body = append(body, 0x01)

	// rack_id: empty compact string → varint 1
	body = append(body, 0x01)

	// TAG_BUFFER for request
	body = append(body, 0x00)

	header := makeRequestHeader(APIKeyFetch, 16, body)
	req, err := ParseFetchRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.MaxWaitMs != 500 {
		t.Errorf("MaxWaitMs = %d, want 500", req.MaxWaitMs)
	}
	if req.MinBytes != 1 {
		t.Errorf("MinBytes = %d, want 1", req.MinBytes)
	}
	if req.MaxBytes != 1048576 {
		t.Errorf("MaxBytes = %d, want 1048576", req.MaxBytes)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("len(Topics) = %d, want 1", len(req.Topics))
	}
	if req.Topics[0].TopicID != topicID {
		t.Errorf("TopicID mismatch")
	}
	if len(req.Topics[0].Partitions) != 1 {
		t.Fatalf("len(Partitions) = %d, want 1", len(req.Topics[0].Partitions))
	}
	if req.Topics[0].Partitions[0].PartitionIndex != 0 {
		t.Errorf("PartitionIndex = %d, want 0", req.Topics[0].Partitions[0].PartitionIndex)
	}
	if req.Topics[0].Partitions[0].FetchOffset != 0 {
		t.Errorf("FetchOffset = %d, want 0", req.Topics[0].Partitions[0].FetchOffset)
	}
}

func TestParseDescribeTopicPartitionsRequest(t *testing.T) {
	var body []byte
	body = append(body, buildFlexHeader()...)

	// topics compact array: 2 elements → varint 3
	body = append(body, 0x03)

	// topic 1: "foo" (compact string: len+1 = 4)
	body = append(body, 0x04)
	body = append(body, "foo"...)
	body = append(body, 0x00) // TAG_BUFFER

	// topic 2: "bar" (compact string: len+1 = 4)
	body = append(body, 0x04)
	body = append(body, "bar"...)
	body = append(body, 0x00) // TAG_BUFFER

	header := makeRequestHeader(APIKeyDescribeTopicPartitions, 0, body)
	req, err := ParseDescribeTopicPartitionsRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(req.TopicNames) != 2 {
		t.Fatalf("len(TopicNames) = %d, want 2", len(req.TopicNames))
	}
	if req.TopicNames[0] != "foo" {
		t.Errorf("TopicNames[0] = %q, want %q", req.TopicNames[0], "foo")
	}
	if req.TopicNames[1] != "bar" {
		t.Errorf("TopicNames[1] = %q, want %q", req.TopicNames[1], "bar")
	}
}

func TestParseMetadataRequest_AllTopics(t *testing.T) {
	var body []byte
	body = append(body, buildFlexHeader()...)

	// topics: null compact array → varint 0
	body = append(body, 0x00)

	header := makeRequestHeader(APIKeyMetadata, 12, body)
	req, err := ParseMetadataRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.TopicNames != nil {
		t.Errorf("TopicNames = %v, want nil (all topics)", req.TopicNames)
	}
}

func TestParseMetadataRequest_SpecificTopics(t *testing.T) {
	var body []byte
	body = append(body, buildFlexHeader()...)

	// topics compact array: 1 element → varint 2
	body = append(body, 0x02)

	// topic: "events" (compact string: len+1 = 7)
	body = append(body, 0x07)
	body = append(body, "events"...)
	body = append(body, 0x00) // TAG_BUFFER

	header := makeRequestHeader(APIKeyMetadata, 12, body)
	req, err := ParseMetadataRequest(header)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(req.TopicNames) != 1 {
		t.Fatalf("len(TopicNames) = %d, want 1", len(req.TopicNames))
	}
	if req.TopicNames[0] != "events" {
		t.Errorf("TopicNames[0] = %q, want %q", req.TopicNames[0], "events")
	}
}

func TestParseRequest_UnknownAPIKey(t *testing.T) {
	header := makeRequestHeader(999, 0, nil)
	_, err := ParseRequest(header)
	if err == nil {
		t.Fatal("expected error for unknown API key, got nil")
	}
}
