package protocol

import (
	"fmt"
)

// interface for all request types
type Request interface {
	GetHeader() *RequestHeader
	GetAPIKey() int16
}

// ApiVersionsRequest request (api_key=18)
type ApiVersionsRequest struct {
	Header   *RequestHeader
	ClientID string
}

func (r *ApiVersionsRequest) GetHeader() *RequestHeader {
	return r.Header
}

func (r *ApiVersionsRequest) GetAPIKey() int16 {
	return APIKeyApiVersions
}

// ProduceRequest request (api_key=0) - PLACEHOLDER
type ProduceRequest struct {
	Header          *RequestHeader
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	// TopicData    []TopicProduceData // TODO: implement when needed
}

func (r *ProduceRequest) GetHeader() *RequestHeader {
	return r.Header
}

func (r *ProduceRequest) GetAPIKey() int16 {
	return APIKeyProduce
}

// FetchRequest request (api_key=1) - PLACEHOLDER
type FetchRequest struct {
	Header    *RequestHeader
	ReplicaID int32
	MaxWaitMs int32
	MinBytes  int32
	// Topics []FetchTopic // TODO: implement when needed
}

func (r *FetchRequest) GetHeader() *RequestHeader {
	return r.Header
}

func (r *FetchRequest) GetAPIKey() int16 {
	return APIKeyFetch
}

// DescribeTopicPartitionsRequest request (api_key=75) - PLACEHOLDER
type DescribeTopicPartitionsRequest struct {
	Header     *RequestHeader
	TopicNames []string
	// Additional fields TODO: implement when needed
}

func (r *DescribeTopicPartitionsRequest) GetHeader() *RequestHeader {
	return r.Header
}

func (r *DescribeTopicPartitionsRequest) GetAPIKey() int16 {
	return APIKeyDescribeTopicPartitions
}

// ParseRequest parses the request header and body into an API-specific request type
func ParseRequest(header *RequestHeader) (Request, error) {
	switch header.GetAPIKey() {
	case APIKeyApiVersions:
		return ParseApiVersionsRequest(header)
	case APIKeyProduce:
		return ParseProduceRequest(header)
	case APIKeyFetch:
		return ParseFetchRequest(header)
	case APIKeyDescribeTopicPartitions:
		return ParseDescribeTopicPartitionsRequest(header)
	default:
		return nil, fmt.Errorf("unknown API key: %d", header.GetAPIKey())
	}
}

// parse an ApiVersions request
func ParseApiVersionsRequest(header *RequestHeader) (*ApiVersionsRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// ApiVersions request body contains:
	// - client_id (COMPACT_STRING for v3+)
	// - TAG_BUFFER (1 byte)
	clientID, err := decoder.ReadCompactString()
	if err != nil {
		return nil, fmt.Errorf("failed to read client_id: %w", err)
	}

	return &ApiVersionsRequest{
		Header:   header,
		ClientID: clientID,
	}, nil
}

// placeholder for Produce request parsing
func ParseProduceRequest(header *RequestHeader) (*ProduceRequest, error) {
	// TODO: implement when Produce API is needed
	return &ProduceRequest{
		Header: header,
	}, nil
}

// placeholder for Fetch request parsing
func ParseFetchRequest(header *RequestHeader) (*FetchRequest, error) {
	// TODO: implement when Fetch API is needed
	return &FetchRequest{
		Header: header,
	}, nil
}

// ParseDescribeTopicPartitionsRequest parses a DescribeTopicPartitions request
func ParseDescribeTopicPartitionsRequest(header *RequestHeader) (*DescribeTopicPartitionsRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// DescribeTopicPartitions v0 uses flexible request format (KIP-482).
	// The body includes flexible request header fields before the API-specific fields.

	// 1. Read cursor field - nullable byte (0x00 = null/no cursor)
	cursorByte, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor: %w", err)
	}
	_ = cursorByte // Not used for this stage

	// 2. Read client software identifier - simple length-prefixed string
	// Note: This uses plain length encoding (not COMPACT_STRING +1 encoding)
	// Format: [length_byte][string_bytes...]
	lengthByte, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read client software length: %w", err)
	}

	// Read the client software name bytes
	clientBytes := make([]byte, lengthByte)
	for i := 0; i < int(lengthByte); i++ {
		b, err := decoder.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read client software byte %d: %w", i, err)
		}
		clientBytes[i] = b
	}
	_ = clientBytes // Not used for this stage

	// 3. Read TAG_BUFFER for flexible request header
	tagBuffer, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read header TAG_BUFFER: %w", err)
	}
	_ = tagBuffer // Should be 0x00 (empty)

	// Now read the actual DescribeTopicPartitions v0 request fields:
	// - topics (COMPACT_ARRAY of topic entries)
	// - response_partition_limit (INT32) - comes after topics
	// - cursor (nullable complex type) - comes after limit
	// - TAG_BUFFER for request body

	// Read topics array length (compact array: length+1)
	arrayLen, err := decoder.ReadUnsignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read topics array length: %w", err)
	}

	var topicNames []string
	if arrayLen > 0 {
		actualLen := arrayLen - 1 // compact array encoding
		for i := uint64(0); i < actualLen; i++ {
			// Read topic_name (COMPACT_STRING)
			topicName, err := decoder.ReadCompactString()
			if err != nil {
				return nil, fmt.Errorf("failed to read topic name: %w", err)
			}
			topicNames = append(topicNames, topicName)

			// Skip TAG_BUFFER for each topic entry
			_, err = decoder.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read topic TAG_BUFFER: %w", err)
			}
		}
	}

	return &DescribeTopicPartitionsRequest{
		Header:     header,
		TopicNames: topicNames,
	}, nil
}
