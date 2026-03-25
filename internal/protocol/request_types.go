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

// FetchTopic represents a topic in a Fetch request
type FetchTopic struct {
	TopicID    [16]byte // UUID in v13+
	Partitions []FetchPartition
}

// FetchPartition represents a partition in a Fetch request
type FetchPartition struct {
	PartitionIndex int32
	FetchOffset    int64
}

// FetchRequest request (api_key=1)
type FetchRequest struct {
	Header       *RequestHeader
	MaxWaitMs    int32
	MinBytes     int32
	MaxBytes     int32
	SessionID    int32
	SessionEpoch int32
	Topics       []FetchTopic
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

// ParseFetchRequest parses a Fetch request (v16)
func ParseFetchRequest(header *RequestHeader) (*FetchRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// Fetch v16 uses flexible request format (KIP-482)
	// First, parse the flexible request header fields

	// 1. Read cursor field - nullable byte (0x00 = null/no cursor)
	cursorByte, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor: %w", err)
	}
	_ = cursorByte

	// 2. Read client software identifier - simple length-prefixed string
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

	// 3. Read TAG_BUFFER for flexible request header
	tagBuffer, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read header TAG_BUFFER: %w", err)
	}
	_ = tagBuffer

	// Now parse the actual Fetch v16 request fields

	// max_wait_ms (INT32)
	maxWaitMs, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read max_wait_ms: %w", err)
	}

	// min_bytes (INT32)
	minBytes, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read min_bytes: %w", err)
	}

	// max_bytes (INT32)
	maxBytes, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read max_bytes: %w", err)
	}

	// isolation_level (INT8)
	_, err = decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read isolation_level: %w", err)
	}

	// session_id (INT32)
	sessionID, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read session_id: %w", err)
	}

	// session_epoch (INT32)
	sessionEpoch, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read session_epoch: %w", err)
	}

	// topics (COMPACT_ARRAY)
	topicsLen, err := decoder.ReadUnsignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read topics array length: %w", err)
	}

	var topics []FetchTopic
	if topicsLen > 0 {
		actualLen := topicsLen - 1 // compact array encoding
		for i := uint64(0); i < actualLen; i++ {
			// topic_id (UUID - 16 bytes) - for v13+
			topicIDBytes := make([]byte, 16)
			for j := 0; j < 16; j++ {
				b, err := decoder.ReadByte()
				if err != nil {
					return nil, fmt.Errorf("failed to read topic_id byte %d: %w", j, err)
				}
				topicIDBytes[j] = b
			}

			// partitions (COMPACT_ARRAY)
			partitionsLen, err := decoder.ReadUnsignedVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read partitions array length: %w", err)
			}

			var partitions []FetchPartition
			if partitionsLen > 0 {
				actualPartLen := partitionsLen - 1
				for j := uint64(0); j < actualPartLen; j++ {
					// partition_index (INT32)
					partitionIndex, err := decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read partition_index: %w", err)
					}

					// current_leader_epoch (INT32)
					_, err = decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read current_leader_epoch: %w", err)
					}

					// fetch_offset (INT64)
					fetchOffset, err := decoder.ReadInt64()
					if err != nil {
						return nil, fmt.Errorf("failed to read fetch_offset: %w", err)
					}

					// last_fetched_epoch (INT32)
					_, err = decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read last_fetched_epoch: %w", err)
					}

					// log_start_offset (INT64)
					_, err = decoder.ReadInt64()
					if err != nil {
						return nil, fmt.Errorf("failed to read log_start_offset: %w", err)
					}

					// partition_max_bytes (INT32)
					_, err = decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read partition_max_bytes: %w", err)
					}

					// TAG_BUFFER for partition
					_, err = decoder.ReadByte()
					if err != nil {
						return nil, fmt.Errorf("failed to read partition TAG_BUFFER: %w", err)
					}

					partitions = append(partitions, FetchPartition{
						PartitionIndex: partitionIndex,
						FetchOffset:    fetchOffset,
					})
				}
			}

			// Store the topic ID
			var topicID [16]byte
			copy(topicID[:], topicIDBytes)

			topics = append(topics, FetchTopic{
				TopicID:    topicID,
				Partitions: partitions,
			})

			// TAG_BUFFER for topic
			_, err = decoder.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read topic TAG_BUFFER: %w", err)
			}
		}
	}

	// forgotten_topics_data (COMPACT_ARRAY)
	forgottenLen, err := decoder.ReadUnsignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read forgotten_topics_data length: %w", err)
	}
	// If length > 0, we need to parse the forgotten topics
	if forgottenLen > 1 { // Remember: compact array, so 1 means 0 elements
		actualLen := forgottenLen - 1
		for i := uint64(0); i < actualLen; i++ {
			// topic_id (UUID - 16 bytes)
			for j := 0; j < 16; j++ {
				_, err := decoder.ReadByte()
				if err != nil {
					return nil, fmt.Errorf("failed to read forgotten topic_id: %w", err)
				}
			}
			// partitions (COMPACT_ARRAY of INT32)
			partLen, err := decoder.ReadUnsignedVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read forgotten partitions length: %w", err)
			}
			if partLen > 1 {
				actualPartLen := partLen - 1
				for j := uint64(0); j < actualPartLen; j++ {
					_, err := decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read forgotten partition: %w", err)
					}
				}
			}
			// TAG_BUFFER for forgotten topic
			_, err = decoder.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read forgotten topic TAG_BUFFER: %w", err)
			}
		}
	}

	// rack_id (COMPACT_STRING)
	_, err = decoder.ReadCompactString()
	if err != nil {
		return nil, fmt.Errorf("failed to read rack_id: %w", err)
	}

	// TAG_BUFFER for request
	_, err = decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read request TAG_BUFFER: %w", err)
	}

	return &FetchRequest{
		Header:       header,
		MaxWaitMs:    maxWaitMs,
		MinBytes:     minBytes,
		MaxBytes:     maxBytes,
		SessionID:    sessionID,
		SessionEpoch: sessionEpoch,
		Topics:       topics,
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
