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

// ProduceTopic represents a topic in a Produce request
type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

// ProducePartition represents a partition in a Produce request
type ProducePartition struct {
	Index   int32
	Records []byte // Raw record batch data
}

// ProduceRequest request (api_key=0)
type ProduceRequest struct {
	Header          *RequestHeader
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopic
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
	TopicName  string   // topic name in v0-v12
	Partitions []FetchPartition
}

// FetchPartition represents a partition in a Fetch request
type FetchPartition struct {
	PartitionIndex     int32
	FetchOffset        int64
	CurrentLeaderEpoch int32 // -1 means requester is not tracking epochs (Fetch v0 / old clients)
}

// FetchRequest request (api_key=1)
type FetchRequest struct {
	Header       *RequestHeader
	ReplicaID    int32 // -1 for consumers, >= 0 for follower replicas
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

// MetadataRequest request (api_key=3)
// TopicNames is nil when the client requests metadata for all topics.
type MetadataRequest struct {
	Header     *RequestHeader
	TopicNames []string // nil = all topics
}

func (r *MetadataRequest) GetHeader() *RequestHeader { return r.Header }
func (r *MetadataRequest) GetAPIKey() int16          { return APIKeyMetadata }

// DescribeTopicPartitionsRequest request (api_key=75) - PLACEHOLDER
type DescribeTopicPartitionsRequest struct {
	Header     *RequestHeader
	TopicNames []string
}

func (r *DescribeTopicPartitionsRequest) GetHeader() *RequestHeader {
	return r.Header
}

func (r *DescribeTopicPartitionsRequest) GetAPIKey() int16 {
	return APIKeyDescribeTopicPartitions
}

// --- Consumer Group API request types (v0, non-flexible) ---

// GroupProtocol is a name+metadata pair sent in JoinGroup.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// FindCoordinatorRequest (api_key=10, v0)
type FindCoordinatorRequest struct {
	Header *RequestHeader
	Key    string // group_id
}

func (r *FindCoordinatorRequest) GetHeader() *RequestHeader { return r.Header }
func (r *FindCoordinatorRequest) GetAPIKey() int16          { return APIKeyFindCoordinator }

// JoinGroupRequest (api_key=11, v0)
type JoinGroupRequest struct {
	Header           *RequestHeader
	GroupID          string
	SessionTimeoutMs int32
	MemberID         string
	ProtocolType     string
	Protocols        []GroupProtocol
}

func (r *JoinGroupRequest) GetHeader() *RequestHeader { return r.Header }
func (r *JoinGroupRequest) GetAPIKey() int16          { return APIKeyJoinGroup }

// SyncGroupAssignment is a member_id→assignment pair in SyncGroup.
type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest (api_key=14, v0)
type SyncGroupRequest struct {
	Header       *RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

func (r *SyncGroupRequest) GetHeader() *RequestHeader { return r.Header }
func (r *SyncGroupRequest) GetAPIKey() int16          { return APIKeySyncGroup }

// HeartbeatRequest (api_key=12, v0)
type HeartbeatRequest struct {
	Header       *RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (r *HeartbeatRequest) GetHeader() *RequestHeader { return r.Header }
func (r *HeartbeatRequest) GetAPIKey() int16          { return APIKeyHeartbeat }

// LeaveGroupRequest (api_key=13, v0)
type LeaveGroupRequest struct {
	Header   *RequestHeader
	GroupID  string
	MemberID string
}

func (r *LeaveGroupRequest) GetHeader() *RequestHeader { return r.Header }
func (r *LeaveGroupRequest) GetAPIKey() int16          { return APIKeyLeaveGroup }

// OffsetCommitTopic is a topic in an OffsetCommit request.
type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

// OffsetCommitPartition is a partition in an OffsetCommit request.
type OffsetCommitPartition struct {
	Index           int32
	CommittedOffset int64
	Metadata        string
}

// OffsetCommitRequest (api_key=8, v2)
type OffsetCommitRequest struct {
	Header       *RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
	RetentionMs  int64
	Topics       []OffsetCommitTopic
}

func (r *OffsetCommitRequest) GetHeader() *RequestHeader { return r.Header }
func (r *OffsetCommitRequest) GetAPIKey() int16          { return APIKeyOffsetCommit }

// OffsetFetchTopic is a topic in an OffsetFetch request.
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest (api_key=9, v1)
type OffsetFetchRequest struct {
	Header  *RequestHeader
	GroupID string
	Topics  []OffsetFetchTopic
}

func (r *OffsetFetchRequest) GetHeader() *RequestHeader { return r.Header }
func (r *OffsetFetchRequest) GetAPIKey() int16          { return APIKeyOffsetFetch }

// ListOffsetsTopic is a topic in a ListOffsets request.
type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

// ListOffsetsPartition is a partition in a ListOffsets request.
type ListOffsetsPartition struct {
	Index     int32
	Timestamp int64 // -1 = latest, -2 = earliest
}

// ListOffsetsRequest (api_key=2, v1)
type ListOffsetsRequest struct {
	Header    *RequestHeader
	ReplicaID int32
	Topics    []ListOffsetsTopic
}

func (r *ListOffsetsRequest) GetHeader() *RequestHeader { return r.Header }
func (r *ListOffsetsRequest) GetAPIKey() int16          { return APIKeyListOffsets }

// ParseRequest parses the request header and body into an API-specific request type
func ParseRequest(header *RequestHeader) (Request, error) {
	switch header.GetAPIKey() {
	case APIKeyApiVersions:
		return ParseApiVersionsRequest(header)
	case APIKeyProduce:
		return ParseProduceRequest(header)
	case APIKeyFetch:
		if header.GetAPIVersion() == 0 {
			return ParseFetchRequestV0(header)
		}
		return ParseFetchRequest(header)
	case APIKeyDescribeTopicPartitions:
		return ParseDescribeTopicPartitionsRequest(header)
	case APIKeyMetadata:
		return ParseMetadataRequest(header)
	case APIKeyFindCoordinator:
		return ParseFindCoordinatorRequest(header)
	case APIKeyJoinGroup:
		return ParseJoinGroupRequest(header)
	case APIKeySyncGroup:
		return ParseSyncGroupRequest(header)
	case APIKeyHeartbeat:
		return ParseHeartbeatRequest(header)
	case APIKeyLeaveGroup:
		return ParseLeaveGroupRequest(header)
	case APIKeyOffsetCommit:
		return ParseOffsetCommitRequest(header)
	case APIKeyOffsetFetch:
		return ParseOffsetFetchRequest(header)
	case APIKeyListOffsets:
		return ParseListOffsetsRequest(header)
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

// ParseProduceRequest parses a Produce request (v11)
func ParseProduceRequest(header *RequestHeader) (*ProduceRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// Produce v11 uses flexible request format (KIP-482)
	// First, parse the flexible request header fields

	// 1. Read cursor field (nullable byte)
	_, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor: %w", err)
	}

	// 2. Read client software identifier (length-prefixed string)
	lengthByte, err := decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read client software length: %w", err)
	}

	// Skip client software name bytes
	for i := 0; i < int(lengthByte); i++ {
		_, err = decoder.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read client software byte %d: %w", i, err)
		}
	}

	// 3. Read TAG_BUFFER for flexible request header
	_, err = decoder.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read header TAG_BUFFER: %w", err)
	}

	// Now read the actual Produce v11 request fields:
	// - transactional_id (COMPACT_NULLABLE_STRING)
	// - acks (INT16)
	// - timeout_ms (INT32)
	// - topics (COMPACT_ARRAY)
	// - TAG_BUFFER for request body

	// Read transactional_id
	transactionalID, err := decoder.ReadCompactNullableString()
	if err != nil {
		return nil, fmt.Errorf("failed to read transactional_id: %w", err)
	}

	// Read acks
	acks, err := decoder.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read acks: %w", err)
	}

	// Read timeout_ms
	timeoutMs, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to read timeout_ms: %w", err)
	}

	// Read topics array (COMPACT_ARRAY)
	topicsLen, err := decoder.ReadUnsignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read topics array length: %w", err)
	}

	var topics []ProduceTopic
	if topicsLen > 0 {
		actualLen := topicsLen - 1 // compact array encoding
		for i := uint64(0); i < actualLen; i++ {
			// Read topic name (COMPACT_STRING)
			topicName, err := decoder.ReadCompactString()
			if err != nil {
				return nil, fmt.Errorf("failed to read topic name: %w", err)
			}

			// Read partitions array (COMPACT_ARRAY)
			partitionsLen, err := decoder.ReadUnsignedVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read partitions array length: %w", err)
			}

			var partitions []ProducePartition
			if partitionsLen > 0 {
				actualPartLen := partitionsLen - 1
				for j := uint64(0); j < actualPartLen; j++ {
					// Read partition index (INT32)
					partitionIndex, err := decoder.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed to read partition index: %w", err)
					}

					// Read records (COMPACT_BYTES)
					records, err := decoder.ReadCompactBytes()
					if err != nil {
						return nil, fmt.Errorf("failed to read records: %w", err)
					}

					partitions = append(partitions, ProducePartition{
						Index:   partitionIndex,
						Records: records,
					})

					// Skip TAG_BUFFER for partition
					_, err = decoder.ReadByte()
					if err != nil {
						return nil, fmt.Errorf("failed to read partition TAG_BUFFER: %w", err)
					}
				}
			}

			topics = append(topics, ProduceTopic{
				Name:       topicName,
				Partitions: partitions,
			})

			// Skip TAG_BUFFER for topic
			_, err = decoder.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read topic TAG_BUFFER: %w", err)
			}
		}
	}

	return &ProduceRequest{
		Header:          header,
		TransactionalID: transactionalID,
		Acks:            acks,
		TimeoutMs:       timeoutMs,
		Topics:          topics,
	}, nil
}

// ParseFetchRequestV0 parses a Fetch request using the v0 wire format.
// Fetch v0 layout: ReplicaID(INT32), MaxWaitMs(INT32), MinBytes(INT32),
// Topics(ARRAY[TopicName(STRING), Partitions(ARRAY[Partition(INT32), FetchOffset(INT64), MaxBytes(INT32)])])
func ParseFetchRequestV0(header *RequestHeader) (*FetchRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// Skip client_id (STRING) in the v0 request header
	_, err := decoder.ReadString()
	if err != nil {
		return nil, fmt.Errorf("v0 fetch: failed to read client_id: %w", err)
	}

	replicaID, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("v0 fetch: failed to read replica_id: %w", err)
	}

	maxWaitMs, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("v0 fetch: failed to read max_wait_ms: %w", err)
	}

	minBytes, err := decoder.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("v0 fetch: failed to read min_bytes: %w", err)
	}

	numTopics, err := decoder.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("v0 fetch: failed to read topics array len: %w", err)
	}

	var topics []FetchTopic
	for i := int32(0); i < numTopics; i++ {
		topicName, err := decoder.ReadString()
		if err != nil {
			return nil, fmt.Errorf("v0 fetch: failed to read topic name: %w", err)
		}

		numPartitions, err := decoder.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("v0 fetch: failed to read partitions array len: %w", err)
		}

		var partitions []FetchPartition
		for j := int32(0); j < numPartitions; j++ {
			partIdx, err := decoder.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("v0 fetch: failed to read partition index: %w", err)
			}
			fetchOffset, err := decoder.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("v0 fetch: failed to read fetch_offset: %w", err)
			}
			// max_bytes per partition (INT32) — skip
			_, err = decoder.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("v0 fetch: failed to read partition max_bytes: %w", err)
			}
			partitions = append(partitions, FetchPartition{
				PartitionIndex:     partIdx,
				FetchOffset:        fetchOffset,
				CurrentLeaderEpoch: -1, // Fetch v0 has no epoch field
			})
		}

		// v0 uses topic name, not UUID — store name for lookup later
		ft := FetchTopic{
			TopicName:  topicName,
			Partitions: partitions,
		}
		topics = append(topics, ft)
	}

	return &FetchRequest{
		Header:    header,
		ReplicaID: replicaID,
		MaxWaitMs: maxWaitMs,
		MinBytes:  minBytes,
		Topics:    topics,
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
					currentLeaderEpoch, err := decoder.ReadInt32()
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
						PartitionIndex:     partitionIndex,
						FetchOffset:        fetchOffset,
						CurrentLeaderEpoch: currentLeaderEpoch,
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

// ParseMetadataRequest parses a Metadata request (v1–v12).
// The topic list may be empty (nil) meaning the client wants all topics.
func ParseMetadataRequest(header *RequestHeader) (*MetadataRequest, error) {
	decoder := NewDecoder(header.GetBody())

	// Flexible request header fields (same pattern as other v9+ APIs)
	// 1. cursor byte
	if _, err := decoder.ReadByte(); err != nil {
		return &MetadataRequest{Header: header}, nil
	}
	// 2. client software name (length-prefixed)
	lengthByte, err := decoder.ReadByte()
	if err != nil {
		return &MetadataRequest{Header: header}, nil
	}
	for i := 0; i < int(lengthByte); i++ {
		if _, err := decoder.ReadByte(); err != nil {
			return &MetadataRequest{Header: header}, nil
		}
	}
	// 3. TAG_BUFFER
	if _, err := decoder.ReadByte(); err != nil {
		return &MetadataRequest{Header: header}, nil
	}

	// topics COMPACT_ARRAY (null or empty = all topics)
	arrayLen, err := decoder.ReadUnsignedVarint()
	if err != nil || arrayLen == 0 {
		// null array → all topics
		return &MetadataRequest{Header: header, TopicNames: nil}, nil
	}

	var topicNames []string
	actualLen := arrayLen - 1
	for i := uint64(0); i < actualLen; i++ {
		name, err := decoder.ReadCompactString()
		if err != nil {
			break
		}
		topicNames = append(topicNames, name)
		decoder.ReadByte() // TAG_BUFFER per entry
	}

	return &MetadataRequest{Header: header, TopicNames: topicNames}, nil
}

// --- Parsers for consumer group / offset / list-offsets (v0 non-flexible) ---

// ParseFindCoordinatorRequest parses a FindCoordinator v0 request.
// ParseFindCoordinatorRequest parses a FindCoordinator v0 request.
func ParseFindCoordinatorRequest(header *RequestHeader) (*FindCoordinatorRequest, error) {
	if header.GetAPIVersion() != 0 {
		return nil, fmt.Errorf("FindCoordinator: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	key, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("FindCoordinator: read key: %w", err)
	}
	return &FindCoordinatorRequest{Header: header, Key: key}, nil
}

// ParseJoinGroupRequest parses a JoinGroup v0 request.
func ParseJoinGroupRequest(header *RequestHeader) (*JoinGroupRequest, error) {
	if header.GetAPIVersion() != 0 {
		return nil, fmt.Errorf("JoinGroup: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("JoinGroup: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("JoinGroup: group_id must not be empty")
	}
	sessionTimeout, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("JoinGroup: read session_timeout_ms: %w", err)
	}
	if sessionTimeout <= 0 {
		return nil, fmt.Errorf("JoinGroup: session_timeout_ms must be > 0")
	}
	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("JoinGroup: read member_id: %w", err)
	}
	protocolType, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("JoinGroup: read protocol_type: %w", err)
	}
	numProtocols, err := d.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("JoinGroup: read protocols array len: %w", err)
	}
	protocols := make([]GroupProtocol, 0, numProtocols)
	for i := int32(0); i < numProtocols; i++ {
		name, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("JoinGroup: read protocol name: %w", err)
		}
		meta, err := d.ReadBytes()
		if err != nil {
			return nil, fmt.Errorf("JoinGroup: read protocol metadata: %w", err)
		}
		protocols = append(protocols, GroupProtocol{Name: name, Metadata: meta})
	}
	if len(protocols) == 0 {
		return nil, fmt.Errorf("JoinGroup: at least one protocol is required")
	}
	return &JoinGroupRequest{
		Header:           header,
		GroupID:          groupID,
		SessionTimeoutMs: sessionTimeout,
		MemberID:         memberID,
		ProtocolType:     protocolType,
		Protocols:        protocols,
	}, nil
}

// ParseSyncGroupRequest parses a SyncGroup v0 request.
func ParseSyncGroupRequest(header *RequestHeader) (*SyncGroupRequest, error) {
	if header.GetAPIVersion() != 0 {
		return nil, fmt.Errorf("SyncGroup: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("SyncGroup: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("SyncGroup: group_id must not be empty")
	}
	generationID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("SyncGroup: read generation_id: %w", err)
	}
	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("SyncGroup: read member_id: %w", err)
	}
	numAssignments, err := d.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("SyncGroup: read assignments array len: %w", err)
	}
	assignments := make([]SyncGroupAssignment, 0, numAssignments)
	for i := int32(0); i < numAssignments; i++ {
		mid, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("SyncGroup: read assignment member_id: %w", err)
		}
		data, err := d.ReadBytes()
		if err != nil {
			return nil, fmt.Errorf("SyncGroup: read assignment data: %w", err)
		}
		assignments = append(assignments, SyncGroupAssignment{MemberID: mid, Assignment: data})
	}
	return &SyncGroupRequest{
		Header:       header,
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
		Assignments:  assignments,
	}, nil
}

// ParseHeartbeatRequest parses a Heartbeat v0 request.
func ParseHeartbeatRequest(header *RequestHeader) (*HeartbeatRequest, error) {
	if header.GetAPIVersion() != 0 {
		return nil, fmt.Errorf("Heartbeat: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("Heartbeat: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("Heartbeat: group_id must not be empty")
	}
	generationID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("Heartbeat: read generation_id: %w", err)
	}
	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("Heartbeat: read member_id: %w", err)
	}
	return &HeartbeatRequest{
		Header:       header,
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
	}, nil
}

// ParseLeaveGroupRequest parses a LeaveGroup v0 request.
func ParseLeaveGroupRequest(header *RequestHeader) (*LeaveGroupRequest, error) {
	if header.GetAPIVersion() != 0 {
		return nil, fmt.Errorf("LeaveGroup: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("LeaveGroup: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("LeaveGroup: group_id must not be empty")
	}
	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("LeaveGroup: read member_id: %w", err)
	}
	return &LeaveGroupRequest{
		Header:   header,
		GroupID:  groupID,
		MemberID: memberID,
	}, nil
}

// ParseOffsetCommitRequest parses an OffsetCommit v2 request.
func ParseOffsetCommitRequest(header *RequestHeader) (*OffsetCommitRequest, error) {
	if header.GetAPIVersion() != 2 {
		return nil, fmt.Errorf("OffsetCommit: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("OffsetCommit: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("OffsetCommit: group_id must not be empty")
	}
	generationID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("OffsetCommit: read generation_id: %w", err)
	}
	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("OffsetCommit: read member_id: %w", err)
	}
	retentionMs, err := d.ReadInt64()
	if err != nil {
		return nil, fmt.Errorf("OffsetCommit: read retention_time_ms: %w", err)
	}
	numTopics, err := d.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("OffsetCommit: read topics array len: %w", err)
	}
	topics := make([]OffsetCommitTopic, 0, numTopics)
	for i := int32(0); i < numTopics; i++ {
		name, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("OffsetCommit: read topic name: %w", err)
		}
		numParts, err := d.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("OffsetCommit: read partitions array len: %w", err)
		}
		parts := make([]OffsetCommitPartition, 0, numParts)
		for j := int32(0); j < numParts; j++ {
			idx, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("OffsetCommit: read partition index: %w", err)
			}
			offset, err := d.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("OffsetCommit: read committed_offset: %w", err)
			}
			meta, err := d.ReadString()
			if err != nil {
				return nil, fmt.Errorf("OffsetCommit: read metadata: %w", err)
			}
			parts = append(parts, OffsetCommitPartition{Index: idx, CommittedOffset: offset, Metadata: meta})
		}
		topics = append(topics, OffsetCommitTopic{Name: name, Partitions: parts})
	}
	return &OffsetCommitRequest{
		Header:       header,
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
		RetentionMs:  retentionMs,
		Topics:       topics,
	}, nil
}

// ParseOffsetFetchRequest parses an OffsetFetch v1 request.
func ParseOffsetFetchRequest(header *RequestHeader) (*OffsetFetchRequest, error) {
	if header.GetAPIVersion() != 1 {
		return nil, fmt.Errorf("OffsetFetch: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("OffsetFetch: read group_id: %w", err)
	}
	if groupID == "" {
		return nil, fmt.Errorf("OffsetFetch: group_id must not be empty")
	}
	numTopics, err := d.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("OffsetFetch: read topics array len: %w", err)
	}
	topics := make([]OffsetFetchTopic, 0, numTopics)
	for i := int32(0); i < numTopics; i++ {
		name, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("OffsetFetch: read topic name: %w", err)
		}
		numParts, err := d.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("OffsetFetch: read partitions array len: %w", err)
		}
		parts := make([]int32, 0, numParts)
		for j := int32(0); j < numParts; j++ {
			idx, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("OffsetFetch: read partition index: %w", err)
			}
			parts = append(parts, idx)
		}
		topics = append(topics, OffsetFetchTopic{Name: name, Partitions: parts})
	}
	return &OffsetFetchRequest{
		Header:  header,
		GroupID: groupID,
		Topics:  topics,
	}, nil
}

// ParseListOffsetsRequest parses a ListOffsets v1 request.
// ParseListOffsetsRequest parses a ListOffsets v1 request.
func ParseListOffsetsRequest(header *RequestHeader) (*ListOffsetsRequest, error) {
	if header.GetAPIVersion() != 1 {
		return nil, fmt.Errorf("ListOffsets: unsupported version %d", header.GetAPIVersion())
	}
	d := NewDecoder(header.GetBody())
	replicaID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("ListOffsets: read replica_id: %w", err)
	}
	numTopics, err := d.ReadArrayLen()
	if err != nil {
		return nil, fmt.Errorf("ListOffsets: read topics array len: %w", err)
	}
	topics := make([]ListOffsetsTopic, 0, numTopics)
	for i := int32(0); i < numTopics; i++ {
		name, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("ListOffsets: read topic name: %w", err)
		}
		numParts, err := d.ReadArrayLen()
		if err != nil {
			return nil, fmt.Errorf("ListOffsets: read partitions array len: %w", err)
		}
		parts := make([]ListOffsetsPartition, 0, numParts)
		for j := int32(0); j < numParts; j++ {
			idx, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("ListOffsets: read partition index: %w", err)
			}
			ts, err := d.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("ListOffsets: read timestamp: %w", err)
			}
			parts = append(parts, ListOffsetsPartition{Index: idx, Timestamp: ts})
		}
		topics = append(topics, ListOffsetsTopic{Name: name, Partitions: parts})
	}
	return &ListOffsetsRequest{
		Header:    header,
		ReplicaID: replicaID,
		Topics:    topics,
	}, nil
}
