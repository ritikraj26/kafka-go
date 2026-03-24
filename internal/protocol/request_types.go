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

// placeholder 
func ParseDescribeTopicPartitionsRequest(header *RequestHeader) (*DescribeTopicPartitionsRequest, error) {
	// TODO: implement when DescribeTopicPartitions API is needed
	return &DescribeTopicPartitionsRequest{
		Header: header,
	}, nil
}
