package protocol

// API keys for the Kafka protocol requests implemented in this broker.
// TODO: implement
const (
	APIKeyProduce                 int16 = 0
	APIKeyFetch                   int16 = 1
	APIKeyMetadata                int16 = 3
	APIKeyApiVersions             int16 = 18
	APIKeyDescribeTopicPartitions int16 = 75
)
