package protocol

// API keys for the Kafka protocol requests implemented in this broker.
const (
	APIKeyProduce                 int16 = 0
	APIKeyFetch                   int16 = 1
	APIKeyListOffsets             int16 = 2
	APIKeyMetadata                int16 = 3
	APIKeyOffsetCommit            int16 = 8
	APIKeyOffsetFetch             int16 = 9
	APIKeyFindCoordinator         int16 = 10
	APIKeyJoinGroup               int16 = 11
	APIKeyHeartbeat               int16 = 12
	APIKeyLeaveGroup              int16 = 13
	APIKeySyncGroup               int16 = 14
	APIKeyApiVersions             int16 = 18
	APIKeyDescribeTopicPartitions int16 = 75
)
