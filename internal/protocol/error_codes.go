package protocol

// Kafka error codes returned in response headers and bodies.
const (
	ErrNone                         int16 = 0
	ErrOffsetOutOfRange             int16 = 1
	ErrUnknownServerError           int16 = -1
	ErrCorruptMessage               int16 = 2
	ErrUnknownTopicOrPartition      int16 = 3
	ErrNotLeaderOrFollower          int16 = 6
	ErrRequestTimedOut              int16 = 7
	ErrCoordinatorNotAvailable      int16 = 15
	ErrNotCoordinator               int16 = 16
	ErrInvalidRequiredAcks          int16 = 21
	ErrIllegalGeneration            int16 = 22
	ErrUnknownMemberID              int16 = 25
	ErrRebalanceInProgress          int16 = 27
	ErrNotEnoughReplicas            int16 = 19
	ErrNotEnoughReplicasAfterAppend int16 = 20
	ErrUnsupportedVersion           int16 = 35
	ErrInvalidRequest               int16 = 42
	ErrUnsupportedForMessageFormat  int16 = 43
	ErrUnsupportedCompressionType   int16 = 76
	ErrFetchSessionIDNotFound       int16 = 70
	ErrMemberIDRequired             int16 = 79
	ErrGroupMaxSizeReached          int16 = 81
	ErrUnknownTopicID               int16 = 100
	ErrFencedLeaderEpoch            int16 = 74
	ErrUnknownLeaderEpoch           int16 = 75
)
