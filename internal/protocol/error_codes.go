package protocol

// Kafka error codes returned in response headers and bodies.
const (
	ErrNone                    int16 = 0
	ErrUnknownServerError      int16 = 1
	ErrUnknownTopicOrPartition int16 = 3
	ErrNotLeaderOrFollower     int16 = 6
	ErrCoordinatorNotAvailable int16 = 15
	ErrNotCoordinator          int16 = 16
	ErrIllegalGeneration       int16 = 22
	ErrUnknownMemberID         int16 = 25
	ErrRebalanceInProgress     int16 = 27
	ErrUnsupportedVersion      int16 = 35
	ErrInvalidRequest          int16 = 42
	ErrMemberIDRequired        int16 = 79
	ErrGroupMaxSizeReached     int16 = 81
	ErrUnknownTopicID          int16 = 100
)
