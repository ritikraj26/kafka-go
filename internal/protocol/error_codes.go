package protocol

// Kafka error codes returned in response headers and bodies.
// TODO: implement
const (
	ErrNone                    int16 = 0
	ErrUnknownTopicOrPartition int16 = 3
	ErrUnsupportedVersion      int16 = 35
	ErrUnknownTopicID          int16 = 100
)
