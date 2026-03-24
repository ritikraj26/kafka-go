package apiversions

import (
	"bytes"
	"encoding/binary"
)

// supported API and its version range
type APIVersion struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

// return the list of APIs supported by this broker
func getSupportedAPIs() []APIVersion {
	return []APIVersion{
		{18, 0, 4}, // ApiVersions
		{75, 0, 0}, // DescribeTopicPartitions
		// TODO: Add more APIs as they are implemented
		// {0, 0, 9},  // Produce
		// {1, 0, 13}, // Fetch
	}
}

func BuildBody(errorCode int16) []byte {
	buf := new(bytes.Buffer)

	// error_code
	binary.Write(buf, binary.BigEndian, errorCode)

	supportedAPIs := getSupportedAPIs()

	// COMPACT_ARRAY length: N+1 (so 1 entry = 0x02, 2 entries = 0x03, etc.)
	buf.WriteByte(byte(len(supportedAPIs) + 1))

	// Write each API entry
	for _, api := range supportedAPIs {
		binary.Write(buf, binary.BigEndian, api.Key)        // api_key
		binary.Write(buf, binary.BigEndian, api.MinVersion) // min_version
		binary.Write(buf, binary.BigEndian, api.MaxVersion) // max_version
		buf.WriteByte(0x00)                                 // TAG_BUFFER for entry
	}

	// throttle_time_ms
	binary.Write(buf, binary.BigEndian, int32(0))

	// TAG_BUFFER for response
	buf.WriteByte(0x00)

	return buf.Bytes()
}
