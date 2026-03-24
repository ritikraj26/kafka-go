package apiversions

import (
	"bytes"
	"encoding/binary"
)

func BuildBody(errorCode int16) []byte {
	buf := new(bytes.Buffer)

	// error_code
	binary.Write(buf, binary.BigEndian, errorCode)

	// COMPACT_ARRAY length: N+1, so 1 entry = 0x02
	buf.WriteByte(0x02)

	// Entry: ApiVersions (key=18), versions 0-4
	binary.Write(buf, binary.BigEndian, int16(18)) // api_key
	binary.Write(buf, binary.BigEndian, int16(0))  // min_version
	binary.Write(buf, binary.BigEndian, int16(4))  // max_version
	buf.WriteByte(0x00)                            // TAG_BUFFER for entry

	// throttle_time_ms
	binary.Write(buf, binary.BigEndian, int32(0))

	// TAG_BUFFER for response
	buf.WriteByte(0x00)

	return buf.Bytes()
}
