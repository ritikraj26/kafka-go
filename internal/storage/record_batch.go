package storage

import "encoding/binary"

// RecordBatch fixed header layout (61 bytes total before records):
//
//	baseOffset        INT64   8
//	batchLength       INT32   4
//	partitionEpoch    INT32   4
//	magic             INT8    1
//	crc               UINT32  4
//	attributes        INT16   2
//	lastOffsetDelta   INT32   4
//	baseTimestamp     INT64   8
//	maxTimestamp      INT64   8
//	producerId        INT64   8
//	producerEpoch     INT16   2
//	baseSequence      INT32   4
//	recordsCount      INT32   4
const recordBatchHeaderSize = 61

// ParseRecordValues extracts the raw value bytes from every Record inside a
// single RecordBatch byte slice (as received in a Produce request).
// Malformed data is silently skipped — the caller must not rely on completeness.
func ParseRecordValues(data []byte) [][]byte {
	if len(data) < recordBatchHeaderSize {
		return nil
	}

	recordsCount := int(int32(binary.BigEndian.Uint32(data[57:61])))
	if recordsCount <= 0 {
		return nil
	}

	var values [][]byte
	pos := recordBatchHeaderSize

	for i := 0; i < recordsCount; i++ {
		if pos >= len(data) {
			break
		}

		// Each Record field is a signed zigzag varint unless noted.
		// length (varint) — total byte length of the record after this field
		recLen, n := readSignedVarint(data, pos)
		if n == 0 || recLen <= 0 {
			break
		}
		pos += n
		recEnd := pos + int(recLen)
		if recEnd > len(data) {
			break
		}

		// attributes (INT8)
		pos++

		// timestampDelta (varint)
		_, n = readSignedVarint(data, pos)
		if n == 0 {
			pos = recEnd
			continue
		}
		pos += n

		// offsetDelta (varint)
		_, n = readSignedVarint(data, pos)
		if n == 0 {
			pos = recEnd
			continue
		}
		pos += n

		// keyLength (varint) — -1 means null key
		keyLen, n := readSignedVarint(data, pos)
		if n == 0 {
			pos = recEnd
			continue
		}
		pos += n
		if keyLen > 0 {
			pos += int(keyLen)
		}

		// valueLength (varint)
		valLen, n := readSignedVarint(data, pos)
		if n == 0 {
			pos = recEnd
			continue
		}
		pos += n

		if valLen > 0 && pos+int(valLen) <= recEnd {
			values = append(values, data[pos:pos+int(valLen)])
		}

		pos = recEnd
	}

	return values
}

// readSignedVarint reads a zigzag-encoded signed varint from data[pos:].
// Returns the decoded int64 and the number of bytes consumed (0 on error).
func readSignedVarint(data []byte, pos int) (int64, int) {
	uv, n := readUnsignedVarint(data, pos)
	// zigzag decode
	return int64((uv >> 1) ^ -(uv & 1)), n
}

// readUnsignedVarint reads an unsigned varint from data[pos:].
func readUnsignedVarint(data []byte, pos int) (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if pos+i >= len(data) {
			return 0, 0
		}
		b := data[pos+i]
		x |= uint64(b&0x7f) << s
		s += 7
		if b < 0x80 {
			return x, i + 1
		}
	}
	return 0, 0
}
