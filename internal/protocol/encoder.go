package protocol

import (
	"bytes"
	"encoding/binary"
)

// Encoder provides BigEndian binary write helpers for building Kafka response buffers.
type Encoder struct {
	buf *bytes.Buffer
}

func NewEncoder() *Encoder {
	return &Encoder{
		buf: new(bytes.Buffer),
	}
}

func (e *Encoder) WriteInt16(val int16) {
	binary.Write(e.buf, binary.BigEndian, val)
}

func (e *Encoder) WriteInt32(val int32) {
	binary.Write(e.buf, binary.BigEndian, val)
}

func (e *Encoder) WriteInt64(val int64) {
	binary.Write(e.buf, binary.BigEndian, val)
}

func (e *Encoder) WriteByte(val byte) error {
	return e.buf.WriteByte(val)
}

// WriteCompactString writes a COMPACT_STRING (length+1 as varint, then UTF-8 bytes)
func (e *Encoder) WriteCompactString(s string) {
	if s == "" {
		e.WriteUnsignedVarint(1) // empty string is length 1 (0+1)
		return
	}
	e.WriteUnsignedVarint(uint64(len(s) + 1))
	e.buf.WriteString(s)
}

// WriteString writes a standard STRING (INT16 length-prefixed, then UTF-8 bytes).
// Used by v0 non-flexible APIs.
func (e *Encoder) WriteString(s string) {
	binary.Write(e.buf, binary.BigEndian, int16(len(s)))
	e.buf.WriteString(s)
}

// WriteNullableString writes a NULLABLE_STRING (INT16 length, -1 for null).
func (e *Encoder) WriteNullableString(s *string) {
	if s == nil {
		binary.Write(e.buf, binary.BigEndian, int16(-1))
		return
	}
	e.WriteString(*s)
}

// WriteBytes writes standard BYTES (INT32 length-prefixed, then raw bytes).
// Used by v0 non-flexible APIs.
func (e *Encoder) WriteBytes(data []byte) {
	if data == nil {
		binary.Write(e.buf, binary.BigEndian, int32(-1))
		return
	}
	binary.Write(e.buf, binary.BigEndian, int32(len(data)))
	e.buf.Write(data)
}

// WriteArrayLen writes the INT32 count prefix for a standard (non-compact) array.
func (e *Encoder) WriteArrayLen(n int) {
	binary.Write(e.buf, binary.BigEndian, int32(n))
}

// WriteCompactBytes writes COMPACT_BYTES (length+1 as varint, then raw bytes)
func (e *Encoder) WriteCompactBytes(data []byte) {
	e.WriteUnsignedVarint(uint64(len(data) + 1))
	e.buf.Write(data)
}

// WriteUnsignedVarint writes an unsigned varint
func (e *Encoder) WriteUnsignedVarint(value uint64) {
	for value >= 0x80 {
		e.buf.WriteByte(byte(value) | 0x80)
		value >>= 7
	}
	e.buf.WriteByte(byte(value))
}

// WriteUUID writes a 16-byte UUID
func (e *Encoder) WriteUUID(uuid [16]byte) {
	e.buf.Write(uuid[:])
}

// WriteTagBuffer writes an empty TAG_BUFFER (0x00)
func (e *Encoder) WriteTagBuffer() {
	e.buf.WriteByte(0x00)
}

// Bytes returns the encoded bytes
func (e *Encoder) Bytes() []byte {
	return e.buf.Bytes()
}
