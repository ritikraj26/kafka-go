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

func (e *Encoder) WriteByte(val byte) {
	e.buf.WriteByte(val)
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
