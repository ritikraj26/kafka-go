package protocol

import (
	"encoding/binary"
	"io"
)

// Decoder provides BigEndian binary read helpers for parsing Kafka request buffers.
type Decoder struct {
	data []byte
	pos  int
}

func NewDecoder(data []byte) *Decoder {
	return &Decoder{
		data: data,
		pos:  0,
	}
}

// ReadCompactString reads a COMPACT_STRING (length as unsigned varint, then UTF-8 bytes)
// Compact strings use length+1 encoding where 0 means null
func (d *Decoder) ReadCompactString() (string, error) {
	length, err := d.ReadUnsignedVarint()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil // null string
	}

	strLen := length - 1 // compact encoding is length+1
	if d.pos+int(strLen) > len(d.data) {
		return "", io.ErrUnexpectedEOF
	}

	str := string(d.data[d.pos : d.pos+int(strLen)])
	d.pos += int(strLen)
	return str, nil
}

// ReadUnsignedVarint reads an unsigned varint
func (d *Decoder) ReadUnsignedVarint() (uint64, error) {
	var value uint64
	var shift uint

	for {
		if d.pos >= len(d.data) {
			return 0, io.ErrUnexpectedEOF
		}

		b := d.data[d.pos]
		d.pos++

		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	return value, nil
}

// ReadInt16 reads a 16-bit integer
func (d *Decoder) ReadInt16() (int16, error) {
	if d.pos+2 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int16(binary.BigEndian.Uint16(d.data[d.pos : d.pos+2]))
	d.pos += 2
	return val, nil
}

// ReadInt32 reads a 32-bit integer
func (d *Decoder) ReadInt32() (int32, error) {
	if d.pos+4 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int32(binary.BigEndian.Uint32(d.data[d.pos : d.pos+4]))
	d.pos += 4
	return val, nil
}

// ReadInt64 reads a 64-bit integer
func (d *Decoder) ReadInt64() (int64, error) {
	if d.pos+8 > len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := int64(binary.BigEndian.Uint64(d.data[d.pos : d.pos+8]))
	d.pos += 8
	return val, nil
}

// ReadByte reads a single byte
func (d *Decoder) ReadByte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	b := d.data[d.pos]
	d.pos++
	return b, nil
}

// Remaining returns the number of unread bytes
func (d *Decoder) Remaining() int {
	return len(d.data) - d.pos
}

// RemainingBytes returns the unread bytes
func (d *Decoder) RemainingBytes() []byte {
	if d.pos >= len(d.data) {
		return nil
	}
	return d.data[d.pos:]
}
