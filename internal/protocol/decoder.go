package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
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
	if strLen > uint64(d.Remaining()) {
		return "", io.ErrUnexpectedEOF
	}

	str := string(d.data[d.pos : d.pos+int(strLen)])
	d.pos += int(strLen)
	return str, nil
}

// ReadString reads a standard STRING (INT16 length-prefixed, then UTF-8 bytes).
// Used by v0 non-flexible APIs.
func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadInt16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil // null string
	}
	if d.pos+int(length) > len(d.data) {
		return "", io.ErrUnexpectedEOF
	}
	str := string(d.data[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return str, nil
}

// ReadBytes reads standard BYTES (INT32 length-prefixed, then raw bytes).
// Used by v0 non-flexible APIs.
func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil // null bytes
	}
	if d.pos+int(length) > len(d.data) {
		return nil, io.ErrUnexpectedEOF
	}
	result := make([]byte, length)
	copy(result, d.data[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return result, nil
}

// ReadArrayLen reads an INT32 array count for standard (non-compact) arrays.
func (d *Decoder) ReadArrayLen() (int32, error) {
	return d.ReadInt32()
}

// ReadCompactNullableString reads a COMPACT_NULLABLE_STRING
// Returns nil pointer for null, otherwise pointer to string
func (d *Decoder) ReadCompactNullableString() (*string, error) {
	length, err := d.ReadUnsignedVarint()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil // null string
	}

	strLen := length - 1 // compact encoding is length+1
	if strLen > uint64(d.Remaining()) {
		return nil, io.ErrUnexpectedEOF
	}

	str := string(d.data[d.pos : d.pos+int(strLen)])
	d.pos += int(strLen)
	return &str, nil
}

// ReadCompactBytes reads COMPACT_BYTES (length as unsigned varint, then bytes)
// Returns nil for null (length=0)
func (d *Decoder) ReadCompactBytes() ([]byte, error) {
	length, err := d.ReadUnsignedVarint()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil // null bytes
	}

	bytesLen := length - 1 // compact encoding is length+1
	if bytesLen > uint64(d.Remaining()) {
		return nil, io.ErrUnexpectedEOF
	}

	bytes := make([]byte, bytesLen)
	copy(bytes, d.data[d.pos:d.pos+int(bytesLen)])
	d.pos += int(bytesLen)
	return bytes, nil
}

// ReadUnsignedVarint reads an unsigned varint
func (d *Decoder) ReadUnsignedVarint() (uint64, error) {
	value, n := binary.Uvarint(d.data[d.pos:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, fmt.Errorf("unsigned varint overflow")
	}
	d.pos += n
	return value, nil
}

// ReadTaggedFields validates the tag envelope and skips unknown fields without
// allocating. A visitor can decode known tags from their bounded byte slices.
func (d *Decoder) ReadTaggedFields(visit func(uint32, []byte) error) error {
	count, err := d.ReadUnsignedVarint()
	if err != nil {
		return err
	}
	if count > uint64(d.Remaining()/2) {
		return fmt.Errorf("tag count exceeds remaining bytes")
	}
	var previous uint64
	for i := uint64(0); i < count; i++ {
		tag, err := d.ReadUnsignedVarint()
		if err != nil {
			return err
		}
		if tag > math.MaxUint32 || (i > 0 && tag <= previous) {
			return fmt.Errorf("invalid or unordered tag ID")
		}
		size, err := d.ReadUnsignedVarint()
		if err != nil {
			return err
		}
		if size > math.MaxUint32 || size > uint64(d.Remaining()) {
			return io.ErrUnexpectedEOF
		}
		data := d.data[d.pos : d.pos+int(size)]
		d.pos += int(size)
		if visit != nil {
			if err := visit(uint32(tag), data); err != nil {
				return err
			}
		}
		previous = tag
	}
	return nil
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
