package storage

import (
	"encoding/binary"
	"testing"
)

// buildRecordBatch builds a minimal but valid RecordBatch containing one record with
// the given value. Offsets/timestamps/CRC are zero — sufficient for ParseRecordValues.
func buildRecordBatch(value []byte) []byte {
	// We'll build the record first, then the header.
	record := buildRecord(value)
	// RecordBatch header (61 bytes)
	hdr := make([]byte, recordBatchHeaderSize)
	// baseOffset = 0 (bytes 0-7 are already 0)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(hdr)-12+len(record))) // batchLength
	// magic = 2 (byte 16)
	hdr[16] = 2
	// recordsCount (bytes 57-60)
	binary.BigEndian.PutUint32(hdr[57:61], 1)
	var batch []byte
	batch = append(batch, hdr...)
	batch = append(batch, record...)
	return batch
}

// buildRecord returns a single Record with a null key and the given value,
// using zigzag varint encoding as required by the RecordBatch format.
func buildRecord(value []byte) []byte {
	var rec []byte
	rec = append(rec, 0)                                        // attributes (INT8)
	rec = append(rec, encodeSignedVarint(0)...)                 // timestampDelta (varint) = 0
	rec = append(rec, encodeSignedVarint(0)...)                 // offsetDelta (varint) = 0
	rec = append(rec, encodeSignedVarint(-1)...)                // keyLength (varint) = -1 (null key)
	rec = append(rec, encodeSignedVarint(int64(len(value)))...) // valueLength (varint)
	rec = append(rec, value...)                                 // value bytes
	rec = append(rec, encodeSignedVarint(0)...)                 // headers count (varint) = 0
	// Prepend record length (varint of the remaining bytes)
	length := encodeSignedVarint(int64(len(rec)))
	return append(length, rec...)
}

func encodeSignedVarint(v int64) []byte {
	// zigzag encode
	uv := uint64((v << 1) ^ (v >> 63))
	var buf [10]byte
	n := 0
	for uv >= 0x80 {
		buf[n] = byte(uv) | 0x80
		uv >>= 7
		n++
	}
	buf[n] = byte(uv)
	return buf[:n+1]
}

func TestParseRecordValues_SingleRecord(t *testing.T) {
	want := []byte(`{"msg":"hello"}`)
	batch := buildRecordBatch(want)
	values := ParseRecordValues(batch)
	if len(values) != 1 {
		t.Fatalf("got %d values, want 1", len(values))
	}
	if string(values[0]) != string(want) {
		t.Errorf("value = %q, want %q", values[0], want)
	}
}

func TestParseRecordValues_TooShort(t *testing.T) {
	values := ParseRecordValues([]byte{0x00, 0x01})
	if values != nil {
		t.Errorf("expected nil for too-short input, got %v", values)
	}
}

func TestParseRecordValues_Empty(t *testing.T) {
	values := ParseRecordValues(nil)
	if values != nil {
		t.Errorf("expected nil for nil input, got %v", values)
	}
}

func TestParseRecordValues_ZeroRecords(t *testing.T) {
	hdr := make([]byte, recordBatchHeaderSize)
	// recordsCount deliberately left at 0
	values := ParseRecordValues(hdr)
	if values != nil {
		t.Errorf("expected nil for zero-record batch, got %v", values)
	}
}

func TestParseRecordValues_MultipleRecords(t *testing.T) {
	// Build a batch with 3 records manually
	r1 := buildRecord([]byte("aaa"))
	r2 := buildRecord([]byte("bb"))
	r3 := buildRecord([]byte("c"))
	records := append(append(r1, r2...), r3...)
	hdr := make([]byte, recordBatchHeaderSize)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(hdr)-12+len(records)))
	hdr[16] = 2
	binary.BigEndian.PutUint32(hdr[57:61], 3) // recordsCount = 3
	batch := append(hdr, records...)
	values := ParseRecordValues(batch)
	if len(values) != 3 {
		t.Fatalf("got %d values, want 3", len(values))
	}
	if string(values[0]) != "aaa" || string(values[1]) != "bb" || string(values[2]) != "c" {
		t.Errorf("unexpected values: %v", values)
	}
}
