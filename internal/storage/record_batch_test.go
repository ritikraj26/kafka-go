package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"testing"
)

// This fixture is assembled independently of EncodeRecordBatch.
func fixtureBatch() []byte {
	header := make([]byte, 61)
	header[16] = 2
	binary.BigEndian.PutUint32(header[57:61], 1)
	record := []byte{14, 0, 0, 0, 1, 2, 'a', 0}
	return sealBatch(append(header, record...))
}

func sealBatch(data []byte) []byte {
	binary.BigEndian.PutUint32(data[8:12], uint32(len(data)-12))
	binary.BigEndian.PutUint32(data[17:21], crc32.Checksum(data[21:], crc32.MakeTable(crc32.Castagnoli)))
	return data
}

func TestParseRecordBatches(t *testing.T) {
	first := fixtureBatch()
	second := bytes.Clone(first)
	binary.BigEndian.PutUint64(second[:8], 1)
	batches, err := ParseRecordBatches(append(first, second...))
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want 2", len(batches))
	}
	for i, batch := range batches {
		if batch.BaseOffset != int64(i) || batch.RecordsCount != 1 || batch.LastOffsetDelta != 0 ||
			len(batch.Values) != 1 || string(batch.Values[0]) != "a" {
			t.Fatalf("unexpected batch %d: %+v", i, batch)
		}
	}
}

func TestParseRecordBatchesRejectsMalformedData(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   error
	}{
		{"empty", func([]byte) []byte { return nil }, ErrCorruptRecord},
		{"short header", func(b []byte) []byte { return b[:11] }, ErrCorruptRecord},
		{"short batch", func(b []byte) []byte { return b[:len(b)-1] }, ErrCorruptRecord},
		{"negative batch length", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], math.MaxUint32); return b }, ErrCorruptRecord},
		{"oversized batch", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], MaxRecordBatchBytes); return b }, ErrCorruptRecord},
		{"bad CRC", func(b []byte) []byte { b[67] ^= 1; return b }, ErrCorruptRecord},
		{"old magic", func(b []byte) []byte { b[16] = 1; return b }, ErrUnsupportedFormat},
		{"compression", func(b []byte) []byte { b[22] = 1; return sealBatch(b) }, ErrUnsupportedCompression},
		{"transaction", func(b []byte) []byte { b[22] = 16; return sealBatch(b) }, ErrUnsupportedFormat},
		{"control", func(b []byte) []byte { b[22] = 32; return sealBatch(b) }, ErrUnsupportedFormat},
		{"zero records", func(b []byte) []byte { binary.BigEndian.PutUint32(b[57:61], 0); return sealBatch(b) }, ErrCorruptRecord},
		{"negative count", func(b []byte) []byte { binary.BigEndian.PutUint32(b[57:61], math.MaxUint32); return sealBatch(b) }, ErrCorruptRecord},
		{"last offset delta", func(b []byte) []byte { binary.BigEndian.PutUint32(b[23:27], 1); return sealBatch(b) }, ErrCorruptRecord},
		{"record length", func(b []byte) []byte { b[61] = 0; return sealBatch(b) }, ErrCorruptRecord},
		{"record attributes", func(b []byte) []byte { b[62] = 1; return sealBatch(b) }, ErrCorruptRecord},
		{"record offset delta", func(b []byte) []byte { b[64] = 2; return sealBatch(b) }, ErrCorruptRecord},
		{"negative key length", func(b []byte) []byte { b[65] = 3; return sealBatch(b) }, ErrCorruptRecord},
		{"value length", func(b []byte) []byte { b[66] = 100; return sealBatch(b) }, ErrCorruptRecord},
		{"negative headers", func(b []byte) []byte { b[68] = 1; return sealBatch(b) }, ErrCorruptRecord},
		{"missing headers", func(b []byte) []byte { b[68] = 2; return sealBatch(b) }, ErrCorruptRecord},
		{"trailing bytes", func(b []byte) []byte { return sealBatch(append(b, 0)) }, ErrCorruptRecord},
		{"partial next batch", func(b []byte) []byte { return append(b, 0) }, ErrCorruptRecord},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches, err := ParseRecordBatches(tt.mutate(fixtureBatch()))
			if !errors.Is(err, tt.want) || batches != nil {
				t.Fatalf("batches=%v err=%v, want nil/%v", batches, err, tt.want)
			}
		})
	}
}

func TestParseRecordHeaders(t *testing.T) {
	batch := fixtureBatch()
	batch[61] = 22
	batch[68] = 2
	batch = sealBatch(append(batch, 2, 'k', 2, 'v'))
	if _, err := ParseRecordBatches(batch); err != nil {
		t.Fatal(err)
	}
	batch[69] = 1 // null header keys are invalid
	if _, err := ParseRecordBatches(sealBatch(batch)); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("null header key: %v", err)
	}
}

func TestEncodeRecordBatch(t *testing.T) {
	records := []Record{
		{Key: []byte("key"), Value: []byte("first")},
		{Value: []byte("second")},
		{Value: nil},
		{Key: []byte{}, Value: []byte{}},
	}
	data, err := EncodeRecordBatch(42, records)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := ParseRecordBatches(data)
	if err != nil {
		t.Fatal(err)
	}
	batch := batches[0]
	if batch.BaseOffset != 42 || batch.RecordsCount != 4 || batch.LastOffsetDelta != 3 ||
		len(batch.Values) != 2 || string(batch.Values[0]) != "first" || string(batch.Values[1]) != "second" {
		t.Fatalf("unexpected batch: %+v", batch)
	}
	for _, offset := range []int64{-1, math.MaxInt64} {
		if _, err := EncodeRecordBatch(offset, records); !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("baseOffset=%d: %v", offset, err)
		}
	}
	if _, err := EncodeRecordBatch(0, nil); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("empty batch: %v", err)
	}
}

func TestReadRecordBatch(t *testing.T) {
	data := fixtureBatch()
	r := bytes.NewReader(append(bytes.Clone(data), data...))
	for i := 0; i < 2; i++ {
		batch, err := ReadRecordBatch(r)
		if err != nil || !bytes.Equal(batch.Data, data) {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	if _, err := ReadRecordBatch(r); err != io.EOF {
		t.Fatalf("clean boundary: %v", err)
	}
	for size := 1; size < len(data); size++ {
		if _, err := ReadRecordBatch(bytes.NewReader(data[:size])); !errors.Is(err, ErrCorruptRecord) {
			t.Fatalf("truncation at %d: %v", size, err)
		}
	}
	binary.BigEndian.PutUint32(data[8:12], math.MaxInt32)
	if _, err := ReadRecordBatch(bytes.NewReader(data[:12])); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("oversized recovery allocation: %v", err)
	}
}

func TestReadVarintRejectsOverflow(t *testing.T) {
	for _, input := range [][]byte{{0x80}, bytes.Repeat([]byte{0xff}, 11), binary.AppendVarint(nil, math.MaxInt64)} {
		if _, err := readVarint(&input, 32); !errors.Is(err, ErrCorruptRecord) {
			t.Fatalf("expected invalid varint, got %v", err)
		}
	}
}

func FuzzParseRecordBatches(f *testing.F) {
	f.Add(fixtureBatch())
	f.Add([]byte{})
	f.Add(append(fixtureBatch(), fixtureBatch()...))
	f.Fuzz(func(t *testing.T, data []byte) {
		batches, err := ParseRecordBatches(data)
		if err != nil {
			if batches != nil {
				t.Fatal("partially validated records escaped on error")
			}
			return
		}
		total := 0
		for _, batch := range batches {
			total += len(batch.Data)
			if batch.RecordsCount <= 0 || int64(batch.LastOffsetDelta)+1 != int64(batch.RecordsCount) {
				t.Fatal("invalid offset/count invariant")
			}
		}
		if total != len(data) {
			t.Fatal("unparsed trailing data")
		}
	})
}
