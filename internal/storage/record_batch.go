package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

const recordBatchHeaderSize = 61

// MaxRecordBatchBytes bounds both incoming batches and recovery allocations.
const MaxRecordBatchBytes = 100 * 1024 * 1024

var (
	ErrCorruptRecord          = errors.New("corrupt record batch")
	ErrUnsupportedCompression = errors.New("unsupported compression")
	ErrUnsupportedFormat      = errors.New("unsupported record format")
	crcTable                  = crc32.MakeTable(crc32.Castagnoli)
)

// RecordBatch references a validated, uncompressed magic-v2 batch.
// Data and Values alias the input and must not be modified while being read.
type RecordBatch struct {
	Data            []byte
	BaseOffset      int64
	LastOffsetDelta int32
	RecordsCount    int32
	Values          [][]byte
}

// ParseRecordBatches validates the entire record set before returning any batches.
// Transactional, control and compressed batches are not supported.
func ParseRecordBatches(data []byte) ([]RecordBatch, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty record set", ErrCorruptRecord)
	}
	var batches []RecordBatch
	for len(data) > 0 {
		size, err := batchSize(data)
		if err != nil {
			return nil, err
		}
		if size > len(data) {
			return nil, fmt.Errorf("%w: incomplete batch", ErrCorruptRecord)
		}
		batch, err := parseBatch(data[:size])
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
		data = data[size:]
	}
	return batches, nil
}

// ReadRecordBatch reads one complete batch, returning io.EOF only at a clean boundary.
func ReadRecordBatch(r io.Reader) (RecordBatch, error) {
	var prefix [12]byte
	n, err := io.ReadFull(r, prefix[:])
	if err == io.EOF && n == 0 {
		return RecordBatch{}, io.EOF
	}
	if err != nil {
		return RecordBatch{}, fmt.Errorf("%w: batch header: %w", ErrCorruptRecord, err)
	}
	size, err := batchSize(prefix[:])
	if err != nil {
		return RecordBatch{}, err
	}
	data := make([]byte, size)
	copy(data, prefix[:])
	if _, err := io.ReadFull(r, data[12:]); err != nil {
		return RecordBatch{}, fmt.Errorf("%w: batch body: %w", ErrCorruptRecord, err)
	}
	return parseBatch(data)
}

func batchSize(data []byte) (int, error) {
	if len(data) < 12 {
		return 0, fmt.Errorf("%w: incomplete batch header", ErrCorruptRecord)
	}
	length := int32(binary.BigEndian.Uint32(data[8:12]))
	if length < recordBatchHeaderSize-12 || length > MaxRecordBatchBytes-12 {
		return 0, fmt.Errorf("%w: invalid batch length %d", ErrCorruptRecord, length)
	}
	return int(length) + 12, nil
}

func parseBatch(data []byte) (RecordBatch, error) {
	if data[16] != 2 {
		return RecordBatch{}, fmt.Errorf("%w: magic %d", ErrUnsupportedFormat, data[16])
	}
	if got := crc32.Checksum(data[21:], crcTable); got != binary.BigEndian.Uint32(data[17:21]) {
		return RecordBatch{}, fmt.Errorf("%w: CRC32C mismatch", ErrCorruptRecord)
	}
	attributes := binary.BigEndian.Uint16(data[21:23])
	if attributes&7 != 0 {
		return RecordBatch{}, fmt.Errorf("%w: codec %d", ErrUnsupportedCompression, attributes&7)
	}
	if attributes & ^uint16(8) != 0 {
		return RecordBatch{}, fmt.Errorf("%w: attributes %#x", ErrUnsupportedFormat, attributes)
	}
	batch := RecordBatch{
		Data:            data,
		BaseOffset:      int64(binary.BigEndian.Uint64(data[:8])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(data[23:27])),
		RecordsCount:    int32(binary.BigEndian.Uint32(data[57:61])),
	}
	if batch.RecordsCount <= 0 || batch.LastOffsetDelta != batch.RecordsCount-1 {
		return RecordBatch{}, fmt.Errorf("%w: inconsistent record count and offset delta", ErrCorruptRecord)
	}
	remaining := data[recordBatchHeaderSize:]
	for i := int32(0); i < batch.RecordsCount; i++ {
		value, err := readRecord(&remaining, i)
		if err != nil {
			return RecordBatch{}, fmt.Errorf("record %d: %w", i, err)
		}
		// Preserve schema validation's existing treatment of null/empty values.
		if len(value) > 0 {
			batch.Values = append(batch.Values, value)
		}
	}
	if len(remaining) != 0 {
		return RecordBatch{}, fmt.Errorf("%w: bytes after last record", ErrCorruptRecord)
	}
	return batch, nil
}

func readRecord(data *[]byte, expectedDelta int32) ([]byte, error) {
	length, err := readVarint(data, 32)
	if err != nil {
		return nil, err
	}
	if length <= 0 || length > int64(len(*data)) {
		return nil, fmt.Errorf("%w: invalid record length", ErrCorruptRecord)
	}
	record := (*data)[:int(length)]
	*data = (*data)[int(length):]
	if record[0] != 0 {
		return nil, fmt.Errorf("%w: nonzero record attributes", ErrCorruptRecord)
	}
	record = record[1:]
	if _, err := readVarint(&record, 64); err != nil {
		return nil, err
	}
	delta, err := readVarint(&record, 32)
	if err != nil {
		return nil, err
	}
	if delta != int64(expectedDelta) {
		return nil, fmt.Errorf("%w: noncontiguous record offset delta", ErrCorruptRecord)
	}
	if _, err := readRecordBytes(&record, true); err != nil {
		return nil, err
	}
	value, err := readRecordBytes(&record, true)
	if err != nil {
		return nil, err
	}
	headers, err := readVarint(&record, 32)
	if err != nil {
		return nil, err
	}
	if headers < 0 || headers > int64(len(record)/2) {
		return nil, fmt.Errorf("%w: invalid header count", ErrCorruptRecord)
	}
	for i := int64(0); i < headers; i++ {
		if _, err := readRecordBytes(&record, false); err != nil {
			return nil, err
		}
		if _, err := readRecordBytes(&record, true); err != nil {
			return nil, err
		}
	}
	if len(record) != 0 {
		return nil, fmt.Errorf("%w: bytes after record headers", ErrCorruptRecord)
	}
	return value, nil
}

func readVarint(data *[]byte, bits int) (int64, error) {
	value, n := binary.Varint(*data)
	if n <= 0 || (bits == 32 && (n > 5 || value < math.MinInt32 || value > math.MaxInt32)) {
		return 0, fmt.Errorf("%w: invalid varint", ErrCorruptRecord)
	}
	*data = (*data)[n:]
	return value, nil
}

func readRecordBytes(data *[]byte, nullable bool) ([]byte, error) {
	length, err := readVarint(data, 32)
	if err != nil {
		return nil, err
	}
	if length == -1 && nullable {
		return nil, nil
	}
	if length < 0 || length > int64(len(*data)) {
		return nil, fmt.Errorf("%w: invalid record field length", ErrCorruptRecord)
	}
	value := (*data)[:int(length)]
	*data = (*data)[int(length):]
	return value, nil
}

// Record is a key/value pair used when constructing local, nontransactional batches.
type Record struct {
	Key   []byte
	Value []byte
}

// EncodeRecordBatch builds a checksummed batch with contiguous record offsets.
func EncodeRecordBatch(baseOffset int64, records []Record) ([]byte, error) {
	if baseOffset < 0 || len(records) == 0 || len(records) > math.MaxInt32 ||
		int64(len(records)) > math.MaxInt64-baseOffset {
		return nil, fmt.Errorf("%w: invalid base offset or record count", ErrCorruptRecord)
	}
	data := make([]byte, recordBatchHeaderSize)
	binary.BigEndian.PutUint64(data[:8], uint64(baseOffset))
	data[16] = 2
	binary.BigEndian.PutUint32(data[23:27], uint32(len(records)-1))
	now := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(data[27:35], now)
	binary.BigEndian.PutUint64(data[35:43], now)
	binary.BigEndian.PutUint64(data[43:51], math.MaxUint64)
	binary.BigEndian.PutUint16(data[51:53], math.MaxUint16)
	binary.BigEndian.PutUint32(data[53:57], math.MaxUint32)
	binary.BigEndian.PutUint32(data[57:61], uint32(len(records)))
	for i, record := range records {
		// Each record needs at most 26 bytes of length/delta/framing overhead.
		if len(record.Key) > MaxRecordBatchBytes || len(record.Value) > MaxRecordBatchBytes ||
			len(data)+len(record.Key)+len(record.Value)+26 > MaxRecordBatchBytes {
			return nil, fmt.Errorf("%w: batch exceeds size limit", ErrCorruptRecord)
		}
		r := []byte{0, 0} // attributes and zero timestamp delta
		r = binary.AppendVarint(r, int64(i))
		r = appendRecordBytes(r, record.Key)
		r = appendRecordBytes(r, record.Value)
		r = append(r, 0) // no headers
		data = binary.AppendVarint(data, int64(len(r)))
		data = append(data, r...)
	}
	binary.BigEndian.PutUint32(data[8:12], uint32(len(data)-12))
	binary.BigEndian.PutUint32(data[17:21], crc32.Checksum(data[21:], crcTable))
	return data, nil
}

func appendRecordBytes(dst, value []byte) []byte {
	if value == nil {
		return binary.AppendVarint(dst, -1)
	}
	dst = binary.AppendVarint(dst, int64(len(value)))
	return append(dst, value...)
}
