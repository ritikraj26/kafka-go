package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const logDir = "/tmp/kraft-broker-logs"

type seedTopic struct {
	name       string
	partitions int
	records    [][]seedRecord
}

type seedRecord struct {
	key   []byte
	value []byte
}

func main() {
	topics := []seedTopic{
		{
			name:       "orders",
			partitions: 3,
			records: [][]seedRecord{
				{
					{[]byte("order-1"), []byte(`{"item":"laptop","qty":1,"price":999}`)},
					{[]byte("order-2"), []byte(`{"item":"phone","qty":2,"price":499}`)},
					{[]byte("order-3"), []byte(`{"item":"tablet","qty":1,"price":349}`)},
					{[]byte("order-4"), []byte(`{"item":"monitor","qty":1,"price":279}`)},
					{[]byte("order-5"), []byte(`{"item":"keyboard","qty":3,"price":79}`)},
				},
				{
					{[]byte("order-6"), []byte(`{"item":"mouse","qty":5,"price":29}`)},
					{[]byte("order-7"), []byte(`{"item":"headset","qty":1,"price":149}`)},
					{[]byte("order-8"), []byte(`{"item":"webcam","qty":2,"price":89}`)},
					{[]byte("order-9"), []byte(`{"item":"charger","qty":4,"price":39}`)},
					{[]byte("order-10"), []byte(`{"item":"cable","qty":10,"price":12}`)},
				},
				{
					{[]byte("order-11"), []byte(`{"item":"ssd","qty":1,"price":119}`)},
					{[]byte("order-12"), []byte(`{"item":"ram","qty":2,"price":59}`)},
					{[]byte("order-13"), []byte(`{"item":"case","qty":1,"price":69}`)},
					{[]byte("order-14"), []byte(`{"item":"fan","qty":3,"price":15}`)},
					{[]byte("order-15"), []byte(`{"item":"psu","qty":1,"price":89}`)},
				},
			},
		},
		{
			name:       "events",
			partitions: 2,
			records: [][]seedRecord{
				{
					{[]byte("evt-1"), []byte(`{"type":"click","page":"home","ts":1711700000}`)},
					{[]byte("evt-2"), []byte(`{"type":"scroll","page":"products","ts":1711700010}`)},
					{[]byte("evt-3"), []byte(`{"type":"click","page":"checkout","ts":1711700020}`)},
				},
				{
					{[]byte("evt-4"), []byte(`{"type":"pageview","page":"about","ts":1711700030}`)},
					{[]byte("evt-5"), []byte(`{"type":"click","page":"signup","ts":1711700040}`)},
					{[]byte("evt-6"), []byte(`{"type":"submit","page":"signup","ts":1711700050}`)},
				},
			},
		},
	}

	fmt.Printf("Seeding log directory: %s\n\n", logDir)

	for _, t := range topics {
		for pi := 0; pi < t.partitions; pi++ {
			dir := filepath.Join(logDir, fmt.Sprintf("%s-%d", t.name, pi))
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", dir, err)
				os.Exit(1)
			}

			logFile := filepath.Join(dir, "00000000000000000000.log")
			indexFile := filepath.Join(dir, "00000000000000000000.index")

			var logBuf bytes.Buffer
			var indexBuf bytes.Buffer

			records := t.records[pi]
			for offset, rec := range records {
				filePos := int32(logBuf.Len())
				batch := buildRecordBatch(int64(offset), rec.key, rec.value)
				logBuf.Write(batch)

				binary.Write(&indexBuf, binary.BigEndian, int32(offset))
				binary.Write(&indexBuf, binary.BigEndian, filePos)
			}

			if err := os.WriteFile(logFile, logBuf.Bytes(), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "write %s: %v\n", logFile, err)
				os.Exit(1)
			}
			if err := os.WriteFile(indexFile, indexBuf.Bytes(), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "write %s: %v\n", indexFile, err)
				os.Exit(1)
			}

			fmt.Printf("  %s-%d: %d records written\n", t.name, pi, len(records))
		}
	}

	fmt.Printf("\nDone. Start the broker with:\n  go run app/main.go\n")
}

func writeZigzagVarint(buf *bytes.Buffer, v int64) {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
}

func buildRecordBatch(baseOffset int64, key, value []byte) []byte {
	var record bytes.Buffer
	record.WriteByte(0x00)
	record.WriteByte(0x00)
	record.WriteByte(0x00)

	if key == nil {
		record.WriteByte(0x01)
	} else {
		writeZigzagVarint(&record, int64(len(key)))
		record.Write(key)
	}

	writeZigzagVarint(&record, int64(len(value)))
	record.Write(value)
	record.WriteByte(0x00)

	recBytes := record.Bytes()

	var recWithLen bytes.Buffer
	writeZigzagVarint(&recWithLen, int64(len(recBytes)))
	recWithLen.Write(recBytes)

	now := time.Now().UnixMilli()

	var batch bytes.Buffer
	binary.Write(&batch, binary.BigEndian, baseOffset)
	batchLengthPos := batch.Len()
	binary.Write(&batch, binary.BigEndian, int32(0))
	binary.Write(&batch, binary.BigEndian, int32(0))
	batch.WriteByte(2)
	binary.Write(&batch, binary.BigEndian, uint32(0))
	binary.Write(&batch, binary.BigEndian, int16(0))
	binary.Write(&batch, binary.BigEndian, int32(0))
	binary.Write(&batch, binary.BigEndian, now)
	binary.Write(&batch, binary.BigEndian, now)
	binary.Write(&batch, binary.BigEndian, int64(-1))
	binary.Write(&batch, binary.BigEndian, int16(-1))
	binary.Write(&batch, binary.BigEndian, int32(-1))
	binary.Write(&batch, binary.BigEndian, int32(1))

	batch.Write(recWithLen.Bytes())

	result := batch.Bytes()
	batchLen := int32(len(result) - 12)
	binary.BigEndian.PutUint32(result[batchLengthPos:batchLengthPos+4], uint32(batchLen))

	return result
}
