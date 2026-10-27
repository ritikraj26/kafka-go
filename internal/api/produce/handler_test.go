package produce

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/storage"
)

func TestBuildBody_UnknownTopic(t *testing.T) {
	metaMgr := metadata.NewManager()

	req := &protocol.ProduceRequest{
		Acks: -1,
		Topics: []protocol.ProduceTopic{
			{
				Name: "nonexistent",
				Partitions: []protocol.ProducePartition{
					{Index: 0, Records: []byte{0x01, 0x02}},
				},
			},
		},
	}

	body := BuildBody(req, metaMgr, 1)
	pos := 0

	topicsArrayLen := int(body[pos]) - 1
	pos++
	if topicsArrayLen != 1 {
		t.Fatalf("topics array len = %d, want 1", topicsArrayLen)
	}

	nameLen := int(body[pos]) - 1
	pos++
	topicName := string(body[pos : pos+nameLen])
	pos += nameLen
	if topicName != "nonexistent" {
		t.Errorf("topic name = %q, want %q", topicName, "nonexistent")
	}

	partArrayLen := int(body[pos]) - 1
	pos++
	if partArrayLen != 1 {
		t.Fatalf("partitions array len = %d, want 1", partArrayLen)
	}

	partIdx := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if partIdx != 0 {
		t.Errorf("partition index = %d, want 0", partIdx)
	}

	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	if errCode != protocol.ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want %d", errCode, protocol.ErrUnknownTopicOrPartition)
	}
}

func TestBuildBody_ValidTopic(t *testing.T) {
	metaMgr := metadata.NewManager()
	dir := t.TempDir()
	metaMgr.SetLogDir(dir)

	metaMgr.CreateTopic("orders", 1)
	metaMgr.Schemas.Register("orders", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})

	req := &protocol.ProduceRequest{
		Acks: -1,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{Index: 0, Records: produceBatch(t, `{"value":"record"}`)},
				},
			},
		},
	}

	body := BuildBody(req, metaMgr, 1)
	pos := 0

	topicsArrayLen := int(body[pos]) - 1
	pos++
	if topicsArrayLen != 1 {
		t.Fatalf("topics array len = %d, want 1", topicsArrayLen)
	}

	nameLen := int(body[pos]) - 1
	pos++
	topicName := string(body[pos : pos+nameLen])
	pos += nameLen
	if topicName != "orders" {
		t.Errorf("topic name = %q, want %q", topicName, "orders")
	}
	partArrayLen := int(body[pos]) - 1
	pos++
	if partArrayLen != 1 {
		t.Fatalf("partitions array len = %d, want 1", partArrayLen)
	}
	partIdx := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	if partIdx != 0 {
		t.Errorf("partition index = %d, want 0", partIdx)
	}
	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if errCode != protocol.ErrNone {
		t.Errorf("error_code = %d, want %d (ErrNone)", errCode, protocol.ErrNone)
	}
	baseOffset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	if baseOffset != 0 {
		t.Errorf("base_offset = %d, want 0", baseOffset)
	}
}

func produceBatch(t *testing.T, values ...string) []byte {
	t.Helper()
	records := make([]storage.Record, len(values))
	for i, value := range values {
		records[i].Value = []byte(value)
	}
	data, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func produceRequest(data []byte) *protocol.ProduceRequest {
	return &protocol.ProduceRequest{Acks: 1, TimeoutMs: 5, Topics: []protocol.ProduceTopic{{
		Name: "orders", Partitions: []protocol.ProducePartition{{Index: 0, Records: data}},
	}}}
}

func responseError(t *testing.T, data []byte) int16 {
	t.Helper()
	d := protocol.NewDecoder(data)
	if _, err := d.ReadUnsignedVarint(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadCompactString(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadUnsignedVarint(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadInt32(); err != nil {
		t.Fatal(err)
	}
	code, err := d.ReadInt16()
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func produceManager(t *testing.T) (*metadata.Manager, *metadata.Partition) {
	t.Helper()
	m := metadata.NewManager()
	m.SetLogDir(t.TempDir())
	m.Schemas.Register("orders", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	return m, &m.CreateTopic("orders", 1).Partitions[0]
}

func TestProduceRejectsInvalidBatches(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
		code int16
	}{
		{"short", func(b []byte) []byte { return b[:2] }, 2},
		{"CRC", func(b []byte) []byte { b[17] ^= 1; return b }, 2},
		{"second batch", func(b []byte) []byte { return append(b, 0) }, 2},
		{"magic", func(b []byte) []byte { b[16] = 1; return b }, 43},
		{"compression", func(b []byte) []byte {
			b[22] = 1
			binary.BigEndian.PutUint32(b[17:21], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
			return b
		}, 76},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, p := produceManager(t)
			req := produceRequest(tc.edit(produceBatch(t, `{"value":"valid"}`)))
			if code := responseError(t, BuildBody(req, m, 1)); code != tc.code {
				t.Fatalf("error=%d, want %d", code, tc.code)
			}
			if p.GetLogEndOffset() != 0 || m.GetTopic("orders.dlq") != nil {
				t.Fatal("invalid batches caused write side effects")
			}
			if _, err := os.Stat(p.LogDirForBroker(1)); !os.IsNotExist(err) {
				t.Fatalf("invalid records touched the partition directory: %v", err)
			}
		})
	}
}

func TestProduceWaitsForEveryRecord(t *testing.T) {
	m, p := produceManager(t)
	p.ISRNodes = []int32{1, 2}
	p.UpdateReplicaState(2, 1, 0)
	req := produceRequest(produceBatch(t, `{"value":"a"}`, `{"value":"b"}`, `{"value":"c"}`))
	req.Acks = -1
	if code := responseError(t, BuildBody(req, m, 1)); code != protocol.ErrRequestTimedOut {
		t.Fatalf("acks=-1 returned %d despite HW covering only the first record", code)
	}
	if p.GetLogEndOffset() != 3 || p.GetHighWatermark() != 1 {
		t.Fatal("unexpected offset accounting")
	}
}

func TestProduceMultipleBatches(t *testing.T) {
	m, p := produceManager(t)
	data := append(produceBatch(t, `{"value":"a"}`, `{"value":"b"}`), produceBatch(t, `{"value":"c"}`)...)
	if code := responseError(t, BuildBody(produceRequest(data), m, 1)); code != 0 {
		t.Fatalf("produce failed: %d", code)
	}
	stored, err := metadata.ReadPartitionLog(p)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := storage.ParseRecordBatches(stored)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].BaseOffset != 0 || batches[1].BaseOffset != 2 || p.GetLogEndOffset() != 3 {
		t.Fatal("wrong offsets for multi-batch produce")
	}
}

func TestDLQWritesValidBatches(t *testing.T) {
	m, _ := produceManager(t)
	data := produceBatch(t, `{"value":1}`, `{"value":2}`)
	if code := responseError(t, BuildBody(produceRequest(data), m, 1)); code != 0 {
		t.Fatalf("DLQ routing failed: %d", code)
	}
	dlq := m.GetTopic("orders.dlq")
	if dlq == nil {
		t.Fatal("DLQ not created")
	}
	stored, err := metadata.ReadPartitionLog(&dlq.Partitions[0])
	if err != nil {
		t.Fatal(err)
	}
	batches, err := storage.ParseRecordBatches(stored)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || batches[0].RecordsCount != 2 || len(batches[0].Values) != 2 ||
		string(batches[0].Values[0]) != `{"value":1}` || string(batches[0].Values[1]) != `{"value":2}` {
		t.Fatal("DLQ did not preserve rejected values in a valid batch")
	}
}

func TestDLQPersistenceFailureIsReported(t *testing.T) {
	m, p := produceManager(t)
	dlq := m.GetOrCreateDLQTopic("orders")
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	dp := &dlq.Partitions[0]
	dp.BrokerLogDirs[dp.LeaderID] = blocker
	if code := responseError(t, BuildBody(produceRequest(produceBatch(t, `{"value":1}`)), m, 1)); code != -1 {
		t.Fatalf("DLQ disk failure returned %d, want UNKNOWN_SERVER_ERROR (-1)", code)
	}
	if p.GetLogEndOffset() != 0 {
		t.Fatal("source log advanced after failed routing")
	}
}
