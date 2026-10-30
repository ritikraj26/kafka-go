package fetch

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func fetchFixture(t *testing.T, partitions, replicas int) (*metadata.Manager, *metadata.Topic) {
	t.Helper()
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	for i := 1; i <= replicas; i++ {
		m.Brokers.Register(&broker.Broker{ID: int32(i)})
	}
	return m, testutil.CreateTopic(t, m, "events", partitions)
}

func appendValues(t *testing.T, p *metadata.Partition, values ...string) []byte {
	t.Helper()
	records := make([]storage.Record, len(values))
	for i, value := range values {
		records[i].Value = []byte(value)
	}
	data, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.AppendRecords(data, p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	return data
}

func requestFor(topic *metadata.Topic, offset int64) *protocol.FetchRequest {
	return &protocol.FetchRequest{ReplicaID: -1, MaxBytes: 1 << 20, MinBytes: 1,
		Topics: []protocol.FetchTopic{{TopicID: topic.ID, TopicName: topic.Name, Partitions: []protocol.FetchPartition{{
			PartitionIndex: 0, FetchOffset: offset, CurrentLeaderEpoch: -1, MaxBytes: 1 << 20,
		}}}},
	}
}

func decodeResults(t *testing.T, data []byte, modern bool) []partResult {
	t.Helper()
	d := protocol.NewDecoder(data)
	i32 := func() int32 {
		value, err := d.ReadInt32()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	i64 := func() int64 {
		value, err := d.ReadInt64()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	array := func() int {
		if !modern {
			return int(i32())
		}
		n, err := d.ReadUnsignedVarint()
		if err != nil || n == 0 {
			t.Fatalf("invalid compact array %d: %v", n, err)
		}
		return int(n - 1)
	}
	tags := func() {
		if modern {
			if err := d.ReadTaggedFields(nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if modern {
		i32()
		if code, err := d.ReadInt16(); err != nil || code != 0 {
			t.Fatalf("top-level error %d: %v", code, err)
		}
		if session := i32(); session != 0 {
			t.Fatal("stateless response claimed a session")
		}
	}
	var parts []partResult
	for n := array(); n > 0; n-- {
		if modern {
			for i := 0; i < 16; i++ {
				if _, err := d.ReadByte(); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			if _, err := d.ReadString(); err != nil {
				t.Fatal(err)
			}
		}
		for count := array(); count > 0; count-- {
			p := partResult{index: i32()}
			code, err := d.ReadInt16()
			if err != nil {
				t.Fatal(err)
			}
			p.errCode, p.hwm = code, i64()
			if modern {
				i64()
				p.start = i64()
				if null, err := d.ReadByte(); err != nil || null != 0 {
					t.Fatalf("aborted transactions %d: %v", null, err)
				}
				i32()
				p.records, err = d.ReadCompactBytes()
			} else {
				p.records, err = d.ReadBytes()
			}
			if err != nil {
				t.Fatal(err)
			}
			tags()
			parts = append(parts, p)
		}
		tags()
	}
	tags()
	if d.Remaining() != 0 {
		t.Fatalf("%d trailing response bytes", d.Remaining())
	}
	return parts
}

func fetchParts(t *testing.T, m *metadata.Manager, req *protocol.FetchRequest, modern bool) []partResult {
	t.Helper()
	if modern {
		return decodeResults(t, BuildBody(req, m, 1), true)
	}
	return decodeResults(t, BuildBodyV0(req, m, 1), false)
}

func assertValues(t *testing.T, data []byte, base int64, values ...string) {
	t.Helper()
	batches, err := storage.ParseRecordBatches(data)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	next := base
	for _, batch := range batches {
		if batch.BaseOffset != next {
			t.Fatalf("base offset=%d, want %d", batch.BaseOffset, next)
		}
		next += int64(batch.RecordsCount)
		for _, value := range batch.Values {
			got = append(got, string(value))
		}
	}
	if len(got) != len(values) {
		t.Fatalf("got values %q, want %q", got, values)
	}
	for i := range got {
		if got[i] != values[i] {
			t.Fatalf("record %d=%q want %q", i, got[i], values[i])
		}
	}
}

func TestFetchHighWatermarkAndReplicaProgress(t *testing.T) {
	m, topic := fetchFixture(t, 1, 2)
	p := &topic.Partitions[0]
	appendValues(t, p, "a", "b")
	appendValues(t, p, "c")
	p.UpdateReplicaState(2, 2, time.Now().UnixMilli())
	p.RunUnderLock(func() { p.AdvanceHighWatermark() })
	for _, modern := range []bool{false, true} {
		part := fetchParts(t, m, requestFor(topic, 0), modern)[0]
		if part.errCode != 0 || part.hwm != 2 {
			t.Fatalf("consumer result: %+v", part)
		}
		assertValues(t, part.records, 0, "a", "b")
	}
	p.RunUnderLock(func() { p.HighWatermark = 1 })
	part := fetchParts(t, m, requestFor(topic, 0), true)[0]
	if len(part.records) != 0 {
		t.Fatal("batch crossing HW leaked uncommitted records")
	}
	p.RunUnderLock(func() { p.HighWatermark = 0 })
	p.UpdateReplicaState(2, 0, 0)
	req := requestFor(topic, 0)
	req.ReplicaID = 2
	req.Topics[0].Partitions[0].MaxBytes = int32(len(appendBatch(t, "a", "b")))
	part = fetchParts(t, m, req, false)[0]
	assertValues(t, part.records, 0, "a", "b")
	if p.GetHighWatermark() != 0 || p.GetReplicaLEO(2) != 0 {
		t.Fatal("building a replica response acknowledged records not yet persisted")
	}
	if err := p.AppendFetchedRecords(2, 1, 0, 0, part.records); err != nil {
		t.Fatal(err)
	}
	req.Topics[0].Partitions[0].FetchOffset = 2
	part = fetchParts(t, m, req, false)[0]
	assertValues(t, part.records, 2, "c")
	if p.GetHighWatermark() != 2 || p.GetReplicaLEO(2) != 2 {
		t.Fatal("leader did not acknowledge exactly the follower's reported prior progress")
	}
}

func appendBatch(t *testing.T, values ...string) []byte {
	t.Helper()
	records := make([]storage.Record, len(values))
	for i, v := range values {
		records[i].Value = []byte(v)
	}
	batch, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestFetchByteBudgets(t *testing.T) {
	m, topic := fetchFixture(t, 2, 1)
	a := appendValues(t, &topic.Partitions[0], "a")
	appendValues(t, &topic.Partitions[0], "b")
	appendValues(t, &topic.Partitions[1], "c")
	for _, modern := range []bool{false, true} {
		for _, budget := range []int32{0, 1, int32(len(a)), int32(2*len(a) - 1), int32(2 * len(a))} {
			req := requestFor(topic, 0)
			req.MaxBytes = budget
			req.Topics[0].Partitions[0].MaxBytes = budget
			part := fetchParts(t, m, req, modern)[0]
			expected := int(budget) / len(a)
			if modern && expected == 0 {
				expected = 1
			}
			if len(part.records) != expected*len(a) {
				t.Fatalf("modern=%v budget=%d returned %d bytes, want %d", modern, budget, len(part.records), expected*len(a))
			}
			if len(part.records) > 0 {
				if _, err := storage.ParseRecordBatches(part.records); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	req := requestFor(topic, 0)
	req.MaxBytes = 1
	req.Topics[0].Partitions = append(req.Topics[0].Partitions, protocol.FetchPartition{
		PartitionIndex: 1, CurrentLeaderEpoch: -1, MaxBytes: 1 << 20,
	})
	parts := fetchParts(t, m, req, true)
	if len(parts[0].records) != len(a) || len(parts[1].records) != 0 {
		t.Fatal("oversized-first-batch exception applied more than once per response")
	}
	req.Topics[0].Partitions[0].FetchOffset = 2 // First partition has no more data.
	parts = fetchParts(t, m, req, true)
	assertValues(t, parts[1].records, 0, "c")
	req.Topics[0].Partitions[0].FetchOffset = 0
	req.MaxBytes = int32(3 * len(a))
	req.Topics[0].Partitions[0].MaxBytes = int32(len(a))
	parts = fetchParts(t, m, req, true)
	assertValues(t, parts[0].records, 0, "a")
	assertValues(t, parts[1].records, 0, "c")
}

func TestFetchAcrossSegmentsAndMissingIndexes(t *testing.T) {
	for _, removeIndexes := range []bool{false, true} {
		m, topic := fetchFixture(t, 1, 1)
		p := &topic.Partitions[0]
		original := metadata.MaxSegmentBytes
		metadata.MaxSegmentBytes = 1
		for _, values := range [][]string{{"a", "b"}, {"c", "d"}, {"e"}} {
			appendValues(t, p, values...)
		}
		metadata.MaxSegmentBytes = original
		if removeIndexes {
			paths, err := filepath.Glob(filepath.Join(p.LogDirForBroker(1), "*.index"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, modern := range []bool{false, true} {
			part := fetchParts(t, m, requestFor(topic, 3), modern)[0]
			if part.errCode != 0 {
				t.Fatalf("cross-segment error %d", part.errCode)
			}
			assertValues(t, part.records, 2, "c", "d", "e")
		}
	}
	// A missing index within one segment must scan past earlier batches.
	m, topic := fetchFixture(t, 1, 1)
	p := &topic.Partitions[0]
	appendValues(t, p, "a")
	appendValues(t, p, "b")
	if err := os.Remove(filepath.Join(p.LogDirForBroker(1), "00000000000000000000.index")); err != nil {
		t.Fatal(err)
	}
	assertValues(t, fetchParts(t, m, requestFor(topic, 1), true)[0].records, 1, "b")
}

func TestFetchErrorsAndEmptyOffsets(t *testing.T) {
	m, topic := fetchFixture(t, 1, 2)
	p := &topic.Partitions[0]
	appendValues(t, p, "a", "b")
	for _, tc := range []struct {
		offset  int64
		replica int32
		epoch   int32
		want    int16
	}{
		{-1, -1, -1, protocol.ErrOffsetOutOfRange},
		{3, -1, -1, protocol.ErrOffsetOutOfRange},
		{0, 99, -1, protocol.ErrNotLeaderOrFollower},
		{0, 1, -1, protocol.ErrNotLeaderOrFollower},
		{0, -1, 1, protocol.ErrUnknownLeaderEpoch},
		{1, 2, -1, protocol.ErrInvalidRequest},
		{2, -1, -1, 0},
		{1, -1, -1, 0},
	} {
		req := requestFor(topic, tc.offset)
		req.ReplicaID = tc.replica
		req.Topics[0].Partitions[0].CurrentLeaderEpoch = tc.epoch
		for _, modern := range []bool{false, true} {
			part := fetchParts(t, m, req, modern)[0]
			if part.errCode != tc.want || len(part.records) != 0 {
				t.Fatalf("case %+v modern=%v result=%+v", tc, modern, part)
			}
		}
	}
	req := requestFor(topic, 0)
	parts := decodeResults(t, BuildBody(req, m, 2), true)
	if parts[0].errCode != protocol.ErrNotLeaderOrFollower {
		t.Fatal("follower served a leader-only read")
	}
	req.Topics[0].TopicID = [16]byte{}
	req.Topics[0].TopicName = "missing"
	req.Topics[0].Partitions = []protocol.FetchPartition{{PartitionIndex: 4}, {PartitionIndex: 9}}
	for _, modern := range []bool{false, true} {
		parts = fetchParts(t, m, req, modern)
		code := protocol.ErrUnknownTopicOrPartition
		if modern {
			code = protocol.ErrUnknownTopicID
		}
		if len(parts) != 2 || parts[0].index != 4 || parts[1].index != 9 || parts[0].errCode != code || parts[1].errCode != code {
			t.Fatal("unknown-topic response did not preserve requested partitions")
		}
	}
}

func TestFetchDoesNotReadPayloadBeyondBudgetOrHW(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	p := &topic.Partitions[0]
	first := appendValues(t, p, "good")
	second := appendValues(t, p, "bad")
	path := filepath.Join(p.LogDirForBroker(1), "00000000000000000000.log")
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, int64(len(first)+len(second)-2)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	req := requestFor(topic, 0)
	req.MaxBytes = int32(len(first))
	req.Topics[0].Partitions[0].MaxBytes = int32(len(first))
	part := fetchParts(t, m, req, true)[0]
	if part.errCode != 0 {
		t.Fatal("fetch read a payload beyond its byte budget")
	}
	assertValues(t, part.records, 0, "good")
	req.MaxBytes = 1 << 20
	req.Topics[0].Partitions[0].MaxBytes = 1 << 20
	p.RunUnderLock(func() { p.HighWatermark = 1 })
	part = fetchParts(t, m, req, true)[0]
	if part.errCode != 0 {
		t.Fatal("fetch read a payload above HW")
	}
	assertValues(t, part.records, 0, "good")
	p.RunUnderLock(func() { p.HighWatermark = 2 })
	part = fetchParts(t, m, req, true)[0]
	if part.errCode != protocol.ErrUnknownServerError || len(part.records) != 0 {
		t.Fatal("disk corruption did not produce an explicit error without partial data")
	}
}

func TestFetchLongPollAndSessions(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	req := requestFor(topic, 0)
	req.MaxWaitMs = 200
	req.MinBytes = 0
	start := time.Now()
	fetchParts(t, m, req, true)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("min_bytes=0 did not return immediately")
	}
	req.MinBytes = 1
	req.Topics[0].Partitions[0].FetchOffset = -1
	start = time.Now()
	fetchParts(t, m, req, true)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("partition error waited for max_wait_ms")
	}
	req.SessionID = 123
	data := BuildBody(req, m)
	if int16(binary.BigEndian.Uint16(data[4:6])) != protocol.ErrFetchSessionIDNotFound {
		t.Fatal("unknown incremental session was silently accepted")
	}
	req.SessionID = 0
	req.Topics[0].Partitions[0].FetchOffset = 0
	req.MaxWaitMs = 10
	start = time.Now()
	fetchParts(t, m, req, true)
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond || elapsed > time.Second {
		t.Fatalf("empty long poll duration=%s", elapsed)
	}
}

func TestConcurrentAppendAndFetch(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	p := &topic.Partitions[0]
	batch := appendBatch(t, "record")
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 40; i++ {
			if _, err := p.AppendRecords(bytes.Clone(batch), p.LogDirForBroker(1)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 80; i++ {
			results, _, failed := collectData(requestFor(topic, 0), m, 1<<20, 1, true)
			if failed {
				t.Error("concurrent fetch failed")
				return
			}
			part := results[0].parts[0]
			if len(part.records) == 0 {
				continue
			}
			batches, err := storage.ParseRecordBatches(part.records)
			if err != nil {
				t.Error(err)
				return
			}
			last := batches[len(batches)-1]
			if last.BaseOffset+int64(last.RecordsCount) > part.hwm {
				t.Error("response exceeded its HW snapshot")
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}
