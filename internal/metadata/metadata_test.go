package metadata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/storage"
)

func testRecordBatch(t *testing.T, values ...string) []byte {
	t.Helper()
	records := make([]storage.Record, len(values))
	for i, value := range values {
		records[i].Value = []byte(value)
	}
	batch, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

// TestSegmentRolling verifies that AppendRecords rolls to a new segment when
// MaxSegmentBytes is exceeded, that multiple .log files are created, and that
// recoverNextOffset returns the correct total across all segments.
func TestSegmentRolling(t *testing.T) {
	dir := t.TempDir()

	// Each complete batch exceeds this limit, forcing the next append to roll.
	orig := MaxSegmentBytes
	MaxSegmentBytes = 20
	defer func() { MaxSegmentBytes = orig }()

	p := &Partition{
		Index:      0,
		LeaderID:   1,
		ISRNodes:   []int32{1},
		ReplicaLEO: map[int32]int64{},
	}

	partDir := filepath.Join(dir, "test-0")
	os.MkdirAll(partDir, 0755)

	for i := 0; i < 4; i++ {
		if _, err := p.AppendRecords(testRecordBatch(t, "a", "b", "c"), partDir); err != nil {
			t.Fatalf("AppendRecords[%d]: %v", i, err)
		}
	}

	// There should be more than one .log file.
	segs, err := listSegments(partDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Errorf("expected at least 2 segments, got %d", len(segs))
	}

	got, err := recoverNextOffset(partDir)
	if err != nil {
		t.Fatal(err)
	}
	if got != 12 {
		t.Errorf("recoverNextOffset = %d, want 12", got)
	}
}

// TestAppendRecords verifies that AppendRecords writes to disk and advances NextOffset.
func TestAppendRecords(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "topic-0")
	os.MkdirAll(partDir, 0755)

	p := &Partition{
		Index:      0,
		LeaderID:   1,
		ISRNodes:   []int32{1},
		ReplicaLEO: map[int32]int64{},
	}

	data := testRecordBatch(t, "hello-record")
	offset, err := p.AppendRecords(data, partDir)
	if err != nil {
		t.Fatalf("AppendRecords: %v", err)
	}
	if offset != 0 {
		t.Errorf("baseOffset = %d, want 0", offset)
	}
	if p.NextOffset != 1 {
		t.Errorf("NextOffset = %d, want 1", p.NextOffset)
	}

	// Second append
	offset2, err := p.AppendRecords(testRecordBatch(t, "second"), partDir)
	if err != nil {
		t.Fatalf("second AppendRecords: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("second baseOffset = %d, want 1", offset2)
	}

	// Data must be on disk
	segs, err := listSegments(partDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("no segment files found after AppendRecords")
	}
}

// TestReadPartitionLogFrom verifies reading from a known byte offset within a segment.
func TestReadPartitionLogFrom(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "00000000000000000000.log")
	content := []byte("AABBCCDD")
	os.WriteFile(logFile, content, 0644)

	p := &Partition{LeaderID: 1, BrokerLogDirs: map[int32]string{1: dir}}
	// Read from offset 4 — should get "CCDD"
	data, err := ReadPartitionLogFrom(p, "00000000000000000000.log", 4)
	if err != nil {
		t.Fatalf("ReadPartitionLogFrom: %v", err)
	}
	if string(data) != "CCDD" {
		t.Errorf("data = %q, want \"CCDD\"", string(data))
	}
}

// TestReadPartitionLogFrom_MissingFile returns empty bytes for a non-existent segment.
func TestReadPartitionLogFrom_MissingFile(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, BrokerLogDirs: map[int32]string{1: dir}}
	data, err := ReadPartitionLogFrom(p, "00000000000000000000.log", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty slice for missing file, got %d bytes", len(data))
	}
}

// TestLoadTopicsFromDisk_SkipsBrokerDirs verifies that broker-N directories are not
// treated as topics (Fix 2).
func TestLoadTopicsFromDisk_SkipsBrokerDirs(t *testing.T) {
	dir := t.TempDir()
	// Create a real topic partition directory
	os.MkdirAll(filepath.Join(dir, "orders-0"), 0755)
	// Create broker replica subdirectories (should be ignored)
	os.MkdirAll(filepath.Join(dir, "broker-1"), 0755)
	os.MkdirAll(filepath.Join(dir, "broker-2"), 0755)
	// Create internal topic (should also be ignored)
	os.MkdirAll(filepath.Join(dir, "__cluster_metadata-0"), 0755)

	mgr := NewManager()
	if err := mgr.LoadTopicsFromDisk(dir); err != nil {
		t.Fatalf("LoadTopicsFromDisk: %v", err)
	}

	topics := mgr.ListTopics()
	for _, name := range topics {
		if name == "broker" {
			t.Error("\"broker\" should not be loaded as a topic (broker-N dirs must be skipped)")
		}
		if name == "__cluster_metadata" {
			t.Error("internal topic __cluster_metadata should not appear")
		}
	}
	// "orders" should be loaded
	found := false
	for _, name := range topics {
		if name == "orders" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected topic \"orders\" to be loaded from disk")
	}
}

// TestRecoverNextOffset_MultiSegment verifies offset recovery across segment boundaries.
func TestRecoverNextOffset_MultiSegment(t *testing.T) {
	dir := t.TempDir()
	orig := MaxSegmentBytes
	MaxSegmentBytes = 20
	defer func() { MaxSegmentBytes = orig }()

	p := &Partition{
		Index:      0,
		LeaderID:   1,
		ISRNodes:   []int32{1},
		ReplicaLEO: map[int32]int64{},
	}
	batch := testRecordBatch(t, "record")

	for i := 0; i < 3; i++ {
		if _, err := p.AppendRecords(batch, dir); err != nil {
			t.Fatalf("AppendRecords[%d]: %v", i, err)
		}
	}

	got, err := recoverNextOffset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Errorf("recoverNextOffset = %d, want 3", got)
	}
}

// TestSeekToOffset verifies indexed seeks against real batch boundaries.
func TestSeekToOffset(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, BrokerLogDirs: map[int32]string{1: dir}}

	// Build a synthetic index with 3 entries:
	//   offset 0  → file position 0
	//   offset 5  → file position 100
	//   offset 10 → file position 200
	// Store real 100-byte batches at the indexed positions. Index hints must
	// correspond to contiguous batch boundaries in the authoritative log.
	records := make([]storage.Record, 5)
	records[0].Value = []byte("abcd")
	var logData []byte
	for _, base := range []int64{0, 5, 10} {
		batchRecords := records
		if base == 10 {
			batchRecords = []storage.Record{{Value: bytes.Repeat([]byte("a"), 32)}}
		}
		batch, err := storage.EncodeRecordBatch(base, batchRecords)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != 100 {
			t.Fatalf("fixture batch size=%d, want 100", len(batch))
		}
		logData = append(logData, batch...)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.log"), logData, 0644); err != nil {
		t.Fatalf("setup log: %v", err)
	}
	idx := filepath.Join(dir, "00000000000000000000.index")
	entries := [][2]int32{{0, 0}, {5, 100}, {10, 200}}
	data := make([]byte, len(entries)*8)
	for i, e := range entries {
		binary.BigEndian.PutUint32(data[i*8:], uint32(e[0]))
		binary.BigEndian.PutUint32(data[i*8+4:], uint32(e[1]))
	}
	if err := os.WriteFile(idx, data, 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tests := []struct {
		target  int64
		wantPos int64
	}{
		{0, 0},    // exact first entry
		{3, 0},    // between 0 and 5, returns position for 0
		{5, 100},  // exact middle entry
		{7, 100},  // between 5 and 10, returns position for 5
		{10, 200}, // exact last entry
		{99, 200}, // beyond all entries, returns position for 10
	}

	for _, tt := range tests {
		_, got, err := p.SeekToOffset(tt.target)
		if err != nil {
			t.Errorf("SeekToOffset(%d): unexpected error: %v", tt.target, err)
			continue
		}
		if got != tt.wantPos {
			t.Errorf("SeekToOffset(%d): got %d, want %d", tt.target, got, tt.wantPos)
		}
	}
}

// TestSeekToOffsetNoIndex returns 0 when no index file exists.
func TestSeekToOffsetNoIndex(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, BrokerLogDirs: map[int32]string{1: dir}}
	_, got, err := p.SeekToOffset(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestAppendRecordsAssignsOffsetsAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, LeaderEpoch: 7}
	first := testRecordBatch(t, "a", "b", "c")
	second := testRecordBatch(t, "d", "e")
	input := append(bytes.Clone(first), second...)
	original := bytes.Clone(input)
	if offset, err := p.AppendRecords(input, dir); err != nil || offset != 0 {
		t.Fatalf("first append: offset=%d err=%v", offset, err)
	}
	if offset, err := p.AppendRecords(testRecordBatch(t, "f"), dir); err != nil || offset != 5 {
		t.Fatalf("second append: offset=%d err=%v", offset, err)
	}
	if !bytes.Equal(input, original) {
		t.Fatal("append mutated the producer's buffer")
	}
	if p.NextOffset != 6 || p.GetLogEndOffset() != 6 || p.GetHighWatermark() != 6 || p.GetReplicaLEO(1) != 6 {
		t.Fatal("record offsets were not propagated to partition state")
	}
	data, err := os.ReadFile(filepath.Join(dir, segmentNameForOffset(0)))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := storage.ParseRecordBatches(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 3 {
		t.Fatalf("batches=%d, want 3", len(batches))
	}
	for i, want := range []int64{0, 3, 5} {
		if batches[i].BaseOffset != want || binary.BigEndian.Uint32(batches[i].Data[12:16]) != 7 {
			t.Fatalf("batch %d has wrong assigned offset or epoch", i)
		}
	}
	_, pos, err := p.SeekToOffset(4, dir)
	if err != nil || pos != int64(len(first)) {
		t.Fatalf("seek inside second batch: pos=%d err=%v", pos, err)
	}
	if next, err := recoverNextOffset(dir); err != nil || next != 6 {
		t.Fatalf("recovery: next=%d err=%v", next, err)
	}
}

func TestAppendRecordsRejectsWholeInvalidRecordSet(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}}
	good := testRecordBatch(t, "good")
	bad := bytes.Clone(good)
	bad[len(bad)-1] ^= 1
	if _, err := p.AppendRecords(append(bytes.Clone(good), bad...), dir); !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("invalid second batch: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || p.NextOffset != 0 {
		t.Fatal("invalid input changed disk or offset state")
	}
	if _, err := p.AppendRecords(good, dir); err != nil {
		t.Fatalf("validation failure fenced valid future writes: %v", err)
	}
}

func TestAppendRecordsFencesDiskFailure(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}}
	batch := testRecordBatch(t, "record")
	if _, err := p.AppendRecords(batch, blocker); err == nil {
		t.Fatal("disk failure was ignored")
	}
	if _, err := p.AppendRecords(batch, dir); err == nil || p.NextOffset != 0 {
		t.Fatal("partition accepted another write after an ambiguous disk failure")
	}
}

func TestRelativeIndexWithLargeOffsets(t *testing.T) {
	dir := t.TempDir()
	start := int64(math.MaxInt32) + 10
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, NextOffset: start}
	first := testRecordBatch(t, "a", "b")
	if _, err := p.AppendRecords(first, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := p.AppendRecords(testRecordBatch(t, "c"), dir); err != nil {
		t.Fatal(err)
	}
	segment, pos, err := p.SeekToOffset(start+2, dir)
	if err != nil || segment != segmentNameForOffset(start) || pos != int64(len(first)) {
		t.Fatalf("large-offset seek: segment=%s pos=%d err=%v", segment, pos, err)
	}
	if next, err := recoverNextOffset(dir); err != nil || next != start+3 {
		t.Fatalf("large-offset recovery: next=%d err=%v", next, err)
	}
}

func TestAppendRecordsStartsSegmentAtFirstOffset(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, NextOffset: 7}
	if _, err := p.AppendRecords(testRecordBatch(t, "a", "b"), dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentNameForOffset(7))); err != nil {
		t.Fatal(err)
	}
	if next, err := recoverNextOffset(dir); err != nil || next != 9 {
		t.Fatalf("nonzero initial offset recovery: next=%d err=%v", next, err)
	}
}

func TestAppendRecordsRejectsOffsetOverflow(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, NextOffset: math.MaxInt64 - 1}
	if _, err := p.AppendRecords(testRecordBatch(t, "a", "b"), dir); !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("offset overflow: %v", err)
	}
	if p.NextOffset != math.MaxInt64-1 {
		t.Fatal("offset overflow changed partition state")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("overflowing append changed disk: %v", err)
	}
}

func TestRecoveryPreservesDamagedLogs(t *testing.T) {
	batch := testRecordBatch(t, "record")
	tests := map[string][]byte{
		"partial header": batch[:11],
		"partial body":   batch[:len(batch)-1],
		"partial tail":   append(bytes.Clone(batch), batch[:20]...),
		"duplicate":      append(bytes.Clone(batch), batch...),
		"bad checksum":   bytes.Clone(batch),
	}
	tests["bad checksum"][17] ^= 1
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, segmentNameForOffset(0))
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := recoverNextOffset(dir); err == nil {
				t.Fatal("damaged log was accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, data) {
				t.Fatalf("recovery modified damaged data: %v", err)
			}
		})
	}
}

func TestRecoveryValidatesInactiveSegments(t *testing.T) {
	dir := t.TempDir()
	first := testRecordBatch(t, "first")
	first[17] ^= 1
	second := testRecordBatch(t, "second")
	binary.BigEndian.PutUint64(second[:8], 1)
	for offset, batch := range [][]byte{first, second} {
		if err := os.WriteFile(filepath.Join(dir, segmentNameForOffset(int64(offset))), batch, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := recoverNextOffset(dir); !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("corrupt inactive segment was not rejected: %v", err)
	}
}

func TestLoadTopicsRestoresRecordOffsets(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "events-0")
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}}
	if _, err := p.AppendRecords(testRecordBatch(t, "a", "b", "c"), partDir); err != nil {
		t.Fatal(err)
	}
	m := NewManager()
	if err := m.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	topic := m.GetTopic("events")
	if topic == nil {
		t.Fatal("topic not recovered")
	}
	recovered := &topic.Partitions[0]
	if recovered.NextOffset != 3 || recovered.GetLogEndOffset() != 3 || recovered.GetHighWatermark() != 3 ||
		recovered.GetReplicaLEO(1) != 3 || m.GetLogDir() != dir {
		t.Fatal("recovery did not restore offsets and configured directory")
	}
	if offset, err := recovered.AppendRecords(testRecordBatch(t, "d"), recovered.LogDirForBroker(1)); err != nil || offset != 3 {
		t.Fatalf("append after restart: offset=%d err=%v", offset, err)
	}
}

func TestLoadTopicsRejectsDamagedReplicaLogs(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "broker-2", "events-0")
	if err := os.MkdirAll(partDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partDir, segmentNameForOffset(0)), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := NewManager().LoadTopicsFromDisk(dir); err == nil {
		t.Fatal("corrupt replica log was ignored")
	}
}

func TestAppendReplicaRecordsPreservesOffsets(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{}
	data := testRecordBatch(t, "a", "b")
	if err := p.AppendReplicaRecords(data, dir, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendReplicaRecords(data, dir, 2); !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("noncontiguous replica write accepted: %v", err)
	}
	if next, err := recoverNextOffset(dir); err != nil || next != 2 {
		t.Fatalf("replica recovery: next=%d err=%v", next, err)
	}
}

func TestWaitForHighWatermarkTimesOut(t *testing.T) {
	p := &Partition{}
	for _, timeout := range []time.Duration{0, time.Millisecond, 5 * time.Millisecond} {
		start := time.Now()
		if p.WaitForHighWatermark(1, timeout) {
			t.Fatal("wait succeeded before replication")
		}
		if time.Since(start) > time.Second {
			t.Fatal("high-watermark timeout did not unblock")
		}
	}
}
