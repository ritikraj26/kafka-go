package metadata

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestSegmentRolling verifies that AppendRecords rolls to a new segment when
// MaxSegmentBytes is exceeded, that multiple .log files are created, and that
// recoverNextOffset returns the correct total across all segments.
func TestSegmentRolling(t *testing.T) {
	dir := t.TempDir()

	// Override segment size: 20 bytes so each ~12-byte-header batch forces a roll.
	orig := MaxSegmentBytes
	MaxSegmentBytes = 20
	defer func() { MaxSegmentBytes = orig }()

	// Build a minimal valid-enough RecordBatch: 8-byte baseOffset + 4-byte batchLength + 12 payload bytes.
	makeBatch := func() []byte {
		b := make([]byte, 24)
		binary.BigEndian.PutUint32(b[8:12], 12) // batchLength = 12
		return b
	}

	p := &Partition{
		Index:      0,
		LeaderID:   1,
		ISRNodes:   []int32{1},
		ReplicaLEO: map[int32]int64{},
	}

	partDir := filepath.Join(dir, "test-0")
	os.MkdirAll(partDir, 0755)

	// Write 4 batches; with MaxSegmentBytes=20 and batch size=24 each write should
	// create a new segment after the first (first segment gets the 24-byte write,
	// which then exceeds 20 → next write rolls).
	for i := 0; i < 4; i++ {
		if _, err := p.AppendRecords(makeBatch(), partDir); err != nil {
			t.Fatalf("AppendRecords[%d]: %v", i, err)
		}
	}

	// There should be more than one .log file.
	segs := listSegments(partDir)
	if len(segs) < 2 {
		t.Errorf("expected at least 2 segments, got %d", len(segs))
	}

	// recoverNextOffset must equal the number of batches we wrote.
	got := recoverNextOffset(partDir)
	if got != 4 {
		t.Errorf("recoverNextOffset = %d, want 4", got)
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

	data := []byte("hello-record")
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
	offset2, err := p.AppendRecords([]byte("second"), partDir)
	if err != nil {
		t.Fatalf("second AppendRecords: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("second baseOffset = %d, want 1", offset2)
	}

	// Data must be on disk
	segs := listSegments(partDir)
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

	p := &Partition{LogDir: dir}
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
	p := &Partition{LogDir: dir}
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
	// Build a 24-byte batch (forces a new segment each time with limit=20)
	batch := make([]byte, 24)
	binary.BigEndian.PutUint32(batch[8:12], 12)

	for i := 0; i < 3; i++ {
		if _, err := p.AppendRecords(batch, dir); err != nil {
			t.Fatalf("AppendRecords[%d]: %v", i, err)
		}
	}

	got := recoverNextOffset(dir)
	if got != 3 {
		t.Errorf("recoverNextOffset = %d, want 3", got)
	}
}

// TestSeekToOffset verifies binary search across a synthetic 3-entry index.
func TestSeekToOffset(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LogDir: dir}

	// Build a synthetic index with 3 entries:
	//   offset 0  → file position 0
	//   offset 5  → file position 100
	//   offset 10 → file position 200
	// Also create the corresponding .log file so listSegments finds the segment.
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.log"), []byte{}, 0644); err != nil {
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
	p := &Partition{LogDir: dir}
	_, got, err := p.SeekToOffset(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}
