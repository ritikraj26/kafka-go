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
