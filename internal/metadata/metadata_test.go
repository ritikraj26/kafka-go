package metadata

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestSeekToOffset verifies binary search across a synthetic 3-entry index.
func TestSeekToOffset(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LogDir: dir}

	// Build a synthetic index with 3 entries:
	//   offset 0  → file position 0
	//   offset 5  → file position 100
	//   offset 10 → file position 200
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
		got, err := p.SeekToOffset(tt.target)
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
	got, err := p.SeekToOffset(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}
