package protocol

import (
	"testing"
)

// TestWriteCompactString verifies the length+1 encoding for compact strings.
func TestWriteCompactString(t *testing.T) {
	tests := []struct {
		input    string
		wantLen  int
		wantByte byte // first byte = varint length+1
	}{
		{"", 1, 0x01},      // empty: length 0 → encode 1
		{"hi", 3, 0x03},    // "hi" (2 bytes) → encode 3, then 2 bytes
		{"hello", 6, 0x06}, // "hello" (5 bytes) → encode 6
	}
	for _, tt := range tests {
		enc := NewEncoder()
		enc.WriteCompactString(tt.input)
		got := enc.Bytes()
		if len(got) != tt.wantLen {
			t.Errorf("WriteCompactString(%q): got len %d, want %d", tt.input, len(got), tt.wantLen)
			continue
		}
		if got[0] != tt.wantByte {
			t.Errorf("WriteCompactString(%q): first byte = 0x%02x, want 0x%02x", tt.input, got[0], tt.wantByte)
		}
	}
}

// TestWriteUnsignedVarint verifies multi-byte varint encoding.
func TestWriteUnsignedVarint(t *testing.T) {
	tests := []struct {
		input uint64
		want  []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x01}}, // two-byte varint
		{300, []byte{0xac, 0x02}},
	}
	for _, tt := range tests {
		enc := NewEncoder()
		enc.WriteUnsignedVarint(tt.input)
		got := enc.Bytes()
		if len(got) != len(tt.want) {
			t.Errorf("WriteUnsignedVarint(%d): got %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range tt.want {
			if got[i] != tt.want[i] {
				t.Errorf("WriteUnsignedVarint(%d): byte[%d] = 0x%02x, want 0x%02x", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

// TestEncoderDecoderRoundtrip encodes a compact string and reads it back via Decoder.
func TestEncoderDecoderRoundtrip(t *testing.T) {
	words := []string{"", "kafka", "hello world", "topic-name-0"}
	for _, word := range words {
		enc := NewEncoder()
		enc.WriteCompactString(word)
		dec := NewDecoder(enc.Bytes())
		got, err := dec.ReadCompactString()
		if err != nil {
			t.Errorf("Roundtrip(%q): decode error: %v", word, err)
			continue
		}
		if got != word {
			t.Errorf("Roundtrip(%q): got %q", word, got)
		}
	}
}

// TestWriteUUID verifies 16 raw bytes are written.
func TestWriteUUID(t *testing.T) {
	enc := NewEncoder()
	var uuid [16]byte
	for i := range uuid {
		uuid[i] = byte(i)
	}
	enc.WriteUUID(uuid)
	got := enc.Bytes()
	if len(got) != 16 {
		t.Fatalf("WriteUUID: got %d bytes, want 16", len(got))
	}
	for i, b := range got {
		if b != byte(i) {
			t.Errorf("WriteUUID: byte[%d] = 0x%02x, want 0x%02x", i, b, byte(i))
		}
	}
}
