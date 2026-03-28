package apiversions

import (
	"encoding/binary"
	"testing"
)

func TestBuildBody_NoError(t *testing.T) {
	body := BuildBody(0) // ErrNone

	if len(body) < 3 {
		t.Fatalf("body too short: %d bytes", len(body))
	}

	// First 2 bytes: error_code (INT16) should be 0
	errCode := int16(binary.BigEndian.Uint16(body[0:2]))
	if errCode != 0 {
		t.Errorf("error_code = %d, want 0", errCode)
	}

	// Third byte: compact array length (N+1)
	arrayLen := int(body[2])
	if arrayLen < 2 {
		t.Errorf("api array compact length = %d, want >= 2 (at least 1 API)", arrayLen)
	}

	// Verify all 5 supported APIs are present by scanning entries.
	// Each entry: api_key(2) + min_version(2) + max_version(2) + TAG_BUFFER(1) = 7 bytes
	numAPIs := arrayLen - 1
	if numAPIs != 5 {
		t.Errorf("number of APIs = %d, want 5", numAPIs)
	}

	expectedKeys := map[int16]bool{18: false, 75: false, 1: false, 0: false, 3: false}
	pos := 3
	for i := 0; i < numAPIs && pos+6 < len(body); i++ {
		key := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		if _, ok := expectedKeys[key]; ok {
			expectedKeys[key] = true
		}
		pos += 7 // 2+2+2+1
	}

	for key, found := range expectedKeys {
		if !found {
			t.Errorf("API key %d not found in response", key)
		}
	}
}

func TestBuildBody_UnsupportedVersion(t *testing.T) {
	body := BuildBody(35) // ErrUnsupportedVersion

	errCode := int16(binary.BigEndian.Uint16(body[0:2]))
	if errCode != 35 {
		t.Errorf("error_code = %d, want 35", errCode)
	}
}
