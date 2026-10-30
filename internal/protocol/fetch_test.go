package protocol

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func fetch16Body(client string, tags []byte) []byte {
	e := NewEncoder()
	e.WriteString(client)
	// Unknown header tag must not misalign the body.
	e.WriteUnsignedVarint(1)
	e.WriteUnsignedVarint(5)
	e.WriteUnsignedVarint(2)
	e.WriteByte(12)
	e.WriteByte(34)
	e.WriteInt32(0)
	e.WriteInt32(1)
	e.WriteInt32(1024)
	e.WriteByte(0)
	e.WriteInt32(0)
	e.WriteInt32(-1)
	e.WriteUnsignedVarint(2)
	e.WriteUUID([16]byte{1})
	e.WriteUnsignedVarint(2)
	e.WriteInt32(3)
	e.WriteInt32(-1)
	e.WriteInt64(42)
	e.WriteInt32(-1)
	e.WriteInt64(0)
	e.WriteInt32(123)
	e.WriteTagBuffer()
	e.WriteTagBuffer()
	e.WriteUnsignedVarint(1)
	e.WriteCompactString("")
	return append(e.Bytes(), tags...)
}

func TestFetch16ConsumerAndReplicaTags(t *testing.T) {
	body := fetch16Body(string(bytes.Repeat([]byte("x"), 300)), []byte{0})
	req, err := ParseFetchRequest(makeRequestHeader(APIKeyFetch, 16, body))
	if err != nil {
		t.Fatal(err)
	}
	if req.ReplicaID != -1 || req.Topics[0].Partitions[0].MaxBytes != 123 || req.Topics[0].Partitions[0].FetchOffset != 42 {
		t.Fatal("consumer identity or partition byte limit was lost")
	}
	state := NewEncoder()
	state.WriteInt32(2)
	state.WriteInt64(-1)
	state.WriteTagBuffer()
	tags := NewEncoder()
	tags.WriteUnsignedVarint(2)
	tags.WriteUnsignedVarint(1)
	tags.WriteUnsignedVarint(uint64(len(state.Bytes())))
	tagBytes := append(tags.Bytes(), state.Bytes()...)
	tagBytes = append(tagBytes, 7, 1, 99) // unknown body tag
	req, err = ParseFetchRequest(makeRequestHeader(APIKeyFetch, 16, fetch16Body("", tagBytes)))
	if err != nil {
		t.Fatal(err)
	}
	if req.ReplicaID != 2 {
		t.Fatalf("replica=%d, want 2", req.ReplicaID)
	}
	for _, invalid := range [][]byte{{1, 1, 0}, {1, 1, 13, 0}, {2, 7, 0, 7, 0}, {1, 2, 0, 9}} {
		if _, err := ParseFetchRequest(makeRequestHeader(APIKeyFetch, 16, fetch16Body("", invalid))); err == nil {
			t.Fatalf("malformed tags %x accepted", invalid)
		}
	}
	for _, version := range []int16{1, 12, 15, 17} {
		if _, err := ParseFetchRequest(makeRequestHeader(APIKeyFetch, version, body)); err == nil {
			t.Fatalf("unsupported Fetch version %d accepted", version)
		}
	}
}

func TestFetchV0RetainsLimits(t *testing.T) {
	e := NewEncoder()
	e.WriteNullableString(nil)
	e.WriteInt32(-1)
	e.WriteInt32(0)
	e.WriteInt32(0)
	e.WriteArrayLen(1)
	e.WriteString("events")
	e.WriteArrayLen(1)
	e.WriteInt32(0)
	e.WriteInt64(0)
	e.WriteInt32(31)
	data := e.Bytes()
	req, err := ParseFetchRequestV0(makeRequestHeader(APIKeyFetch, 0, data))
	if err != nil {
		t.Fatal(err)
	}
	if req.ReplicaID != -1 || req.MinBytes != 0 || req.Topics[0].Partitions[0].MaxBytes != 31 {
		t.Fatal("v0 fetch limits were not retained")
	}
	binary.BigEndian.PutUint32(data[len(data)-4:], math.MaxUint32)
	if _, err := ParseFetchRequestV0(makeRequestHeader(APIKeyFetch, 0, data)); err == nil {
		t.Fatal("negative byte limit accepted")
	}
}

func TestTaggedFieldsBounds(t *testing.T) {
	for _, data := range [][]byte{{1, 1, 5, 0}, {2, 2, 0, 1, 0}, bytes.Repeat([]byte{0xff}, 11)} {
		if err := NewDecoder(data).ReadTaggedFields(nil); err == nil {
			t.Fatalf("malformed tags %x accepted", data)
		}
	}
	overflow := binary.AppendUvarint(nil, math.MaxUint64)
	if _, err := NewDecoder(overflow).ReadCompactString(); err == nil {
		t.Fatal("overflowing compact length accepted")
	}
	if _, err := NewDecoder(overflow).ReadCompactBytes(); err == nil {
		t.Fatal("overflowing byte length accepted")
	}
}

func FuzzFetchRequests(f *testing.F) {
	f.Add(fetch16Body("client", []byte{0}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = ParseFetchRequest(makeRequestHeader(APIKeyFetch, 16, body))
		_, _ = ParseFetchRequestV0(makeRequestHeader(APIKeyFetch, 0, body))
	})
}
