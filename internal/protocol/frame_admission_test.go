package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadingSizeDoesNotAllocateDeclaredPayload(t *testing.T) {
	prefix := binary.BigEndian.AppendUint32(nil, uint32(MaxRequestSize))
	result := testing.Benchmark(func(b *testing.B) {
		var reader bytes.Reader
		header := NewRequestHeader()
		for i := 0; i < b.N; i++ {
			reader.Reset(prefix)
			size, err := header.ReadSize(&reader)
			if err != nil || size != MaxRequestSize || header.GetBody() != nil {
				b.Fatal("size reader consumed or exposed a frame")
			}
		}
	})
	if allocated := result.AllocedBytesPerOp(); allocated > 1024 {
		t.Fatalf("reading a four-byte size prefix allocated %d bytes for a %d-byte declared frame", allocated, MaxRequestSize)
	}
}
