package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/ritiraj/kafka-go/internal/storage"
)

func TestDecoderExactEntryAndByteCharges(t *testing.T) {
	d, err := NewDecoderWithLimits(nil, DefaultDecodeLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ChargeEntries(10000); err != nil {
		t.Fatal(err)
	}
	if err := d.ChargeEntries(1); !errors.Is(err, ErrDecodeLimit) {
		t.Fatal("entry ceiling exceeded")
	}
	e := NewEncoder()
	e.WriteCompactString("1234")
	d, err = NewDecoderWithLimits(e.Bytes(), DecodeLimits{MaxBytes: 1096, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if value, err := d.ReadCompactString(); err != nil || value != "1234" {
		t.Fatal(err)
	}
	if d.budget.bytes != 1096 {
		t.Fatalf("byte charges=%d", d.budget.bytes)
	}
	d, _ = NewDecoderWithLimits(e.Bytes(), DecodeLimits{MaxBytes: 1095, MaxEntries: 1})
	if _, err := d.ReadCompactString(); !errors.Is(err, ErrDecodeLimit) {
		t.Fatal("byte ceiling exceeded")
	}
}

func TestDecoderRejectsCopyBeforeAllocation(t *testing.T) {
	e := NewEncoder()
	e.WriteCompactBytes(make([]byte, 1<<20))
	data := e.Bytes()
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, err := NewDecoderWithLimits(data, DecodeLimits{MaxBytes: 1024, MaxEntries: 1})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := d.ReadCompactBytes(); !errors.Is(err, ErrDecodeLimit) {
				b.Fatal("copy was not rejected")
			}
		}
	})
	if n := result.AllocedBytesPerOp(); n > 1024 {
		t.Fatalf("rejected 1 MiB field allocated %d bytes", n)
	}
}

func TestProducePayloadBorrowsRawFrameAndCountsRecords(t *testing.T) {
	records := make([]storage.Record, 10000)
	for i := range records {
		records[i].Value = []byte("a")
	}
	batch, err := storage.EncodeRecordBatch(0, records)
	if err != nil {
		t.Fatal(err)
	}
	makeBody := func(data []byte) []byte {
		e := NewEncoder()
		e.WriteString("client")
		e.WriteTagBuffer()
		e.WriteByte(0)
		e.WriteInt16(1)
		e.WriteInt32(1000)
		e.WriteUnsignedVarint(2)
		e.WriteCompactString("events")
		e.WriteUnsignedVarint(2)
		e.WriteInt32(0)
		e.WriteCompactBytes(data)
		e.WriteTagBuffer()
		e.WriteTagBuffer()
		e.WriteTagBuffer()
		return e.Bytes()
	}
	h := makeRequestHeader(APIKeyProduce, 11, makeBody(batch))
	if _, err := ParseRequest(h); !errors.Is(err, ErrDecodeLimit) {
		t.Fatalf("record expansion exceeded aggregate entry limit: %v", err)
	}
	single, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: bytes.Repeat([]byte("x"), 1<<20)}})
	if err != nil {
		t.Fatal(err)
	}
	body := makeBody(single)
	h = makeRequestHeader(APIKeyProduce, 11, body)
	req, err := ParseProduceRequest(h)
	if err != nil {
		t.Fatal(err)
	}
	if h.DecodedBytes() > 4096 {
		t.Fatalf("borrowed record payload was charged as a copy: %d", h.DecodedBytes())
	}
	position := bytes.Index(body, single)
	if position < 0 || &req.Topics[0].Partitions[0].Records[0] != &body[position] {
		t.Fatal("produce payload was copied instead of borrowed")
	}
}

type responseWriter struct {
	body  []byte
	calls int
}

func (w *responseWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == 2 && len(data) > 0 && &data[0] != &w.body[0] {
		return 0, errors.New("response body copied")
	}
	return len(data), nil
}

func TestResponseStreamsExistingBody(t *testing.T) {
	body := make([]byte, 1<<20)
	writer := &responseWriter{body: body}
	response := NewResponseV1(42, body)
	if n, err := response.WriteTo(writer); err != nil || n != int64(len(body)+9) || writer.calls != 2 {
		t.Fatalf("stream: n=%d err=%v", n, err)
	}
	var frame bytes.Buffer
	if _, err := response.WriteTo(&frame); err != nil {
		t.Fatal(err)
	}
	serialized, err := response.Serialize()
	if err != nil || !bytes.Equal(serialized, frame.Bytes()) {
		t.Fatal("streaming changed wire format")
	}
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := response.WriteTo(io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
	if n := result.AllocedBytesPerOp(); n > 1024 {
		t.Fatalf("streaming copied a full response: %d bytes/op", n)
	}
}

func TestFixedEncoderCannotGrow(t *testing.T) {
	e := NewFixedEncoder(4)
	e.WriteInt32(42)
	e.WriteByte(1)
	if e.Err() == nil || len(e.Bytes()) != 4 || cap(e.Bytes()) != 4 {
		t.Fatal("encoder grew beyond its reservation")
	}
}

func TestByteDecodingPreservesNullAndEmpty(t *testing.T) {
	for _, compact := range []bool{false, true} {
		e := NewEncoder()
		if compact {
			e.WriteByte(0)
			e.WriteCompactBytes([]byte{})
		} else {
			e.WriteBytes(nil)
			e.WriteBytes([]byte{})
		}
		d, _ := NewDecoderWithLimits(e.Bytes(), DefaultDecodeLimits())
		var null, empty []byte
		var err error
		if compact {
			null, err = d.ReadCompactBytes()
		} else {
			null, err = d.ReadBytes()
		}
		if err != nil || null != nil {
			t.Fatalf("null field changed: %v", err)
		}
		if compact {
			empty, err = d.ReadCompactBytes()
		} else {
			empty, err = d.ReadBytes()
		}
		if err != nil || empty == nil || len(empty) != 0 {
			t.Fatalf("empty field became null: %v", err)
		}
	}
}
