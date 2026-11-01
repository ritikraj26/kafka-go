package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/memory"
)

func TestResponseEncoderExactCapAndNoPartialBytes(t *testing.T) {
	pool, err := memory.NewPool(128<<20, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e := NewResponseEncoder(context.Background(), pool, 16<<20)
	defer e.Release()
	e.WriteCompactBytes(make([]byte, (16<<20)-4))
	if e.Err() != nil || len(e.Bytes()) != 16<<20 || pool.Stats().Used != 16<<20 {
		t.Fatalf("exact response cap: err=%v stats=%+v", e.Err(), pool.Stats())
	}
	e.WriteByte(1)
	if e.Err() == nil || e.Bytes() != nil {
		t.Fatal("oversized encoder exposed a partial success body")
	}
	e.Release()
	if pool.Stats().Used != 0 {
		t.Fatal("response buffer reservation leaked")
	}
}

func TestResponseGrowthDoesNotWaitWhileHoldingCapacity(t *testing.T) {
	pool, _ := memory.NewPool(8192, 2, time.Second)
	held, _ := pool.Acquire(context.Background(), 4096)
	defer held.Release()
	e := NewResponseEncoder(context.Background(), pool, 8192)
	defer e.Release()
	e.WriteBytes(make([]byte, 100))
	start := time.Now()
	e.WriteBytes(make([]byte, 5000))
	if e.Err() == nil || time.Since(start) > 100*time.Millisecond {
		t.Fatal("encoder waited for growth while retaining memory")
	}
	if e.Bytes() != nil {
		t.Fatal("partial response escaped failed growth")
	}
	e.Release()
	if pool.Stats().Used != 4096 {
		t.Fatal("failed growth released another owner's memory")
	}
}

func TestDiscardEncoderNeverClaimsCapacity(t *testing.T) {
	e := NewDiscardEncoder()
	e.WriteCompactBytes(make([]byte, 1024))
	if e.Err() != nil || len(e.Bytes()) != 0 {
		t.Fatal("acks=0 discard encoder built a response")
	}
}
