package fetch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/memory"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

func waitMemory(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("memory precondition was not reached")
}

func TestFetchSoftLimitAndOversizedFirstBatch(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		m, topic := fetchFixture(t, 1, 1)
		p := &topic.Partitions[0]
		size := 9 << 20
		if oversized {
			size = 20 << 20
		}
		first := appendValues(t, p, strings.Repeat("x", size))
		appendValues(t, p, "next")
		if !oversized {
			appendValues(t, p, strings.Repeat("y", size))
		}
		pool, err := memory.NewPool(512<<20, 2, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		req := requestFor(topic, 0)
		req.MaxBytes = 64 << 20
		req.Topics[0].Partitions[0].MaxBytes = 64 << 20
		payload, err := BuildWithMemory(context.Background(), req, m, 1, true, MemoryPolicy{Pool: pool, MaxDataBytes: 16 << 20, MaxEntries: 16})
		if err != nil {
			t.Fatal(err)
		}
		parts := decodeResults(t, payload.Body, true)
		if len(parts) != 1 || parts[0].errCode != 0 {
			t.Fatal("fetch failed")
		}
		if oversized {
			if len(parts[0].records) != len(first) {
				t.Fatal("first-batch exception was truncated or included extra records")
			}
		} else if len(parts[0].records) > 16<<20 || len(parts[0].records) < len(first) {
			t.Fatal("normal fetch did not respect the 16 MiB target")
		}
		if s := pool.Stats(); s.Used < int64(2*len(parts[0].records)) || s.Peak > 512<<20 {
			t.Fatalf("unaccounted response buffers: %+v", s)
		}
		payload.Release()
		if pool.Stats().Used != 0 {
			t.Fatal("response reservation leaked")
		}
	}
}

func TestFetchGrowthReleasesBeforeWaitingAndReadsAfterAdmission(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	p := &topic.Partitions[0]
	data := appendValues(t, p, strings.Repeat("x", 20000))
	const capacity = int64(256 << 20)
	pool, _ := memory.NewPool(capacity, 2, time.Second)
	// One topic, one partition, sixteen maximum batch plans.
	initial := int64(1024 + 512*18 + 1024 + 64 + len(topic.Name) + 64)
	held, err := pool.Acquire(context.Background(), capacity-initial)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	type result struct {
		payload *Payload
		err     error
	}
	done := make(chan result, 1)
	go func() {
		payload, err := BuildWithMemory(context.Background(), requestFor(topic, 0), m, 1, true, MemoryPolicy{Pool: pool, MaxDataBytes: 128, MaxEntries: 16})
		done <- result{payload, err}
	}()
	waitMemory(t, func() bool { return pool.Stats().Waiting == 1 })
	if pool.Stats().Used != capacity-initial {
		t.Fatal("fetch waited for growth while retaining its smaller reservation")
	}
	// Corrupt the payload while the request is waiting for its full reservation.
	// A premature payload read would incorrectly return the old valid data.
	path := filepath.Join(p.LogDirForBroker(1), "00000000000000000000.log")
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, int64(len(data)-2)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	held.Release()
	select {
	case outcome := <-done:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		parts := decodeResults(t, outcome.payload.Body, true)
		if parts[0].errCode != protocol.ErrUnknownServerError || len(parts[0].records) != 0 {
			t.Fatal("payload was read before memory admission")
		}
		outcome.payload.Release()
	case <-time.After(time.Second):
		t.Fatal("memory growth deadlocked")
	}
	if s := pool.Stats(); s.Used != 0 || s.Waiting != 0 {
		t.Fatalf("fetch memory leak: %+v", s)
	}
}

func TestFetchAdmissionCancellation(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	pool, _ := memory.NewPool(256<<20, 1, time.Second)
	held, _ := pool.Acquire(context.Background(), 256<<20)
	defer held.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := BuildWithMemory(ctx, requestFor(topic, 0), m, 1, true, MemoryPolicy{Pool: pool, MaxDataBytes: 1024, MaxEntries: 16})
		done <- err
	}()
	waitMemory(t, func() bool { return pool.Stats().Waiting == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s := pool.Stats(); s.Used != 256<<20 || s.Waiting != 0 {
		t.Fatal("cancellation released another owner's memory")
	}
}

func TestLegacyFetchProgressBeyondServerSoftTarget(t *testing.T) {
	m, topic := fetchFixture(t, 1, 1)
	data := appendValues(t, &topic.Partitions[0], strings.Repeat("x", 1024))
	pool, _ := memory.NewPool(256<<20, 1, time.Second)
	req := requestFor(topic, 0)
	req.Topics[0].Partitions[0].MaxBytes = int32(len(data))
	payload, err := BuildWithMemory(context.Background(), req, m, 1, false, MemoryPolicy{Pool: pool, MaxDataBytes: 64, MaxEntries: 16})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeResults(t, payload.Body, false)[0].records; len(got) != len(data) {
		t.Fatal("server soft target stalled a valid legacy batch")
	}
	payload.Release()
	req.Topics[0].Partitions[0].MaxBytes = int32(len(data) - 1)
	payload, err = BuildWithMemory(context.Background(), req, m, 1, false, MemoryPolicy{Pool: pool, MaxDataBytes: 64, MaxEntries: 16})
	if err != nil {
		t.Fatal(err)
	}
	if len(decodeResults(t, payload.Body, false)[0].records) != 0 {
		t.Fatal("legacy client byte limit was exceeded")
	}
	payload.Release()
}
