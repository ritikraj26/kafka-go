package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProduceContextCancelsReplicationWait(t *testing.T) {
	_, p := producePartition(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := allAcks()
	options.Deadline = time.Now().Add(time.Hour)
	batch := testRecordBatch(t, "a", "b", "c")
	result := make(chan produceOutcome, 1)
	go func() {
		offset, err := p.ProduceRecordsContext(ctx, batch, options)
		result <- produceOutcome{offset, err}
	}()
	deadline := time.Now().Add(time.Second)
	pending := false
	for time.Now().Before(deadline) {
		p.mu.Lock()
		pending = len(p.pendingProduces) == 1
		p.mu.Unlock()
		if pending {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !pending {
		t.Fatal("produce did not enter acknowledgement wait")
	}
	cancel()
	select {
	case outcome := <-result:
		if outcome.offset != 0 || !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("outcome=%+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("produce waited for client timeout instead of cancellation")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingProduces) != 0 || p.LogEndOffset != 3 {
		t.Fatal("cancellation leaked waiter or removed appended records")
	}
}

func TestCancelledProduceDoesNotStartDiskWrite(t *testing.T) {
	_, p := producePartition(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if offset, err := p.ProduceRecordsContext(ctx, testRecordBatch(t, "a"), allAcks()); offset != -1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission: offset=%d err=%v", offset, err)
	}
	if p.GetLogEndOffset() != 0 {
		t.Fatal("cancelled request appended data")
	}
}
