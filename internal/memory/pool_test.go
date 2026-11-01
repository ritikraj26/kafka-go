package memory

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReservationResizeAndRelease(t *testing.T) {
	p, err := NewPool(100, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a, err := p.Acquire(context.Background(), 60)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Acquire(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	if a.Resize(61) {
		t.Fatal("growth exceeded pool capacity")
	}
	if !b.Resize(20) || !a.Resize(80) {
		t.Fatal("released bytes were not reusable")
	}
	if s := p.Stats(); s.Used != 100 || s.Peak != 100 {
		t.Fatalf("bad accounting: %+v", s)
	}
	a.Release()
	a.Release()
	b.Release()
	if a.Resize(1) || p.Stats().Used != 0 {
		t.Fatal("released reservation changed accounting")
	}
}

func TestCancelledPoolWaitReleasesNoOtherOwner(t *testing.T) {
	p, _ := NewPool(10, 1, time.Second)
	held, _ := p.Acquire(context.Background(), 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if s := p.Stats(); s.Used != 10 || s.Waiting != 0 {
		t.Fatalf("cancelled wait leaked: %+v", s)
	}
	held.Release()
}
