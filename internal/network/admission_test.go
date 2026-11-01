package network

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testAdmission(t *testing.T, connections int, bytes int64, wait time.Duration) *Admission {
	t.Helper()
	a, err := NewAdmission(Limits{MaxConnections: connections, MaxRequestBytes: bytes, MaxInFlightBytes: bytes, AdmissionWait: wait})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAdmissionDefaultsAndValidation(t *testing.T) {
	defaults := DefaultLimits()
	if defaults.MaxConnections != 256 || defaults.MaxInFlightBytes != 256<<20 || defaults.MaxRequestBytes != 100<<20 || defaults.AdmissionWait != 2*time.Second {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
	for _, modify := range []func(*Limits){
		func(l *Limits) { l.MaxConnections = 0 },
		func(l *Limits) { l.MaxRequestBytes = 7 },
		func(l *Limits) { l.MaxRequestBytes = 101 << 20 },
		func(l *Limits) { l.MaxInFlightBytes = l.MaxRequestBytes - 1 },
		func(l *Limits) { l.AdmissionWait = 0 },
		func(l *Limits) { l.AdmissionWait = 31 * time.Second },
	} {
		limits := defaults
		modify(&limits)
		if _, err := NewAdmission(limits); err == nil {
			t.Fatalf("invalid limits accepted: %+v", limits)
		}
	}
	replicas, err := NewReplicationAdmission(3)
	if err != nil {
		t.Fatal(err)
	}
	if replicas.limits.MaxConnections != 6 || replicas.limits.MaxInFlightBytes != 1<<20 || replicas.limits.MaxRequestBytes != 64<<10 {
		t.Fatalf("unexpected protected capacity: %+v", replicas.limits)
	}
}

func TestConnectionAdmissionExactLimitAndIdempotentRelease(t *testing.T) {
	a := testAdmission(t, 2, 64, time.Second)
	first, ok := a.tryConnection()
	if !ok {
		t.Fatal("first rejected")
	}
	second, ok := a.tryConnection()
	if !ok {
		t.Fatal("second rejected")
	}
	if _, ok := a.tryConnection(); ok {
		t.Fatal("connection ceiling exceeded")
	}
	first()
	first()
	third, ok := a.tryConnection()
	if !ok {
		t.Fatal("released capacity was not reusable")
	}
	second()
	third()
	stats := a.Stats()
	if stats.Connections != 0 || stats.PeakConnections != 2 || stats.RejectedConnections != 1 {
		t.Fatalf("incorrect connection accounting: %+v", stats)
	}
}

func TestDefaultProfileExactCeilings(t *testing.T) {
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var connections []func()
	for i := 0; i < 256; i++ {
		release, ok := a.tryConnection()
		if !ok {
			t.Fatalf("connection %d rejected below default limit", i+1)
		}
		connections = append(connections, release)
	}
	if _, ok := a.tryConnection(); ok {
		t.Fatal("257th connection was admitted")
	}
	for _, release := range connections {
		release()
	}
	var frames []func()
	for _, size := range []int64{100 << 20, 100 << 20, 56 << 20} {
		release, err := a.Reserve(context.Background(), size)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, release)
		defer release()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	extra := reserveAsync(a, ctx, 8)
	waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
	if stats := a.Stats(); stats.InFlightBytes != 256<<20 || stats.PeakInFlightBytes != 256<<20 || stats.PeakConnections != 256 {
		t.Fatalf("default ceilings were not exact: %+v", stats)
	}
	cancel()
	if outcome := <-extra; !errors.Is(outcome.err, context.Canceled) {
		t.Fatal("request beyond 256 MiB was not blocked")
	}
	for _, release := range frames {
		release()
	}
	if a.Stats().InFlightBytes != 0 {
		t.Fatal("default-profile reservation leak")
	}
}

type admissionResult struct {
	release func()
	err     error
}

func reserveAsync(a *Admission, ctx context.Context, bytes int64) <-chan admissionResult {
	result := make(chan admissionResult, 1)
	go func() { release, err := a.Reserve(ctx, bytes); result <- admissionResult{release, err} }()
	return result
}

func takeReservation(t *testing.T, result <-chan admissionResult) func() {
	t.Helper()
	select {
	case outcome := <-result:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		return outcome.release
	case <-time.After(time.Second):
		t.Fatal("reservation did not complete")
	}
	return nil
}

func TestRequestAdmissionExactByteLimit(t *testing.T) {
	a := testAdmission(t, 2, 64, time.Second)
	first, err := a.Reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.Reserve(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pending := reserveAsync(a, ctx, 8)
	waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
	if stats := a.Stats(); stats.InFlightBytes != 64 || stats.PeakInFlightBytes != 64 {
		t.Fatalf("byte ceiling: %+v", stats)
	}
	cancel()
	if outcome := <-pending; !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", outcome.err)
	}
	if a.Stats().InFlightBytes != 64 {
		t.Fatal("cancellation released another request's bytes")
	}
	first()
	second()
	second()
	if stats := a.Stats(); stats.InFlightBytes != 0 || stats.Waiting != 0 {
		t.Fatalf("byte leak: %+v", stats)
	}
}

func TestAdmissionFIFOAndCancelledHead(t *testing.T) {
	a := testAdmission(t, 2, 64, time.Second)
	held, err := a.Reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	large := reserveAsync(a, ctx, 50)
	waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
	small := reserveAsync(a, context.Background(), 8)
	waitLifecycle(t, func() bool { return a.Stats().Waiting == 2 })
	select {
	case <-small:
		t.Fatal("small request bypassed FIFO head")
	default:
	}
	if _, err := a.Reserve(context.Background(), 8); !errors.Is(err, ErrAdmissionQueueFull) {
		t.Fatalf("unbounded wait queue: %v", err)
	}
	cancel()
	if result := <-large; !errors.Is(result.err, context.Canceled) {
		t.Fatal(result.err)
	}
	release := takeReservation(t, small)
	if stats := a.Stats(); stats.InFlightBytes != 48 || stats.Waiting != 0 {
		t.Fatalf("head cancellation did not wake next request: %+v", stats)
	}
	release()
}

func TestAdmissionTimeoutAndOversizedRequest(t *testing.T) {
	a := testAdmission(t, 2, 64, 20*time.Millisecond)
	held, err := a.Reserve(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer held()
	start := time.Now()
	if _, err := a.Reserve(context.Background(), 8); !errors.Is(err, ErrAdmissionTimeout) {
		t.Fatalf("expected admission timeout: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond || elapsed > time.Second {
		t.Fatalf("incorrect wait duration %s", elapsed)
	}
	if _, err := a.Reserve(context.Background(), 65); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized request queued: %v", err)
	}
	if stats := a.Stats(); stats.InFlightBytes != 64 || stats.Waiting != 0 || stats.TimedOutRequests != 1 || stats.RejectedRequests != 2 {
		t.Fatalf("incorrect rejection accounting: %+v", stats)
	}
}

func TestCancellationRacingCapacityReleaseDoesNotLeak(t *testing.T) {
	a := testAdmission(t, 2, 64, time.Second)
	for i := 0; i < 100; i++ {
		held, err := a.Reserve(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := reserveAsync(a, ctx, 64)
		waitLifecycle(t, func() bool { return a.Stats().Waiting == 1 })
		var race sync.WaitGroup
		race.Add(2)
		go func() { defer race.Done(); held() }()
		go func() { defer race.Done(); cancel() }()
		race.Wait()
		outcome := <-result
		if outcome.err == nil {
			outcome.release()
		} else if !errors.Is(outcome.err, context.Canceled) {
			t.Fatal(outcome.err)
		}
		if stats := a.Stats(); stats.InFlightBytes != 0 || stats.Waiting != 0 || stats.PeakInFlightBytes > 64 {
			t.Fatalf("iteration %d leaked: %+v", i, stats)
		}
	}
}
