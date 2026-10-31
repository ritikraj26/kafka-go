package coordinator

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCloseAllowsInProgressOffsetSyncToFinish(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	c.openOffsetLog = func(path string) (offsetLogFile, error) {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		return &faultyOffsetFile{File: f, syncHook: func() error { close(entered); <-release; return f.Sync() }}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	committed := make(chan int16, 1)
	go func() {
		committed <- c.CommitOffsetWithMetadataContext(ctx, "group", member.GenerationID, member.MemberID, "events", 0, 3, nil)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach disk sync")
	}
	cancel()
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close returned while durable write was still in progress")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case code := <-committed:
		if code != 0 {
			t.Fatalf("in-progress durable commit failed: %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("commit did not finish")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not finish closing")
	}
	restarted := NewCoordinator()
	defer restarted.Close()
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	if offset, code := restarted.FetchOffset("group", "events", 0); code != 0 || offset != 3 {
		t.Fatal("shutdown discarded the completed offset commit")
	}
}
