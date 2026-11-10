package metadata

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/storage"
)

type observedSegmentFile struct {
	*os.File
	events     *[]string
	failure    string
	fault      error
	closeCalls int
}

func (f *observedSegmentFile) Write(data []byte) (int, error) {
	*f.events = append(*f.events, "write")
	if f.failure == "write" {
		return 0, f.fault
	}
	if f.failure == "short write" {
		return f.File.Write(data[:len(data)/2])
	}
	return f.File.Write(data)
}

func (f *observedSegmentFile) Sync() error {
	*f.events = append(*f.events, "file sync")
	if f.failure == "file sync" {
		return f.fault
	}
	return f.File.Sync()
}

func (f *observedSegmentFile) Close() error {
	f.closeCalls++
	*f.events = append(*f.events, "close")
	err := f.File.Close()
	if f.failure == "close" {
		return errors.Join(err, f.fault)
	}
	return err
}

func TestSegmentDurabilityOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new-parent", "partition")
	var events []string
	var files []*observedSegmentFile
	var flags []int
	fs := &segmentIO{
		openFile: func(path string, flag int, mode os.FileMode) (segmentFile, error) {
			events = append(events, "open")
			flags = append(flags, flag)
			f, err := os.OpenFile(path, flag, mode)
			if err != nil {
				return nil, err
			}
			observed := &observedSegmentFile{File: f, events: &events}
			files = append(files, observed)
			return observed, nil
		},
		syncDirectory: func(path string) error {
			events = append(events, "directory sync")
			if path != dir {
				t.Errorf("sync directory=%q, want partition directory %q", path, dir)
			}
			return storage.SyncDirectory(path)
		},
	}
	for i := int64(0); i < 2; i++ {
		events = nil
		_, _, err := writeRecords(dir, testRecordBatch(t, "record"), i, i, fs)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"open", "write", "file sync", "close"}
		if i == 0 {
			want = append(want, "directory sync")
		}
		if !slices.Equal(events, want) {
			t.Fatalf("append %d events=%v, want %v", i, events, want)
		}
		if got := files[i].closeCalls; got != 1 {
			t.Fatalf("append %d closed %d times, want once", i, got)
		}
	}
	if flags[0]&(os.O_CREATE|os.O_EXCL) != os.O_CREATE|os.O_EXCL || flags[1]&os.O_CREATE != 0 {
		t.Fatalf("new/existing segment open flags=%v", flags)
	}
}

func TestSegmentDurabilityErrorsCloseOnce(t *testing.T) {
	for _, failure := range []string{"write", "short write", "file sync", "close", "directory sync"} {
		t.Run(failure, func(t *testing.T) {
			fault := errors.New("injected " + failure)
			var events []string
			var file *observedSegmentFile
			fs := &segmentIO{
				openFile: func(path string, flag int, mode os.FileMode) (segmentFile, error) {
					f, err := os.OpenFile(path, flag, mode)
					if err != nil {
						return nil, err
					}
					file = &observedSegmentFile{File: f, events: &events, failure: failure, fault: fault}
					return file, nil
				},
				syncDirectory: func(string) error {
					events = append(events, "directory sync")
					if failure == "directory sync" {
						return fault
					}
					return nil
				},
			}
			_, _, err := writeRecords(t.TempDir(), testRecordBatch(t, "record"), 0, 0, fs)
			want := fault
			if failure == "short write" {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) {
				t.Fatalf("error=%v, want %v", err, want)
			}
			if file.closeCalls != 1 {
				t.Fatalf("file closed %d times, want once", file.closeCalls)
			}
			if failure != "directory sync" && slices.Contains(events, "directory sync") {
				t.Fatal("directory sync ran after an earlier file failure")
			}
		})
	}
}

func TestRolledSegmentDirectoryFailureFencesLeader(t *testing.T) {
	_, p := producePartition(t, 1)
	options := ProduceOptions{BrokerID: 1, Acks: 1, MinISR: 1}
	if _, err := p.ProduceRecords(testRecordBatch(t, "first"), options); err != nil {
		t.Fatal(err)
	}
	previousLimit := MaxSegmentBytes
	MaxSegmentBytes = 1
	t.Cleanup(func() { MaxSegmentBytes = previousLimit })
	fault := errors.New("directory sync failed")
	syncCalls := 0
	p.logIO = &segmentIO{syncDirectory: func(string) error { syncCalls++; return fault }}
	if offset, err := p.ProduceRecords(testRecordBatch(t, "second"), options); offset != -1 || !errors.Is(err, fault) {
		t.Fatalf("offset=%d err=%v, want failed durable publication", offset, err)
	}
	if p.NextOffset != 1 || p.GetLogEndOffset() != 1 || p.GetReplicaLEO(1) != 1 || p.GetHighWatermark() != 1 {
		t.Fatal("failed directory sync published leader offsets")
	}
	if _, err := os.Stat(filepath.Join(p.LogDirForBroker(1), segmentNameForOffset(1))); err != nil {
		t.Fatalf("ambiguous append was destructively removed: %v", err)
	}
	if _, err := p.ProduceRecords(testRecordBatch(t, "retry"), options); err == nil || syncCalls != 1 {
		t.Fatalf("partition accepted retry after durability failure: err=%v syncCalls=%d", err, syncCalls)
	}
}

func TestRolledSegmentWaitsForDirectoryBeforePublication(t *testing.T) {
	_, p := producePartition(t, 1)
	options := ProduceOptions{BrokerID: 1, Acks: 1, MinISR: 1}
	if _, err := p.ProduceRecords(testRecordBatch(t, "first"), options); err != nil {
		t.Fatal(err)
	}
	previousLimit := MaxSegmentBytes
	MaxSegmentBytes = 1
	t.Cleanup(func() { MaxSegmentBytes = previousLimit })
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("append goroutine did not exit after releasing directory sync")
		}
	})
	p.logIO = &segmentIO{syncDirectory: func(path string) error {
		// This callback runs under p.mu, before the append can publish state.
		if p.NextOffset != 1 || p.LogEndOffset != 1 || p.ReplicaLEO[1] != 1 || p.HighWatermark != 1 {
			t.Error("offsets advanced before directory durability completed")
		}
		close(entered)
		<-release
		return storage.SyncDirectory(path)
	}}
	result := make(chan produceOutcome, 1)
	batch := testRecordBatch(t, "second")
	go func() {
		defer close(finished)
		offset, err := p.ProduceRecords(batch, options)
		result <- produceOutcome{offset: offset, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("append never reached new segment directory sync")
	}
	select {
	case got := <-result:
		t.Fatalf("append returned before directory sync completed: %+v", got)
	default:
	}
	unblock()
	select {
	case got := <-result:
		if got.err != nil || got.offset != 1 || p.GetLogEndOffset() != 2 || p.GetHighWatermark() != 2 {
			t.Fatalf("append did not publish after directory sync: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("append did not finish after directory sync")
	}
}

func TestRolledSegmentDirectoryFailureFencesFollower(t *testing.T) {
	_, p := producePartition(t, 2)
	options := ProduceOptions{BrokerID: 1, Acks: 1, MinISR: 1}
	if _, err := p.ProduceRecords(testRecordBatch(t, "first"), options); err != nil {
		t.Fatal(err)
	}
	first := readLogFile(t, p.LogDirForBroker(1))
	if err := p.AppendFetchedRecords(2, 1, 0, 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceRecords(testRecordBatch(t, "second"), options); err != nil {
		t.Fatal(err)
	}
	leaderData := readLogFile(t, p.LogDirForBroker(1))
	second := leaderData[len(first):]
	previousLimit := MaxSegmentBytes
	MaxSegmentBytes = 1
	t.Cleanup(func() { MaxSegmentBytes = previousLimit })
	fault := errors.New("follower directory sync failed")
	syncCalls := 0
	p.logIO = &segmentIO{syncDirectory: func(string) error { syncCalls++; return fault }}
	if err := p.AppendFetchedRecords(2, 1, 0, 1, second); !errors.Is(err, fault) {
		t.Fatalf("follower append error=%v, want directory sync failure", err)
	}
	if p.GetReplicaLEO(2) != 1 || slices.Contains(p.ISRNodes, 2) || p.ReplicaAvailable(2) {
		t.Fatal("failed follower durability was counted as successful replication")
	}
	if err := p.AppendFetchedRecords(2, 1, 0, 1, second); err == nil || syncCalls != 1 {
		t.Fatalf("follower accepted retry after durability failure: err=%v syncCalls=%d", err, syncCalls)
	}
}
