package metadata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ritiraj/kafka-go/internal/storage"
)

func indexEntryForTest(relative, position int32) []byte {
	entry := make([]byte, 8)
	binary.BigEndian.PutUint32(entry, uint32(relative))
	binary.BigEndian.PutUint32(entry[4:], uint32(position))
	return entry
}

func assertIndexFetch(t *testing.T, p *Partition, offset int64, want []byte) {
	t.Helper()
	got, err := p.FetchRecords(FetchOptions{BrokerID: p.LeaderID, ReplicaID: -1, LeaderEpoch: -1, Offset: offset, MaxBytes: 1 << 20})
	if err != nil || !bytes.Equal(got.Records, want) {
		t.Fatalf("fetch offset %d: bytes=%d want=%d err=%v", offset, len(got.Records), len(want), err)
	}
}

func TestIndexRecoveryInvalidHints(t *testing.T) {
	for name, index := range map[string][]byte{
		"torn entry":        append(indexEntryForTest(0, 0), 1, 2, 3),
		"negative offset":   indexEntryForTest(-1, 0),
		"negative position": indexEntryForTest(0, -1),
		"wrong base":        indexEntryForTest(1, 0),
		"middle of header":  indexEntryForTest(0, 1),
		"beyond EOF":        indexEntryForTest(1, 1<<20),
	} {
		t.Run(name, func(t *testing.T) {
			_, p := producePartition(t, 1)
			dir := p.LogDirForBroker(1)
			first := testRecordBatch(t, "first")
			for _, batch := range [][]byte{first, testRecordBatch(t, "second"), testRecordBatch(t, "third")} {
				if _, err := p.AppendRecords(batch, dir); err != nil {
					t.Fatal(err)
				}
			}
			want := readLogFile(t, dir)
			path := filepath.Join(dir, "00000000000000000000.index")
			if err := os.WriteFile(path, index, 0600); err != nil {
				t.Fatal(err)
			}
			assertIndexFetch(t, p, 1, want[len(first):])
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("unusable regular index was not discarded: %v", err)
			}
			assertIndexFetch(t, p, 0, want)
			if !bytes.Equal(readLogFile(t, dir), want) || p.GetLogEndOffset() != 3 {
				t.Fatal("index fallback changed authoritative records or offsets")
			}
		})
	}
}

func TestIndexRecoveryContinuesAppending(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "torn index"
		if missing {
			name = "missing index"
		}
		t.Run(name, func(t *testing.T) {
			_, p := producePartition(t, 1)
			dir := p.LogDirForBroker(1)
			if _, err := p.AppendRecords(testRecordBatch(t, "first"), dir); err != nil {
				t.Fatal(err)
			}
			first := readLogFile(t, dir)
			path := filepath.Join(dir, "00000000000000000000.index")
			if missing {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else {
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte{1, 2, 3}); err != nil {
					f.Close()
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, value := range []string{"second", "third"} {
				if _, err := p.AppendRecords(testRecordBatch(t, value), dir); err != nil {
					t.Fatal(err)
				}
			}
			want := readLogFile(t, dir)
			assertIndexFetch(t, p, 0, want)
			assertIndexFetch(t, p, 1, want[len(first):])
			index, err := os.ReadFile(path)
			if err != nil || len(index) != 16 || !bytes.Equal(index[:8], indexEntryForTest(1, int32(len(first)))) {
				t.Fatalf("subsequent writes did not form a valid sparse index: bytes=%v err=%v", index, err)
			}
		})
	}
}

type failingIndexFile struct {
	*os.File
	mode       string
	fault      error
	closeCalls int
}

func (f *failingIndexFile) Write(data []byte) (int, error) {
	if f.mode == "partial write" || f.mode == "short write" {
		n, err := f.File.Write(data[:3])
		if err != nil {
			return n, err
		}
		if f.mode == "partial write" {
			return n, f.fault
		}
		return n, nil
	}
	return f.File.Write(data)
}
func (f *failingIndexFile) Sync() error {
	if f.mode == "sync" {
		return f.fault
	}
	return f.File.Sync()
}
func (f *failingIndexFile) Close() error {
	f.closeCalls++
	err := f.File.Close()
	if f.mode == "close" {
		return errors.Join(err, f.fault)
	}
	return err
}

func TestIndexRecoveryWriteFailuresPreserveAcknowledgedLog(t *testing.T) {
	for _, mode := range []string{"partial write", "short write", "sync", "close"} {
		t.Run(mode, func(t *testing.T) {
			_, p := producePartition(t, 1)
			dir := p.LogDirForBroker(1)
			options := ProduceOptions{BrokerID: 1, Acks: 1, MinISR: 1}
			if _, err := p.ProduceRecords(testRecordBatch(t, "first"), options); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected index " + mode)
			var file *failingIndexFile
			p.indexIO = &indexIO{openFile: func(path string, flags int, modeBits os.FileMode) (indexFile, error) {
				f, err := os.OpenFile(path, flags, modeBits)
				if err != nil {
					return nil, err
				}
				file = &failingIndexFile{File: f, mode: mode, fault: fault}
				return file, nil
			}}
			if offset, err := p.ProduceRecords(testRecordBatch(t, "second"), options); err != nil || offset != 1 {
				t.Fatalf("durable log append rejected because of auxiliary index: offset=%d err=%v", offset, err)
			}
			if file == nil || file.closeCalls != 1 {
				t.Fatal("index file was not closed exactly once")
			}
			path := filepath.Join(dir, "00000000000000000000.index")
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("failed index was not discarded: %v", err)
			}
			want := readLogFile(t, dir)
			assertIndexFetch(t, p, 0, want)
			p.indexIO = nil
			if offset, err := p.ProduceRecords(testRecordBatch(t, "third"), options); err != nil || offset != 2 {
				t.Fatalf("index failure fenced healthy log writes: offset=%d err=%v", offset, err)
			}
			assertIndexFetch(t, p, 0, readLogFile(t, dir))
		})
	}
}

func TestIndexRecoveryNonregularPathsRemainUntouched(t *testing.T) {
	for _, kind := range []string{"directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			_, p := producePartition(t, 1)
			dir := p.LogDirForBroker(1)
			if _, err := p.AppendRecords(testRecordBatch(t, "first"), dir); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "00000000000000000000.index")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "sentinel")
			const sentinel = "unrelated data must remain unchanged"
			if err := os.WriteFile(target, []byte(sentinel), 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "child"), []byte(sentinel), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			if _, err := p.AppendRecords(testRecordBatch(t, "second"), dir); err != nil {
				t.Fatal(err)
			}
			assertIndexFetch(t, p, 0, readLogFile(t, dir))
			info, err := os.Lstat(path)
			if err != nil || info.Mode().IsRegular() {
				t.Fatalf("nonregular cache path was changed: %v", err)
			}
			got, err := os.ReadFile(target)
			if err != nil || string(got) != sentinel {
				t.Fatalf("unrelated symlink target was changed: %v", err)
			}
			if kind == "directory" {
				child, err := os.ReadFile(filepath.Join(path, "child"))
				if err != nil || string(child) != sentinel {
					t.Fatal("nonempty index directory was modified")
				}
			}
		})
	}
}

func TestIndexRecoveryAfterRestart(t *testing.T) {
	root := t.TempDir()
	m := persistentManager(t, root, 1)
	p := &createPersistentTopic(t, m, "events", 1).Partitions[0]
	dir := p.LogDirForBroker(1)
	for _, value := range []string{"first", "second"} {
		if _, err := p.AppendRecords(testRecordBatch(t, value), dir); err != nil {
			t.Fatal(err)
		}
	}
	want := readLogFile(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.index"), []byte{1, 2, 3}, 0600); err != nil {
		t.Fatal(err)
	}
	restarted := persistentManager(t, root, 1)
	if err := restarted.LoadTopicsFromDisk(root); err != nil {
		t.Fatal(err)
	}
	recovered := &restarted.GetTopic("events").Partitions[0]
	if recovered.GetLogEndOffset() != 2 {
		t.Fatal("damaged index changed recovered log end")
	}
	assertIndexFetch(t, recovered, 0, want)
	if !bytes.Equal(readLogFile(t, dir), want) {
		t.Fatal("index recovery changed durable records")
	}
	if offset, err := recovered.AppendRecords(testRecordBatch(t, "third"), dir); err != nil || offset != 2 {
		t.Fatalf("append after index recovery: offset=%d err=%v", offset, err)
	}
	assertIndexFetch(t, recovered, 0, readLogFile(t, dir))
}

func TestIndexRecoveryAcrossSegments(t *testing.T) {
	_, p := producePartition(t, 1)
	dir := p.LogDirForBroker(1)
	previous := MaxSegmentBytes
	MaxSegmentBytes = 1
	t.Cleanup(func() { MaxSegmentBytes = previous })
	var all, want []byte
	for i, value := range []string{"first", "second", "third"} {
		if _, err := p.AppendRecords(testRecordBatch(t, value), dir); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, segmentNameForOffset(int64(i))))
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, data...)
		if i > 0 {
			want = append(want, data...)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000001.index"), []byte{1}, 0600); err != nil {
		t.Fatal(err)
	}
	assertIndexFetch(t, p, 1, want)
	assertIndexFetch(t, p, 0, all)
}

func TestIndexRecoveryDoesNotMaskCorruptLog(t *testing.T) {
	root := t.TempDir()
	m := persistentManager(t, root, 1)
	p := &createPersistentTopic(t, m, "events", 1).Partitions[0]
	dir := p.LogDirForBroker(1)
	if _, err := p.AppendRecords(testRecordBatch(t, "first"), dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentNameForOffset(0))
	corrupt := readLogFile(t, dir)
	corrupt[17] ^= 1
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.index"), []byte{1}, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := p.FetchRecords(FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, MaxBytes: 1024}); !errors.Is(err, storage.ErrCorruptRecord) || len(got.Records) != 0 {
		t.Fatalf("index fallback hid log corruption: bytes=%d err=%v", len(got.Records), err)
	}
	if err := persistentManager(t, root, 1).LoadTopicsFromDisk(root); !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("restart accepted corrupt authoritative data: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, corrupt) {
		t.Fatal("index recovery modified corrupt log")
	}
}
