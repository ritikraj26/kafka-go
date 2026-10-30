package metadata

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFetchLogStartAndReadFailure(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, NextOffset: 7, BrokerLogDirs: map[int32]string{1: dir}}
	if _, err := p.AppendRecords(testRecordBatch(t, "a", "b"), dir); err != nil {
		t.Fatal(err)
	}
	options := FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, Offset: 7, MaxBytes: 1024}
	result, err := p.FetchRecords(options)
	if err != nil || result.LogStart != 7 || result.HighWatermark != 9 || len(result.Records) == 0 {
		t.Fatalf("fetch result=%+v err=%v", result, err)
	}
	options.Offset = 6
	if _, err := p.FetchRecords(options); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("offset before log start: %v", err)
	}
	options.Offset = 7
	if err := os.Remove(filepath.Join(dir, segmentNameForOffset(7))); err != nil {
		t.Fatal(err)
	}
	if _, err := p.FetchRecords(options); err == nil {
		t.Fatal("missing log returned a success-shaped empty response")
	}
}

func TestFetchRejectsInvalidIndex(t *testing.T) {
	for _, data := range [][]byte{{0}, {0, 0, 0, 1, 0, 0, 0, 0}, {0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}} {
		dir := t.TempDir()
		p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, BrokerLogDirs: map[int32]string{1: dir}}
		if _, err := p.AppendRecords(testRecordBatch(t, "a", "b"), dir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.index"), data, 0600); err != nil {
			t.Fatal(err)
		}
		options := FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, Offset: 1, MaxBytes: 1024}
		if result, err := p.FetchRecords(options); err == nil || len(result.Records) != 0 {
			t.Fatalf("invalid index returned records: %v", err)
		}
	}
}

func TestFetchRejectsOversizedDiskLengthWithoutAllocating(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, BrokerLogDirs: map[int32]string{1: dir}}
	if _, err := p.AppendRecords(testRecordBatch(t, "a"), dir); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, segmentNameForOffset(0)), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 1<<30)
	if _, err := f.WriteAt(length[:], 8); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.FetchRecords(FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, MaxBytes: 1, AllowOversizedBatch: true}); err == nil {
		t.Fatal("oversized on-disk length accepted")
	}
}

func TestFetchRejectsStaleFollowerResponse(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir, 1, 2)
	topic := createPersistentTopic(t, m, "events", 1)
	p := &topic.Partitions[0]
	if _, err := p.AppendRecords(testRecordBatch(t, "a"), p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	result, err := p.FetchRecords(FetchOptions{BrokerID: 1, ReplicaID: 2, LeaderEpoch: 0, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ElectLeader("events", 0); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendFetchedRecords(2, 1, 0, 0, result.Records); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale response was appended after election: %v", err)
	}
	if got := readLogFile(t, p.LogDirForBroker(2)); len(got) != 0 {
		t.Fatal("stale response changed the new leader's log")
	}
}
