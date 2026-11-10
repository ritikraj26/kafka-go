package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ritiraj/kafka-go/internal/storage"
)

func TestIndexHintEmbeddedBatchDoesNotBecomeRecord(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, BrokerLogDirs: map[int32]string{1: dir}}
	inner, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: []byte("embedded payload")}})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := storage.EncodeRecordBatch(0, []storage.Record{{Key: []byte("actual record"), Value: inner}})
	if err != nil {
		t.Fatal(err)
	}
	position := bytes.Index(outer, inner)
	if position < 61 {
		t.Fatalf("embedded batch position=%d, want a position inside a record value", position)
	}
	if _, err := p.AppendRecords(outer, dir); err != nil {
		t.Fatal(err)
	}
	var index [8]byte
	// The embedded batch has matching base offset, length, magic and CRC. A
	// plausible header still does not make it an actual log batch boundary.
	binary.BigEndian.PutUint32(index[4:], uint32(position))
	path := filepath.Join(dir, "00000000000000000000.index")
	if err := os.WriteFile(path, index[:], 0600); err != nil {
		t.Fatal(err)
	}
	result, err := p.FetchRecords(FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Records, outer) {
		t.Fatalf("fetch returned %d bytes, want the actual %d-byte outer batch", len(result.Records), len(outer))
	}
}

type cancelIndexWalkContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelIndexWalkContext) Err() error {
	c.checks++
	if c.checks == 3 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestIndexHintBoundaryWalkCancellationPreservesCache(t *testing.T) {
	dir := t.TempDir()
	segment := logSegment{baseOffset: 0, name: segmentNameForOffset(0)}
	var log []byte
	var position int
	for offset := int64(0); offset < 4; offset++ {
		batch, err := storage.EncodeRecordBatch(offset, []storage.Record{{Value: []byte("record")}})
		if err != nil {
			t.Fatal(err)
		}
		if offset == 3 {
			position = len(log)
		}
		log = append(log, batch...)
	}
	if err := os.WriteFile(filepath.Join(dir, segment.name), log, 0600); err != nil {
		t.Fatal(err)
	}
	var index [8]byte
	binary.BigEndian.PutUint32(index[:4], 3)
	binary.BigEndian.PutUint32(index[4:], uint32(position))
	path := offsetIndexPath(dir, segment)
	if err := os.WriteFile(path, index[:], 0600); err != nil {
		t.Fatal(err)
	}
	baseContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelIndexWalkContext{Context: baseContext, cancel: cancel}
	var checkpoint indexCheckpoint
	if _, _, err := safeIndexPosition(ctx, dir, segment, 3, &checkpoint); !errors.Is(err, context.Canceled) {
		t.Fatalf("boundary walk error=%v, want cancellation during verification", err)
	}
	if checkpoint != (indexCheckpoint{}) {
		t.Fatalf("canceled verification advanced its checkpoint: %+v", checkpoint)
	}
	stored, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(stored, index[:]) {
		t.Fatalf("cancellation discarded or changed a valid index: %v", err)
	}
}

func TestIndexHintCheckpointSupportsBackwardFetch(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, BrokerLogDirs: map[int32]string{1: dir}}
	var expected [][]byte
	for offset, value := range []string{"first", "second", "third"} {
		batch := testRecordBatch(t, value)
		if _, err := p.AppendRecords(batch, dir); err != nil {
			t.Fatal(err)
		}
		stored := bytes.Clone(batch)
		binary.BigEndian.PutUint64(stored[:8], uint64(offset))
		expected = append(expected, stored)
	}
	fetch := func(offset int64) []byte {
		t.Helper()
		result, err := p.FetchRecords(FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, Offset: offset, MaxBytes: 1 << 20, MaxBatches: 1})
		if err != nil {
			t.Fatal(err)
		}
		return result.Records
	}
	if !bytes.Equal(fetch(2), expected[2]) || p.indexCheckpoint.offset != 2 {
		t.Fatal("later fetch did not establish a proven forward checkpoint")
	}
	if !bytes.Equal(fetch(0), expected[0]) {
		t.Fatal("backward fetch skipped its requested records")
	}
	var index [8]byte
	// A forged earlier offset points at a genuine, later batch boundary. The
	// checkpoint from offset 2 must not authorize this inconsistent mapping.
	binary.BigEndian.PutUint32(index[4:], uint32(len(expected[0])))
	if err := os.WriteFile(offsetIndexPath(dir, logSegment{name: segmentNameForOffset(0)}), index[:], 0600); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fetch(0), expected[0]) {
		t.Fatal("a corrupt backward hint skipped its requested records")
	}
}

func TestIndexHintCheckpointRejectsReplacementFile(t *testing.T) {
	dir := t.TempDir()
	segment := logSegment{baseOffset: 0, name: segmentNameForOffset(0)}
	p := &Partition{LeaderID: 1, ISRNodes: []int32{1}, BrokerLogDirs: map[int32]string{1: dir}}
	first := testRecordBatch(t, "x")
	for _, batch := range [][]byte{first, testRecordBatch(t, "y")} {
		if _, err := p.AppendRecords(batch, dir); err != nil {
			t.Fatal(err)
		}
	}
	options := FetchOptions{BrokerID: 1, ReplicaID: -1, LeaderEpoch: -1, Offset: 1, MaxBytes: 1 << 20, MaxBatches: 1}
	if _, err := p.FetchRecords(options); err != nil {
		t.Fatal(err)
	}
	checkpoint := p.indexCheckpoint
	if checkpoint.position != int64(len(first)) || checkpoint.offset != 1 || checkpoint.file == nil {
		t.Fatalf("first fetch did not prove the second batch boundary: %+v", checkpoint)
	}
	inner, err := storage.EncodeRecordBatch(1, []storage.Record{{Value: []byte("embedded batch")}})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: inner}, {Value: []byte("tail")}})
	if err != nil {
		t.Fatal(err)
	}
	position := bytes.Index(outer, inner)
	if position != len(first) || int64(len(outer)) < checkpoint.file.Size() {
		t.Fatalf("fixture must reuse the old checkpoint byte without shrinking: embedded=%d checkpoint=%d newSize=%d oldSize=%d", position, len(first), len(outer), checkpoint.file.Size())
	}
	path := filepath.Join(dir, segment.name)
	replacement := filepath.Join(dir, "replacement.tmp")
	if err := os.WriteFile(replacement, outer, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(checkpoint.file, info) {
		t.Fatal("fixture did not replace the underlying file incarnation")
	}
	var index [8]byte
	binary.BigEndian.PutUint32(index[:4], 1)
	binary.BigEndian.PutUint32(index[4:], uint32(position))
	if err := os.WriteFile(offsetIndexPath(dir, segment), index[:], 0600); err != nil {
		t.Fatal(err)
	}
	result, err := p.FetchRecords(options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Records, outer) {
		t.Fatalf("stale checkpoint returned %d payload bytes instead of the actual %d-byte replacement batch", len(result.Records), len(outer))
	}
}
