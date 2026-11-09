package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func offsetCoordinator(t *testing.T, dir string) (*Coordinator, *JoinGroupResult) {
	t.Helper()
	c := NewCoordinator()
	if err := c.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	member := c.JoinGroup("group", "", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	if member.ErrorCode != 0 {
		t.Fatalf("join failed: %d", member.ErrorCode)
	}
	t.Cleanup(func() { c.LeaveGroup("group", member.MemberID) })
	return c, member
}

func commitTestOffset(c *Coordinator, member *JoinGroupResult, offset int64, metadata *string) int16 {
	return c.CommitOffsetWithMetadata("group", member.GenerationID, member.MemberID, "events", 0, offset, metadata)
}

func TestOffsetCommitRequiresInitializedStorage(t *testing.T) {
	c := NewCoordinator()
	member := c.JoinGroup("group", "", 10000, "consumer", []MemberProtocol{{Name: "range"}})
	defer c.LeaveGroup("group", member.MemberID)
	if code := commitTestOffset(c, member, 1, nil); code != protocol.ErrCoordinatorNotAvailable {
		t.Fatalf("uninitialized store accepted a commit: %d", code)
	}
	if offset, code := c.FetchOffset("group", "events", 0); offset != -1 || code != protocol.ErrCoordinatorNotAvailable {
		t.Fatalf("uninitialized fetch: offset=%d code=%d", offset, code)
	}
}

func TestOffsetMetadataSurvivesRestartAndDoesNotAlias(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	value := "consumer state\nwith <JSON> and unicode \u03bb"
	want := value
	if code := commitTestOffset(c, member, 42, &value); code != 0 {
		t.Fatalf("commit: %d", code)
	}
	value = "changed by caller"
	offset, metadata, code := c.FetchOffsetWithMetadata("group", "events", 0)
	if code != 0 || offset != 42 || metadata == nil || *metadata != want {
		t.Fatal("committed metadata aliased its caller")
	}
	*metadata = "changed by reader"
	restarted := NewCoordinator()
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	offset, metadata, code = restarted.FetchOffsetWithMetadata("group", "events", 0)
	if code != 0 || offset != 42 || metadata == nil || *metadata != want {
		t.Fatal("restart lost offset metadata")
	}
	_, metadata, _ = c.FetchOffsetWithMetadata("group", "events", 0)
	if metadata == nil || *metadata != want {
		t.Fatal("fetch exposed mutable stored metadata")
	}
	if err := c.LoadOffsets(t.TempDir()); err == nil {
		t.Fatal("live coordinator changed its storage root")
	}
}

func TestNullAndEmptyOffsetMetadata(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	empty := ""
	for partition, value := range []*string{nil, &empty} {
		if code := c.CommitOffsetWithMetadata("group", member.GenerationID, member.MemberID, "events", int32(partition), 0, value); code != 0 {
			t.Fatalf("commit: %d", code)
		}
	}
	restarted := NewCoordinator()
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	for partition := int32(0); partition < 2; partition++ {
		offset, value, code := restarted.FetchOffsetWithMetadata("group", "events", partition)
		if code != 0 || offset != 0 || (partition == 0 && value != nil) || (partition == 1 && (value == nil || *value != "")) {
			t.Fatalf("null/empty metadata lost for partition %d: %v", partition, value)
		}
	}
}

type faultyOffsetFile struct {
	*os.File
	mode     string
	syncHook func() error
}

func (f *faultyOffsetFile) Write(data []byte) (int, error) {
	switch f.mode {
	case "write":
		return 0, errors.New("injected write failure")
	case "short", "partial":
		n, err := f.File.Write(data[:len(data)/2])
		if err != nil {
			return n, err
		}
		if f.mode == "partial" {
			return n, errors.New("injected partial write")
		}
		return n, nil
	default:
		return f.File.Write(data)
	}
}

func (f *faultyOffsetFile) Sync() error {
	if f.mode == "sync" {
		return errors.New("injected sync failure")
	}
	if f.syncHook != nil {
		return f.syncHook()
	}
	return f.File.Sync()
}

func (f *faultyOffsetFile) Close() error {
	err := f.File.Close()
	if f.mode == "close" {
		return errors.Join(err, errors.New("injected close failure"))
	}
	return err
}

func TestOffsetIOFailuresFenceStoreWithoutPublishing(t *testing.T) {
	for _, mode := range []string{"open", "write", "short", "partial", "sync", "close"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			c, member := offsetCoordinator(t, dir)
			if code := commitTestOffset(c, member, 10, nil); code != 0 {
				t.Fatal(code)
			}
			c.openOffsetLog = func(path string) (offsetLogFile, error) {
				if mode == "open" {
					return nil, errors.New("injected open failure")
				}
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					return nil, err
				}
				return &faultyOffsetFile{File: f, mode: mode}, nil
			}
			if code := commitTestOffset(c, member, 20, nil); code != protocol.ErrKafkaStorageError {
				t.Fatalf("I/O failure returned %d", code)
			}
			c.offsetMu.RLock()
			published := c.offsets["group"]["events"][0].Offset
			c.offsetMu.RUnlock()
			if published != 10 {
				t.Fatal("failed commit replaced last published offset")
			}
			if offset, code := c.FetchOffset("group", "events", 0); offset != -1 || code != protocol.ErrKafkaStorageError {
				t.Fatal("fenced store returned success")
			}
			path := filepath.Join(dir, "__consumer_offsets-0", offsetLogName)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			c.openOffsetLog = openOffsetLogFile
			if code := commitTestOffset(c, member, 30, nil); code != protocol.ErrKafkaStorageError {
				t.Fatal("fenced store retried appending")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatal("fenced store modified the failed log")
			}
			if err := c.LoadOffsets(dir); err == nil {
				t.Fatal("fence was cleared without a new coordinator")
			}
			restarted := NewCoordinator()
			err = restarted.LoadOffsets(dir)
			if mode == "short" || mode == "partial" {
				if err == nil {
					t.Fatal("partial tail was silently repaired")
				}
				preserved, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(preserved, before) {
					t.Fatal("failed replay modified the log")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := int64(10)
			if mode == "sync" || mode == "close" {
				want = 20
			} // Complete but unacknowledged records may survive.
			if offset, code := restarted.FetchOffset("group", "events", 0); code != 0 || offset != want {
				t.Fatalf("restart offset=%d, want %d", offset, want)
			}
		})
	}
}

func TestOffsetReplayRejectsDamageWithoutPartialPublication(t *testing.T) {
	valid, err := encodeOffsetRecord(offsetRecord{Group: "group", Topic: "events", Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Replace(bytes.Clone(valid), []byte(`"offset":1`), []byte(`"offset":9`), 1)
	for name, tail := range map[string][]byte{
		"truncated":            valid[:len(valid)-1],
		"checksum":             corrupt,
		"invalid JSON":         []byte("{bad}\n"),
		"missing field":        []byte("{\"group\":\"g\",\"topic\":\"t\",\"offset\":0}\n"),
		"negative offset":      []byte("{\"group\":\"g\",\"topic\":\"t\",\"partition\":0,\"offset\":-1}\n"),
		"unknown field":        []byte("{\"group\":\"g\",\"topic\":\"t\",\"partition\":0,\"offset\":0,\"extra\":1}\n"),
		"empty":                []byte("\n"),
		"future version":       []byte("{\"version\":2,\"record\":{},\"crc32c\":0}\n"),
		"missing checksum":     []byte("{\"version\":1,\"record\":{}}\n"),
		"multiple JSON values": []byte("{} {}\n"),
		"oversized":            append(bytes.Repeat([]byte{'x'}, maxOffsetRecordBytes), '\n'),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			logDir := filepath.Join(dir, "__consumer_offsets-0")
			if err := os.Mkdir(logDir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(logDir, offsetLogName)
			data := append(bytes.Clone(valid), tail...)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			c := NewCoordinator()
			if err := c.LoadOffsets(dir); err == nil {
				t.Fatal("damaged log was accepted")
			}
			if len(c.offsets) != 0 || c.offsetReady {
				t.Fatal("failed replay published a partial offset map")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, data) {
				t.Fatal("recovery altered damaged input")
			}
		})
	}
}

func TestLegacyOffsetsRemainUnchangedWhenNewRecordsAreAppended(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "__consumer_offsets-0")
	if err := os.Mkdir(logDir, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := []byte("{\"group\":\"group\",\"topic\":\"events\",\"partition\":0,\"offset\":5}\n")
	path := filepath.Join(logDir, offsetLogName)
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	c, member := offsetCoordinator(t, dir)
	if offset, code := c.FetchOffset("group", "events", 0); code != 0 || offset != 5 {
		t.Fatal("legacy record not recovered")
	}
	if code := commitTestOffset(c, member, 8, nil); code != 0 {
		t.Fatal(code)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, legacy) {
		t.Fatal("legacy data was rewritten")
	}
	var envelope offsetEnvelope
	if err := json.Unmarshal(data[len(legacy):], &envelope); err != nil || envelope.CRC == nil || envelope.Version == nil {
		t.Fatalf("new commit has no checksum: %v", err)
	}
	restarted := NewCoordinator()
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	if offset, code := restarted.FetchOffset("group", "events", 0); code != 0 || offset != 8 {
		t.Fatal("mixed legacy/new log did not replay in order")
	}
}

func TestOffsetValidationDoesNotFenceHealthyStore(t *testing.T) {
	c, member := offsetCoordinator(t, t.TempDir())
	tooLarge := strings.Repeat("x", maxOffsetMetadataBytes+1)
	if code := commitTestOffset(c, member, 1, &tooLarge); code != protocol.ErrOffsetMetadataTooLarge {
		t.Fatalf("oversized metadata: %d", code)
	}
	if code := commitTestOffset(c, member, -1, nil); code != protocol.ErrInvalidRequest {
		t.Fatalf("negative offset: %d", code)
	}
	if code := c.CommitOffset("group", member.GenerationID+1, member.MemberID, "events", 0, 1); code != protocol.ErrIllegalGeneration {
		t.Fatal("stale generation accepted")
	}
	if code := c.CommitOffset("group", member.GenerationID, "unknown", "events", 0, 1); code != protocol.ErrUnknownMemberID {
		t.Fatal("unknown member accepted")
	}
	exact := strings.Repeat("x", maxOffsetMetadataBytes)
	if code := commitTestOffset(c, member, 1, &exact); code != 0 {
		t.Fatalf("valid commit after validation errors: %d", code)
	}
}

func TestCommitHoldsGenerationLockUntilSync(t *testing.T) {
	c, member := offsetCoordinator(t, t.TempDir())
	g := c.FindGroup("group")
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
	result := make(chan int16, 1)
	go func() { result <- commitTestOffset(c, member, 2, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach sync")
	}
	if g.mu.TryLock() {
		g.mu.Unlock()
		t.Fatal("generation could change before persistence completed")
	}
	if c.offsetMu.TryRLock() {
		c.offsetMu.RUnlock()
		t.Fatal("fetch could observe an incomplete commit")
	}
	if !c.mu.TryRLock() {
		t.Fatal("offset sync blocked the coordinator registry")
	}
	c.mu.RUnlock()
	unblock()
	if code := <-result; code != 0 {
		t.Fatal(code)
	}
}

func TestConcurrentOffsetCommitsReplayExactly(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	var wg sync.WaitGroup
	for partition := int32(0); partition < 4; partition++ {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			for offset := int64(0); offset < 15; offset++ {
				if code := c.CommitOffset("group", member.GenerationID, member.MemberID, "events", p, offset); code != 0 {
					t.Errorf("commit error %d", code)
					return
				}
			}
		}(partition)
	}
	wg.Wait()
	restarted := NewCoordinator()
	if err := restarted.LoadOffsets(dir); err != nil {
		t.Fatal(err)
	}
	for partition := int32(0); partition < 4; partition++ {
		if offset, code := restarted.FetchOffset("group", "events", partition); code != 0 || offset != 14 {
			t.Fatalf("partition %d: offset=%d code=%d", partition, offset, code)
		}
	}
}

func TestMissingInitializedOffsetFileIsNotRecreated(t *testing.T) {
	dir := t.TempDir()
	c, member := offsetCoordinator(t, dir)
	path := filepath.Join(dir, "__consumer_offsets-0", offsetLogName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if code := commitTestOffset(c, member, 1, nil); code != protocol.ErrKafkaStorageError {
		t.Fatalf("missing file: %d", code)
	}
	if err := NewCoordinator().LoadOffsets(dir); err == nil {
		t.Fatal("startup silently recreated a missing log")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing log was recreated")
	}
}

func TestOffsetRecordBoundsIncludeJSONEscaping(t *testing.T) {
	metadata := strings.Repeat("<", maxOffsetMetadataBytes)
	record := offsetRecord{Group: strings.Repeat("<", 32767), Topic: strings.Repeat("<", 249), Offset: 0, Metadata: &metadata}
	encoded, err := encodeOffsetRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxOffsetRecordBytes {
		t.Fatal("valid maximum fields exceeded record bound")
	}
	decoded, err := decodeOffsetRecord(encoded)
	if err != nil || decoded.Group != record.Group || decoded.Topic != record.Topic || decoded.Offset != 0 ||
		decoded.Metadata == nil || *decoded.Metadata != metadata {
		t.Fatalf("maximum escaped fields did not round-trip: %v", err)
	}
}

func TestOffsetRecoveryRejectsSymlinksAndExtraSegments(t *testing.T) {
	for _, kind := range []string{"directory", "file", "extra segment"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			c, _ := offsetCoordinator(t, dir)
			path := filepath.Join(c.offsetLogDir, offsetLogName)
			if kind == "extra segment" {
				if err := os.WriteFile(filepath.Join(c.offsetLogDir, "00000000000000000001.log"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if kind == "directory" {
					path = c.offsetLogDir
				}
				backup := filepath.Join(t.TempDir(), "saved")
				if err := os.Rename(path, backup); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(backup, path); err != nil {
					t.Fatal(err)
				}
			}
			if err := NewCoordinator().LoadOffsets(dir); err == nil {
				t.Fatal("unsafe or unsupported offset layout was accepted")
			}
		})
	}
}

func FuzzOffsetLogReplay(f *testing.F) {
	valid, err := encodeOffsetRecord(offsetRecord{Group: "g", Topic: "t", Offset: 1})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte("{\"group\":\"g\",\"topic\":\"t\",\"partition\":0,\"offset\":0}\n"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxOffsetRecordBytes*2 {
			return
		}
		offsets, count, err := replayOffsets(bytes.NewReader(data))
		if err != nil && (offsets != nil || count != 0) {
			t.Fatal("partial state escaped failed recovery")
		}
	})
}
