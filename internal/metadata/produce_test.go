package metadata

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func producePartition(t *testing.T, replicas int) (*Manager, *Partition) {
	t.Helper()
	ids := make([]int32, replicas)
	for i := range ids {
		ids[i] = int32(i + 1)
	}
	m := persistentManager(t, t.TempDir(), ids...)
	return m, &createPersistentTopic(t, m, "events", 1).Partitions[0]
}

func allAcks() ProduceOptions {
	return ProduceOptions{BrokerID: 1, Acks: -1, MinISR: 2, Deadline: time.Now().Add(2 * time.Second)}
}

type produceOutcome struct {
	offset int64
	err    error
}

func pendingWrite(t *testing.T, p *Partition, options ProduceOptions) <-chan produceOutcome {
	t.Helper()
	result := make(chan produceOutcome, 1)
	finished := make(chan struct{})
	batch := testRecordBatch(t, "a", "b", "c")
	go func() {
		defer close(finished)
		offset, err := p.ProduceRecords(batch, options)
		result <- produceOutcome{offset, err}
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("produce waiter did not exit")
		}
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		pending := len(p.pendingProduces) != 0
		p.mu.Unlock()
		if pending {
			return result
		}
		select {
		case outcome := <-result:
			t.Fatalf("produce finished before the test could change replica state: %+v", outcome)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("produce did not enter replication wait")
	return nil
}

func expectOutcome(t *testing.T, result <-chan produceOutcome, want error) {
	t.Helper()
	select {
	case outcome := <-result:
		if outcome.offset != 0 || !errors.Is(outcome.err, want) {
			t.Fatalf("offset=%d error=%v, want offset=0 error=%v", outcome.offset, outcome.err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("state change did not promptly wake produce")
	}
}

func TestProduceMinimumISRBeforeAppend(t *testing.T) {
	_, p := producePartition(t, 1)
	path := filepath.Join(p.LogDirForBroker(1), segmentNameForOffset(0))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := p.ProduceRecords(testRecordBatch(t, "record"), allAcks())
	if offset != -1 || !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("offset=%d err=%v", offset, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || p.GetLogEndOffset() != 0 {
		t.Fatalf("admission failure changed the log: %v", err)
	}
	if _, err := os.Stat(filepath.Join(p.LogDirForBroker(1), "00000000000000000000.index")); !os.IsNotExist(err) {
		t.Fatalf("admission failure created an index: %v", err)
	}
}

func TestProduceWeakerAcksIgnoreMinimumISR(t *testing.T) {
	for _, acks := range []int16{0, 1} {
		_, p := producePartition(t, 1)
		options := allAcks()
		options.Acks = acks
		options.Deadline = time.Now().Add(-time.Second)
		if offset, err := p.ProduceRecords(testRecordBatch(t, "record"), options); err != nil || offset != 0 {
			t.Fatalf("acks=%d: offset=%d err=%v", acks, offset, err)
		}
		if p.GetLogEndOffset() != 1 {
			t.Fatal("weaker acknowledgement did not append")
		}
	}
}

func TestProduceSingleReplicaRequiresExplicitOptIn(t *testing.T) {
	m, p := producePartition(t, 1)
	if m.MinInSyncReplicas != 2 {
		t.Fatalf("default minimum ISR=%d, want 2", m.MinInSyncReplicas)
	}
	options := allAcks()
	options.MinISR = 1
	options.Deadline = time.Now()
	if offset, err := p.ProduceRecords(testRecordBatch(t, "record"), options); err != nil || offset != 0 {
		t.Fatalf("single-replica opt-in: offset=%d err=%v", offset, err)
	}
}

func TestProduceWaitsForAllCurrentISRNotJustMinimum(t *testing.T) {
	_, p := producePartition(t, 3)
	result := pendingWrite(t, p, allAcks())
	p.RunUnderLock(func() {
		p.ReplicaLEO[2] = 3
		p.AdvanceHighWatermark()
	})
	select {
	case outcome := <-result:
		t.Fatalf("minimum was treated as a quorum instead of all ISR: %+v", outcome)
	default:
	}
	p.RunUnderLock(func() {
		p.ReplicaLEO[3] = 3
		p.AdvanceHighWatermark()
	})
	expectOutcome(t, result, nil)
}

func TestProduceISRChangeWakesWaiters(t *testing.T) {
	for _, scenario := range []string{"above minimum", "below minimum", "drop then recover"} {
		t.Run(scenario, func(t *testing.T) {
			_, p := producePartition(t, 3)
			result := pendingWrite(t, p, allAcks())
			var updateErr error
			p.RunUnderLock(func() {
				p.ReplicaLEO[2] = 3
				if scenario == "above minimum" {
					updateErr = p.ReplaceISR([]int32{1, 2})
				} else {
					updateErr = p.ReplaceISR([]int32{1})
					if scenario == "drop then recover" && updateErr == nil {
						updateErr = p.ReplaceISR([]int32{1, 2})
					}
				}
			})
			if updateErr != nil {
				t.Fatal(updateErr)
			}
			want := ErrNotEnoughReplicasAfterAppend
			if scenario == "above minimum" {
				want = nil
			}
			expectOutcome(t, result, want)
			if p.GetLogEndOffset() != 3 {
				t.Fatal("failed acknowledgement rolled back an existing append")
			}
		})
	}
}

func TestProduceLeadershipChangeAbortsWait(t *testing.T) {
	m, p := producePartition(t, 2)
	result := pendingWrite(t, p, allAcks())
	p.UpdateReplicaState(2, 3, 0)
	if err := m.ElectLeader("events", 0); err != nil {
		t.Fatal(err)
	}
	expectOutcome(t, result, ErrNotLeader)
	if _, err := p.ProduceRecords(testRecordBatch(t, "stale"), allAcks()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("old leader accepted another append: %v", err)
	}
}

func TestProduceLeaderOfflineAbortsWait(t *testing.T) {
	_, p := producePartition(t, 2)
	result := pendingWrite(t, p, allAcks())
	if err := p.SetReplicaOnline(1, false); err != nil {
		t.Fatal(err)
	}
	expectOutcome(t, result, ErrNotLeader)
}

func TestProduceTimeoutKeepsAppendedRecords(t *testing.T) {
	_, p := producePartition(t, 2)
	options := allAcks()
	options.Deadline = time.Now().Add(-time.Second)
	offset, err := p.ProduceRecords(testRecordBatch(t, "record"), options)
	if offset != 0 || !errors.Is(err, ErrProduceTimeout) || p.GetLogEndOffset() != 1 {
		t.Fatalf("timeout outcome: offset=%d err=%v LEO=%d", offset, err, p.GetLogEndOffset())
	}
}

func TestProduceChecksAdmissionAgainAfterPrecheck(t *testing.T) {
	_, p := producePartition(t, 2)
	options := allAcks()
	if err := p.CheckProduce(options); err != nil {
		t.Fatal(err)
	}
	if err := p.SetReplicaOnline(2, false); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceRecords(testRecordBatch(t, "record"), options); !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("append ignored ISR change after precheck: %v", err)
	}
	if p.GetLogEndOffset() != 0 {
		t.Fatal("failed recheck appended data")
	}
}

func TestProduceRejectsInvalidAcksAndFencedDisk(t *testing.T) {
	_, p := producePartition(t, 2)
	options := allAcks()
	options.Acks = 2
	if _, err := p.ProduceRecords(testRecordBatch(t, "record"), options); !errors.Is(err, ErrInvalidRequiredAcks) {
		t.Fatalf("invalid acks accepted: %v", err)
	}
	result := pendingWrite(t, p, allAcks())
	p.RunUnderLock(func() {
		p.appendErr = errors.New("test disk failure")
		p.signalChange()
	})
	select {
	case outcome := <-result:
		if outcome.err == nil || errors.Is(outcome.err, ErrProduceTimeout) {
			t.Fatalf("disk failure was not promptly reported: %v", outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("disk failure did not wake the waiter")
	}
}

func TestFollowerDiskFailureFailsPendingAcknowledgement(t *testing.T) {
	_, p := producePartition(t, 2)
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	p.BrokerLogDirs[2] = blocker
	result := pendingWrite(t, p, allAcks())
	data := readLogFile(t, p.LogDirForBroker(1))
	if err := p.AppendFetchedRecords(2, 1, 0, 0, data); err == nil {
		t.Fatal("follower disk error was ignored")
	}
	expectOutcome(t, result, ErrNotEnoughReplicasAfterAppend)
	if _, err := p.ProduceRecords(testRecordBatch(t, "next"), allAcks()); !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("new write counted a disk-fenced follower as in sync: %v", err)
	}
}
