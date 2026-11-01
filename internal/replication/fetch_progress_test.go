package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/network"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
	"github.com/ritiraj/kafka-go/internal/testutil"
)

func TestFollowerFetchPersistsBeforeAdvancingProgress(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(listener.Addr().(*net.TCPAddr).Port), ReplicationAddress: listener.Addr().String()})
	m.Brokers.Register(&broker.Broker{ID: 2})
	topic := testutil.CreateTopic(t, m, "events", 1)
	p := &topic.Partitions[0]
	// Larger than the old follower's hard-coded 1 MiB limit.
	value := bytes.Repeat([]byte("x"), 2<<20)
	batch, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: value}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.AppendRecords(batch, p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	coord := coordinator.NewCoordinator()
	defer coord.Close()
	admission, err := network.NewReplicationAdmission(2)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(done)
		if err := network.ServeReplication(ctx, listener, m, coord, 1, admission); err != nil {
			t.Errorf("leader server: %v", err)
		}
	}()
	fm := NewFollowerManager(m, 2, time.Millisecond)
	defer func() {
		fm.closeAll()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server failed to stop")
		}
	}()
	fm.replicateAll()
	if p.GetReplicaLEO(2) != 1 {
		t.Fatal("follower did not advance after successful disk write")
	}
	if p.GetHighWatermark() != 0 {
		t.Fatal("first fetch acknowledged its response before the subsequent request")
	}
	stored, err := os.ReadFile(filepath.Join(p.LogDirForBroker(2), "00000000000000000000.log"))
	if err != nil || !bytes.Equal(stored, batch) {
		t.Fatalf("follower did not persist complete data: %v", err)
	}
	fm.replicateAll()
	if p.GetHighWatermark() != 1 {
		t.Fatal("subsequent request did not acknowledge persisted progress")
	}
	after, err := os.ReadFile(filepath.Join(p.LogDirForBroker(2), "00000000000000000000.log"))
	if err != nil || !bytes.Equal(after, stored) {
		t.Fatal("caught-up follower duplicated records")
	}
}

func TestFollowerWriteFailureDoesNotAdvancePosition(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1})
	m.Brokers.Register(&broker.Broker{ID: 2})
	topic := testutil.CreateTopic(t, m, "events", 1)
	p := &topic.Partitions[0]
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	p.BrokerLogDirs[2] = blocker
	batch, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: []byte("record")}})
	if err != nil {
		t.Fatal(err)
	}
	e := protocol.NewEncoder()
	e.WriteInt32(1)
	e.WriteArrayLen(1)
	e.WriteString("events")
	e.WriteArrayLen(1)
	e.WriteInt32(0)
	e.WriteInt16(0)
	e.WriteInt64(0)
	e.WriteBytes(batch)
	fm := NewFollowerManager(m, 2, time.Millisecond)
	fm.decodeFetchV0Response(e.Bytes(), []partInfo{{topic: topic, partition: p, leo: 0, leaderID: 1, epoch: 0}})
	if p.GetReplicaLEO(2) != 0 || p.GetHighWatermark() != 0 {
		t.Fatal("failed write advanced replication progress")
	}
	p.BrokerLogDirs[2] = t.TempDir()
	if err := p.AppendFetchedRecords(2, 1, 0, 0, batch); err == nil {
		t.Fatal("ambiguous failed append was not fenced")
	}
}

func TestFollowerFetchRequestAllowsMaximumBatch(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	topic := testutil.CreateTopic(t, m, "events", 1)
	fm := NewFollowerManager(m, 2, time.Millisecond)
	data := fm.encodeFetchV0(1, []partInfo{{topic: topic, partition: &topic.Partitions[0]}})
	if got := int32(binary.BigEndian.Uint32(data[len(data)-4:])); got != storage.MaxRecordBatchBytes {
		t.Fatalf("follower byte limit=%d, want %d", got, storage.MaxRecordBatchBytes)
	}
}
