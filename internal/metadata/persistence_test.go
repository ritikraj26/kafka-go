package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/storage"
)

func persistentManager(t *testing.T, dir string, ids ...int32) *Manager {
	t.Helper()
	m := NewManager()
	if err := m.SetLogDir(dir); err != nil {
		t.Fatal(err)
	}
	if len(ids) > 0 {
		m.Brokers = broker.NewRegistry(ids[0])
		for _, id := range ids {
			m.Brokers.Register(&broker.Broker{ID: id})
		}
	}
	return m
}

func createPersistentTopic(t *testing.T, m *Manager, name string, partitions int) *Topic {
	t.Helper()
	topic, err := m.CreateTopic(name, partitions)
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

func readLogFile(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, segmentNameForOffset(0)))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPersistentTopicRecovery(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir, 3, 1, 2)
	m.ReplicationFactor = 2
	topic := createPersistentTopic(t, m, "orders", 2)
	empty := createPersistentTopic(t, m, "empty", 1)
	p := &topic.Partitions[0]
	if !slices.Equal(p.ReplicaNodes, []int32{1, 2}) || !slices.Equal(topic.Partitions[1].ReplicaNodes, []int32{2, 3}) {
		t.Fatal("replica placement did not honor sorted broker IDs and RF=2")
	}
	if _, err := p.AppendRecords(testRecordBatch(t, "a", "b", "c"), p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	data := readLogFile(t, p.LogDirForBroker(1))
	if err := p.AppendReplicaRecords(data, p.LogDirForBroker(2), 0); err != nil {
		t.Fatal(err)
	}
	p.UpdateReplicaState(2, 3, time.Now().UnixMilli())
	if err := m.ElectLeader("orders", 0); err != nil {
		t.Fatal(err)
	}
	if p.LeaderID != 2 || p.LeaderEpoch != 1 {
		t.Fatal("leader election did not update state")
	}

	restarted := persistentManager(t, dir, 2, 3, 1)
	restarted.ReplicationFactor = 1 // Only new topics should use the changed default.
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	loaded := restarted.GetTopicByID(topic.ID)
	if loaded == nil || loaded.Name != "orders" || len(loaded.Partitions) != 2 || restarted.GetTopic("empty").ID != empty.ID {
		t.Fatal("topic identity or empty topic was lost")
	}
	rp := &loaded.Partitions[0]
	if rp.LeaderID != 2 || rp.LeaderEpoch != 1 || rp.NextOffset != 3 || rp.GetHighWatermark() != 3 ||
		rp.GetReplicaLEO(1) != 3 || rp.GetReplicaLEO(2) != 3 || !slices.Equal(rp.ReplicaNodes, []int32{1, 2}) {
		t.Fatalf("recovered partition state is wrong: leader=%d epoch=%d offset=%d HW=%d", rp.LeaderID, rp.LeaderEpoch, rp.NextOffset, rp.HighWatermark)
	}
	if !bytes.Equal(readLogFile(t, rp.LogDirForBroker(2)), data) {
		t.Fatal("recovered leader has different records")
	}
	if offset, err := rp.AppendRecords(testRecordBatch(t, "d"), rp.LogDirForBroker(2)); err != nil || offset != 3 {
		t.Fatalf("append after restart: offset=%d err=%v", offset, err)
	}
	again := persistentManager(t, dir, 1, 2, 3)
	if err := again.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	p2 := &again.GetTopic("orders").Partitions[0]
	if p2.GetLogEndOffset() != 4 || p2.GetHighWatermark() != 3 || p2.GetReplicaLEO(1) != 3 {
		t.Fatal("lagging follower incorrectly recovered as caught up")
	}
}

func TestPersistentTopicSurvivesProcessRestart(t *testing.T) {
	if dir := os.Getenv("KAFKA_GO_TEST_PERSISTENCE_DIR"); dir != "" {
		m := persistentManager(t, dir)
		if os.Getenv("KAFKA_GO_TEST_PERSISTENCE_PHASE") == "write" {
			topic := createPersistentTopic(t, m, "events", 1)
			p := &topic.Partitions[0]
			if _, err := p.AppendRecords(testRecordBatch(t, "one", "two", "three"), p.LogDirForBroker(1)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "expected-id"), []byte(topic.ID.String()), 0600); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := m.LoadTopicsFromDisk(dir); err != nil {
				t.Fatal(err)
			}
			id, err := os.ReadFile(filepath.Join(dir, "expected-id"))
			if err != nil {
				t.Fatal(err)
			}
			topic := m.GetTopic("events")
			if topic == nil || topic.ID.String() != string(id) || topic.Partitions[0].GetLogEndOffset() != 3 {
				t.Fatal("new process did not recover topic identity and offset")
			}
			data := readLogFile(t, topic.Partitions[0].LogDirForBroker(1))
			batches, err := storage.ParseRecordBatches(data)
			if err != nil || len(batches) != 1 || len(batches[0].Values) != 3 ||
				string(batches[0].Values[2]) != "three" || topic.Partitions[0].GetHighWatermark() != 3 {
				t.Fatalf("new process could not read persisted records: %v", err)
			}
		}
		return
	}
	dir := t.TempDir()
	for _, phase := range []string{"write", "read"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPersistentTopicSurvivesProcessRestart$")
		cmd.Env = append(os.Environ(), "KAFKA_GO_TEST_PERSISTENCE_DIR="+dir, "KAFKA_GO_TEST_PERSISTENCE_PHASE="+phase)
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("%s process: %v\n%s", phase, err, output)
		}
	}
}

func TestLegacyImportPreservesIdentityAndPaths(t *testing.T) {
	for _, layout := range []string{"events-0", "broker-2/events-0"} {
		t.Run(layout, func(t *testing.T) {
			dir := t.TempDir()
			partDir := filepath.Join(dir, layout)
			if err := os.MkdirAll(partDir, 0755); err != nil {
				t.Fatal(err)
			}
			batch := testRecordBatch(t, "legacy")
			binary.BigEndian.PutUint32(batch[12:16], 4)
			if err := os.WriteFile(filepath.Join(partDir, segmentNameForOffset(0)), batch, 0600); err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			value := append([]byte{1, 2, 0, 7}, []byte("events")...)
			value = append(value, id[:]...)
			value = append(value, 0) // empty metadata-record tag buffer
			clusterBatch, err := storage.EncodeRecordBatch(0, []storage.Record{{Value: value}})
			if err != nil {
				t.Fatal(err)
			}
			clusterDir := filepath.Join(dir, "__cluster_metadata-0")
			if err := os.MkdirAll(clusterDir, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(clusterDir, segmentNameForOffset(0)), clusterBatch, 0600); err != nil {
				t.Fatal(err)
			}
			m := persistentManager(t, dir, 1, 2, 3)
			m.ReplicationFactor = 3
			if err := m.LoadTopicsFromDisk(dir); err != nil {
				t.Fatal(err)
			}
			topic := m.GetTopic("events")
			p := &topic.Partitions[0]
			if topic.ID != id || len(p.ReplicaNodes) != 1 || p.LogDirForBroker(p.LeaderID) != partDir || p.LeaderEpoch != 4 {
				t.Fatal("legacy ownership, path, epoch or topic ID was changed")
			}
			again := persistentManager(t, dir, 1, 2, 3)
			if err := again.LoadTopicsFromDisk(dir); err != nil {
				t.Fatal(err)
			}
			if again.GetTopic("events").ID != id || !bytes.Equal(readLogFile(t, partDir), batch) {
				t.Fatal("legacy import was not stable or modified record data")
			}
		})
	}
}

func TestLegacyRecoveryRejectsAmbiguousOwnership(t *testing.T) {
	dir := t.TempDir()
	for _, path := range []string{"events-0", "broker-2/events-0"} {
		partDir := filepath.Join(dir, path)
		if err := os.MkdirAll(partDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(partDir, segmentNameForOffset(0)), testRecordBatch(t, "value"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := NewManager().LoadTopicsFromDisk(dir); err == nil || !strings.Contains(err.Error(), "ambiguous legacy") {
		t.Fatalf("expected ambiguity failure, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, metadataFileName)); !os.IsNotExist(err) {
		t.Fatal("failed import published metadata")
	}
}

func TestRecoveryRejectsMissingOrDivergentReplicas(t *testing.T) {
	for _, scenario := range []string{"directory", "segment", "ahead", "divergent", "orphan", "unknown broker"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			m := persistentManager(t, dir, 1, 2)
			topic := createPersistentTopic(t, m, "events", 1)
			p := &topic.Partitions[0]
			if _, err := p.AppendRecords(testRecordBatch(t, "leader"), p.LogDirForBroker(1)); err != nil {
				t.Fatal(err)
			}
			followerPath := filepath.Join(p.LogDirForBroker(2), segmentNameForOffset(0))
			switch scenario {
			case "directory":
				if err := os.Rename(p.LogDirForBroker(2), filepath.Join(dir, "saved-replica")); err != nil {
					t.Fatal(err)
				}
			case "segment":
				if err := os.Remove(followerPath); err != nil {
					t.Fatal(err)
				}
			case "ahead", "divergent":
				values := []string{"different"}
				if scenario == "ahead" {
					values = append(values, "extra")
				}
				if err := os.WriteFile(followerPath, testRecordBatch(t, values...), 0600); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				if err := os.MkdirAll(filepath.Join(dir, "broker-1", "unknown-0"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(filepath.Join(dir, metadataFileName))
			if err != nil {
				t.Fatal(err)
			}
			restarted := persistentManager(t, dir, 1, 2)
			if scenario == "unknown broker" {
				restarted = persistentManager(t, dir, 1)
			}
			if err := restarted.LoadTopicsFromDisk(dir); err == nil {
				t.Fatal("invalid recovery state was accepted")
			}
			if len(restarted.ListTopics()) != 0 {
				t.Fatal("failed recovery published partial metadata")
			}
			after, err := os.ReadFile(filepath.Join(dir, metadataFileName))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed recovery modified metadata: %v", err)
			}
		})
	}
}

func TestMetadataValidation(t *testing.T) {
	tests := map[string]func(*diskMetadata){
		"version":          func(d *diskMetadata) { d.Version++ },
		"missing topics":   func(d *diskMetadata) { d.Topics = nil },
		"nil UUID":         func(d *diskMetadata) { d.Topics[0].ID = uuid.Nil },
		"duplicate name":   func(d *diskMetadata) { d.Topics = append(d.Topics, d.Topics[0]) },
		"unsafe name":      func(d *diskMetadata) { d.Topics[0].Name = "../escape" },
		"partition gap":    func(d *diskMetadata) { d.Topics[0].Partitions[0].Index = 3 },
		"invalid leader":   func(d *diskMetadata) { d.Topics[0].Partitions[0].LeaderID = 9 },
		"negative epoch":   func(d *diskMetadata) { d.Topics[0].Partitions[0].LeaderEpoch = -1 },
		"duplicate broker": func(d *diskMetadata) { d.Topics[0].Partitions[0].Replicas = []int32{1, 1} },
		"unsafe path":      func(d *diskMetadata) { d.Topics[0].Partitions[0].LogDirs[1] = "../events-0" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m := persistentManager(t, dir)
			createPersistentTopic(t, m, "events", 1)
			path := filepath.Join(dir, metadataFileName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var disk diskMetadata
			if err := json.Unmarshal(data, &disk); err != nil {
				t.Fatal(err)
			}
			mutate(&disk)
			data, err = json.Marshal(disk)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := NewManager().LoadTopicsFromDisk(dir); err == nil {
				t.Fatal("invalid metadata was accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, after) {
				t.Fatalf("invalid metadata was rewritten: %v", err)
			}
		})
	}
	for _, data := range []string{`{"version":1`, `{"version":1,"topics":[],"unknown":true}`, `{"version":1,"topics":[]} {}`} {
		t.Run(data, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, metadataFileName), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := NewManager().LoadTopicsFromDisk(dir); err == nil {
				t.Fatal("malformed metadata was accepted")
			}
		})
	}
}

func TestCreationIsIdempotentAndDLQCreationIsAtomic(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir)
	original := createPersistentTopic(t, m, "events", 2)
	if again := createPersistentTopic(t, m, "events", 2); again != original {
		t.Fatal("idempotent creation replaced a live topic")
	}
	if _, err := m.CreateTopic("events", 1); err == nil {
		t.Fatal("creation silently changed partition count")
	}
	const callers = 8
	results := make(chan *Topic, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			topic, err := m.GetOrCreateDLQTopic("events")
			results <- topic
			errs <- err
		}()
	}
	wg.Wait()
	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if topic := <-results; topic != m.GetTopic("events.dlq") {
			t.Fatal("concurrent DLQ creation replaced a topic")
		}
	}
	restarted := NewManager()
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	if len(restarted.ListTopics()) != 2 || restarted.GetTopic("events.dlq").ID != m.GetTopic("events.dlq").ID {
		t.Fatal("concurrent DLQ creation did not persist one stable ID")
	}
	if err := m.SetLogDir(t.TempDir()); err == nil {
		t.Fatal("live metadata was allowed to switch storage roots")
	}
}

func TestPersistenceFailureDoesNotPublishChanges(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir, 1, 2)
	topic := createPersistentTopic(t, m, "events", 1)
	path := filepath.Join(dir, metadataFileName)
	backup := filepath.Join(dir, "metadata-backup")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := m.ElectLeader("events", 0); err == nil {
		t.Fatal("leader election succeeded despite persistence failure")
	}
	p := &topic.Partitions[0]
	if p.LeaderID != 1 || p.LeaderEpoch != 0 {
		t.Fatal("failed election was published in memory")
	}
	if _, err := p.AppendRecords(testRecordBatch(t, "blocked"), p.LogDirForBroker(1)); err == nil {
		t.Fatal("writes were not fenced after ambiguous metadata failure")
	}
	if _, err := m.CreateTopic("new", 1); err == nil || m.GetTopic("new") != nil {
		t.Fatal("manager accepted a new topic after metadata failure")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	restarted := persistentManager(t, dir, 1, 2)
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	if restarted.GetTopic("events").Partitions[0].LeaderID != 1 {
		t.Fatal("failed election replaced the last good metadata")
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".broker-metadata-*"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("failed persistence leaked temporary files: %v", err)
	}
}

func TestCreateTopicValidatesConfiguration(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../escape", "__internal", strings.Repeat("x", 250)} {
		m := persistentManager(t, t.TempDir())
		if _, err := m.CreateTopic(name, 1); err == nil {
			t.Errorf("invalid topic %q accepted", name)
		}
	}
	for _, count := range []int{0, -1, maxTopicPartitions + 1} {
		m := persistentManager(t, t.TempDir())
		if _, err := m.CreateTopic("events", count); err == nil {
			t.Errorf("invalid partition count %d accepted", count)
		}
	}
	for _, rf := range []int{-1, 3} {
		m := persistentManager(t, t.TempDir(), 1, 2)
		m.ReplicationFactor = rf
		if _, err := m.CreateTopic("events", 1); err == nil {
			t.Errorf("invalid RF %d accepted", rf)
		}
	}
}

func TestCreationFailureDoesNotPublishTopic(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir)
	createPersistentTopic(t, m, "existing", 1)
	path := filepath.Join(dir, metadataFileName)
	backup := filepath.Join(dir, "last-good-metadata")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("new", 1); err == nil {
		t.Fatal("creation succeeded without persisting its catalog entry")
	}
	if m.GetTopic("new") != nil || len(m.ListTopics()) != 1 || m.persistenceErr == nil {
		t.Fatal("failed creation published a topic or failed to fence future changes")
	}
	var disk diskMetadata
	data, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &disk); err != nil || len(disk.Topics) != 1 || disk.Topics[0].Name != "existing" {
		t.Fatalf("last good catalog changed: %v", err)
	}
}

func TestRecoveryRejectsSymlinkedStorage(t *testing.T) {
	for _, target := range []string{"catalog", "segment", "directory"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			m := persistentManager(t, dir)
			p := &createPersistentTopic(t, m, "events", 1).Partitions[0]
			path := filepath.Join(dir, metadataFileName)
			switch target {
			case "segment":
				path = filepath.Join(p.LogDirForBroker(1), segmentNameForOffset(0))
			case "directory":
				path = p.LogDirForBroker(1)
			}
			backup := filepath.Join(t.TempDir(), "saved")
			if err := os.Rename(path, backup); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(backup, path); err != nil {
				t.Fatal(err)
			}
			if err := NewManager().LoadTopicsFromDisk(dir); err == nil {
				t.Fatal("symlinked broker state was accepted")
			}
		})
	}
}

func TestRecoveryDoesNotPromoteTemporaryCatalog(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir)
	topic := createPersistentTopic(t, m, "events", 1)
	if err := os.WriteFile(filepath.Join(dir, ".broker-metadata-interrupted"), []byte(`{"version":1`), 0600); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager()
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	if restarted.GetTopic("events").ID != topic.ID {
		t.Fatal("temporary metadata replaced the committed catalog")
	}
}

func TestRecoveryAcrossDifferentSegmentBoundaries(t *testing.T) {
	dir := t.TempDir()
	m := persistentManager(t, dir, 1, 2)
	p := &createPersistentTopic(t, m, "events", 1).Partitions[0]
	oldLimit := MaxSegmentBytes
	MaxSegmentBytes = 1
	defer func() { MaxSegmentBytes = oldLimit }()
	for i := 0; i < 3; i++ {
		if _, err := p.AppendRecords(testRecordBatch(t, fmt.Sprint(i)), p.LogDirForBroker(1)); err != nil {
			t.Fatal(err)
		}
	}
	var data []byte
	segments, err := listSegments(p.LogDirForBroker(1))
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segments {
		batch, err := os.ReadFile(filepath.Join(p.LogDirForBroker(1), seg.name))
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, batch...)
	}
	MaxSegmentBytes = oldLimit
	if err := p.AppendReplicaRecords(data, p.LogDirForBroker(2), 0); err != nil {
		t.Fatal(err)
	}
	restarted := persistentManager(t, dir, 1, 2)
	if err := restarted.LoadTopicsFromDisk(dir); err != nil {
		t.Fatal(err)
	}
	if restarted.GetTopic("events").Partitions[0].GetHighWatermark() != 3 {
		t.Fatal("replicas with different segment boundaries were not matched")
	}
}
