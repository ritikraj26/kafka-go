package network

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIndependentOffsetMetadataAndStorageFailure(t *testing.T) {
	address, m, c := interopBroker(t)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = "durable-offsets", "consumer", 10000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("subscription")}}
	joined := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, join, joined, 0)
	if joined.ErrorCode != 0 {
		t.Fatal(joined.ErrorCode)
	}
	defer c.LeaveGroup(join.Group, joined.MemberID)

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.Generation, commit.MemberID = join.Group, joined.Generation, joined.MemberID
	note := "consumer checkpoint"
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "events", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 3, Metadata: &note}}}}
	committed := kmsg.NewPtrOffsetCommitResponse()
	wireInterop(t, conn, commit, committed, 2)
	if committed.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal("offset commit failed")
	}
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = join.Group
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "events", Partitions: []int32{0}}}
	fetched := kmsg.NewPtrOffsetFetchResponse()
	wireInterop(t, conn, fetch, fetched, 1)
	part := fetched.Topics[0].Partitions[0]
	if part.ErrorCode != 0 || part.Offset != 3 || part.Metadata == nil || *part.Metadata != note {
		t.Fatalf("offset metadata was not returned: %+v", part)
	}

	path := filepath.Join(m.GetLogDir(), "__consumer_offsets-0", "00000000000000000000.log")
	backup := filepath.Join(t.TempDir(), "last-good-offsets")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	commit.Topics[0].Partitions[0].Offset = 9
	wireInterop(t, conn, commit, committed, 2)
	if code := committed.Topics[0].Partitions[0].ErrorCode; code != 56 {
		t.Fatalf("persistence failure returned %d, want KAFKA_STORAGE_ERROR", code)
	}
	wireInterop(t, conn, fetch, fetched, 1)
	if code := fetched.Topics[0].Partitions[0].ErrorCode; code != 56 {
		t.Fatalf("fenced OffsetFetch returned %d, want KAFKA_STORAGE_ERROR", code)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	restarted := coordinator.NewCoordinator()
	if err := restarted.LoadOffsets(m.GetLogDir()); err != nil {
		t.Fatal(err)
	}
	offset, meta, code := restarted.FetchOffsetWithMetadata(join.Group, "events", 0)
	if code != 0 || offset != 3 || meta == nil || *meta != note {
		t.Fatal("failed commit changed durable offset/metadata")
	}
}

func offsetRestartBroker(t *testing.T, dir string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := metadata.NewManager()
	m.MinInSyncReplicas = 1
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(listener.Addr().(*net.TCPAddr).Port)})
	if err := m.LoadTopicsFromDisk(dir); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("events", 1); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	c := coordinator.NewCoordinator()
	if err := c.LoadOffsets(dir); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); Serve(ctx, listener, m, c, 1) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("restart-test broker did not stop")
		}
	})
	return listener.Addr().String()
}

func TestFranzGoCommittedOffsetsSurviveBrokerProcessRestart(t *testing.T) {
	if dir := os.Getenv("KAFKA_GO_TEST_OFFSET_RESTART_DIR"); dir != "" {
		address := offsetRestartBroker(t, dir)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if os.Getenv("KAFKA_GO_TEST_OFFSET_RESTART_PHASE") == "write" {
			producer := interopClient(t, address)
			var records []*kgo.Record
			for i := 0; i < 4; i++ {
				records = append(records, &kgo.Record{Topic: "events", Value: []byte(fmt.Sprintf(`{"value":"record-%d"}`, i))})
			}
			if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		consumer := interopClient(t, address, kgo.ConsumerGroup("restart-group"), kgo.ConsumeTopics("events"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.Balancers(kgo.RangeBalancer()), kgo.DisableAutoCommit())
		if os.Getenv("KAFKA_GO_TEST_OFFSET_RESTART_PHASE") == "write" {
			var consumed []*kgo.Record
			for len(consumed) < 3 {
				fetches := consumer.PollRecords(ctx, 3-len(consumed))
				if errs := fetches.Errors(); len(errs) > 0 {
					t.Fatal(errs)
				}
				consumed = append(consumed, fetches.Records()...)
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
			}
			if consumed[2].Offset != 2 {
				t.Fatal("unexpected initial consumer offsets")
			}
			if err := consumer.CommitRecords(ctx, consumed...); err != nil {
				t.Fatal(err)
			}
		} else {
			fetches := consumer.PollRecords(ctx, 1)
			if errs := fetches.Errors(); len(errs) > 0 {
				t.Fatal(errs)
			}
			records := fetches.Records()
			if len(records) != 1 || records[0].Offset != 3 || string(records[0].Value) != `{"value":"record-3"}` {
				t.Fatalf("fresh broker/client did not resume at committed offset 3: %+v", records)
			}
		}
		if err := consumer.LeaveGroupContext(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	dir := t.TempDir()
	for _, phase := range []string{"write", "read"} {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFranzGoCommittedOffsetsSurviveBrokerProcessRestart$")
		cmd.Env = append(os.Environ(), "KAFKA_GO_TEST_OFFSET_RESTART_DIR="+dir, "KAFKA_GO_TEST_OFFSET_RESTART_PHASE="+phase)
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("%s process: %v\n%s", phase, err, output)
		}
	}
}
