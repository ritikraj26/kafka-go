package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func interopBroker(t *testing.T) (string, *metadata.Manager, *coordinator.Coordinator) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := testutil.NewMetadataManager(t)
	m.MinInSyncReplicas = 1
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(listener.Addr().(*net.TCPAddr).Port)})
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	testutil.CreateTopic(t, m, "events", 1)
	coord := coordinator.NewCoordinator()
	coord.LoadOffsets(m.GetLogDir())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); Serve(ctx, listener, m, coord, 1) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("interoperability server did not stop")
		}
	})
	return listener.Addr().String(), m, coord
}

func interopClient(t *testing.T, address string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(address),
		kgo.ClientID(strings.Repeat("x", 256)),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(50 * time.Millisecond),
		kgo.RequestRetries(2),
		kgo.RetryTimeout(5 * time.Second),
		kgo.RequestTimeoutOverhead(2 * time.Second),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestFranzGoProduceConsumeAndCommit(t *testing.T) {
	address, _, coord := interopBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	producer := interopClient(t, address)
	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("discovery: %v", err)
	}
	records := make([]*kgo.Record, 3)
	for i := range records {
		records[i] = &kgo.Record{Topic: "events", Key: []byte(fmt.Sprintf("key-%d", i)), Value: []byte(fmt.Sprintf(`{"value":"record-%d"}`, i))}
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	for i, record := range records {
		if record.Offset != int64(i) {
			t.Fatalf("producer offset=%d, want %d", record.Offset, i)
		}
	}
	consumer := interopClient(t, address,
		kgo.ConsumerGroup("independent-group"),
		kgo.ConsumeTopics("events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(100*time.Millisecond),
	)
	var consumed []*kgo.Record
	for len(consumed) < len(records) {
		fetches := consumer.PollRecords(ctx, len(records)-len(consumed))
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consume errors: %v", errs)
		}
		consumed = append(consumed, fetches.Records()...)
		if ctx.Err() != nil {
			t.Fatalf("consume: %v", ctx.Err())
		}
	}
	for i, record := range consumed {
		if record.Offset != int64(i) || !bytes.Equal(record.Key, records[i].Key) || !bytes.Equal(record.Value, records[i].Value) ||
			record.Timestamp.UnixMilli() != records[i].Timestamp.UnixMilli() {
			t.Fatalf("record %d did not survive independent encode/decode: %+v", i, record)
		}
	}
	if err := consumer.CommitRecords(ctx, consumed...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if offset, code := coord.FetchOffset("independent-group", "events", 0); code != 0 || offset != 3 {
		t.Fatalf("committed offset=%d error=%d", offset, code)
	}
	if err := consumer.LeaveGroupContext(ctx); err != nil {
		t.Fatalf("leave group: %v", err)
	}
	// A new client in the same group must use OffsetFetch to resume at 3.
	resumed := interopClient(t, address, kgo.ConsumerGroup("independent-group"), kgo.ConsumeTopics("events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.Balancers(kgo.RangeBalancer()), kgo.DisableAutoCommit())
	next := &kgo.Record{Topic: "events", Key: []byte("last"), Value: []byte(`{"value":"after-commit"}`)}
	if err := producer.ProduceSync(ctx, next).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fetches := resumed.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs)
	}
	got := fetches.Records()
	if len(got) != 1 || got[0].Offset != 3 || !bytes.Equal(got[0].Value, next.Value) {
		t.Fatalf("group did not resume from committed offset: %+v", got)
	}
}

// wireInterop uses only the independent codec for header/body formatting and
// response decoding, then round-trips the response to detect misplaced fields.
func wireInterop(t *testing.T, conn net.Conn, request kmsg.Request, response kmsg.Response, version int16) {
	t.Helper()
	request.SetVersion(version)
	response.SetVersion(version)
	if request.Key() == 18 && version > 4 {
		response.SetVersion(0)
	}
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(strings.Repeat("long-client-", 30)))
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(formatter.AppendRequest(nil, request, 123)); err != nil {
		t.Fatal(err)
	}
	var size int32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		t.Fatal(err)
	}
	if size < 4 || size > 1<<20 {
		t.Fatalf("invalid response size %d", size)
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(frame[:4]) != 123 {
		t.Fatal("wrong response correlation")
	}
	body := frame[4:]
	if request.IsFlexible() && request.Key() != 18 {
		if len(body) == 0 || body[0] != 0 {
			t.Fatal("invalid flexible response header")
		}
		body = body[1:]
	}
	if err := response.ReadFrom(body); err != nil {
		t.Fatalf("API %d v%d decode: %v (%x)", request.Key(), version, err, body)
	}
	if encoded := response.AppendTo(nil); !bytes.Equal(encoded, body) {
		t.Fatalf("API %d v%d was not a canonical independent-codec response\nreceived %x\nencoded  %x", request.Key(), version, body, encoded)
	}
}

func TestIndependentCodecDiscoveryMetadataAndPagination(t *testing.T) {
	address, m, _ := interopBroker(t)
	testutil.CreateTopic(t, m, "paged", 3)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	expected := map[int16][2]int16{
		0: {11, 11}, 1: {16, 16}, 2: {1, 1}, 3: {12, 12}, 8: {2, 2}, 9: {1, 1}, 10: {0, 0},
		11: {0, 0}, 12: {0, 0}, 13: {0, 0}, 14: {0, 0}, 18: {0, 4}, 75: {0, 0},
	}
	for version := int16(0); version <= 5; version++ {
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName, req.ClientSoftwareVersion = "independent", "1.0"
		resp := kmsg.NewPtrApiVersionsResponse()
		wireInterop(t, conn, req, resp, version)
		code := int16(0)
		if version == 5 {
			code = 35
		}
		if resp.ErrorCode != code || len(resp.ApiKeys) != len(expected) {
			t.Fatalf("discovery v%d: %+v", version, resp)
		}
		for _, api := range resp.ApiKeys {
			want, ok := expected[api.ApiKey]
			if !ok || want != [2]int16{api.MinVersion, api.MaxVersion} {
				t.Fatalf("unsupported API advertised: %+v", api)
			}
		}
	}
	for _, topics := range [][]kmsg.MetadataRequestTopic{
		nil, {},
		{{Topic: kmsg.StringPtr("events")}},
		{{TopicID: m.GetTopic("events").ID}},
		{{Topic: kmsg.StringPtr("missing")}},
		{{TopicID: [16]byte{99}}},
	} {
		req := kmsg.NewPtrMetadataRequest()
		req.Topics = topics
		req.UnknownTags.Set(99, []byte{1, 2, 3})
		resp := kmsg.NewPtrMetadataResponse()
		wireInterop(t, conn, req, resp, 12)
		if len(topics) == 0 {
			want := 0
			if topics == nil {
				want = 2
			}
			if len(resp.Topics) != want {
				t.Fatalf("null/empty topic selection: got %d want %d", len(resp.Topics), want)
			}
		} else {
			if len(resp.Topics) != 1 {
				t.Fatalf("specific topic omitted: %+v", resp)
			}
			q, got := topics[0], resp.Topics[0]
			if q.Topic != nil && *q.Topic == "missing" {
				if got.ErrorCode != 3 {
					t.Fatalf("missing name error=%d", got.ErrorCode)
				}
			} else if q.TopicID == [16]byte{99} {
				if got.ErrorCode != 100 || got.Topic != nil {
					t.Fatalf("missing ID response=%+v", got)
				}
			} else if got.ErrorCode != 0 || got.TopicID != m.GetTopic("events").ID || len(got.Partitions) != 1 {
				t.Fatalf("invalid topic metadata %+v", got)
			}
		}
	}
	req := kmsg.NewPtrDescribeTopicPartitionsRequest()
	req.Topics = []kmsg.DescribeTopicPartitionsRequestTopic{{Topic: "paged"}}
	req.ResponsePartitionLimit = 1
	for index := int32(0); index < 3; index++ {
		resp := kmsg.NewPtrDescribeTopicPartitionsResponse()
		wireInterop(t, conn, req, resp, 0)
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].Partition != index {
			t.Fatalf("page %d: %+v", index, resp)
		}
		if index < 2 {
			if resp.NextCursor == nil || resp.NextCursor.Partition != index+1 {
				t.Fatal("incorrect next cursor")
			}
			req.Cursor = &kmsg.DescribeTopicPartitionsRequestCursor{Topic: resp.NextCursor.Topic, Partition: resp.NextCursor.Partition}
		} else if resp.NextCursor != nil {
			t.Fatal("final page has a cursor")
		}
	}
}
