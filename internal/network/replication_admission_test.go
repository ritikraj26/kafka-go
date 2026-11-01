package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/replication"
	"github.com/ritiraj/kafka-go/internal/schema"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReplicationProgressWithClientConnectionsAndBytesExhausted(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	public, internal := admissionListener(t), admissionListener(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(public.Addr().(*net.TCPAddr).Port), ReplicationAddress: internal.Addr().String()})
	m.Brokers.Register(&broker.Broker{ID: 2})
	m.Schemas.Register("events", &schema.SimpleSchema{Fields: map[string]schema.FieldType{"value": schema.TypeString}})
	p := &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
	body := ackProduceBody(-1, 3000, "events", ackBatch(t))
	clientBudget := testAdmission(t, 1, int64(8+len(body)), time.Second)
	replicaBudget, err := NewReplicationAdmission(2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := coordinator.NewCoordinator()
	publicDone, replicaDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(publicDone)
		if err := ServeWithAdmission(ctx, public, m, c, 1, clientBudget); err != nil {
			t.Errorf("public server: %v", err)
		}
	}()
	go func() {
		defer close(replicaDone)
		if err := ServeReplication(ctx, internal, m, c, 1, replicaBudget); err != nil {
			t.Errorf("replica server: %v", err)
		}
	}()
	fm := replication.NewFollowerManager(m, 2, 5*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		c.Close()
		fm.Wait()
		for _, done := range []<-chan struct{}{publicDone, replicaDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("protected servers did not stop")
			}
		}
		if clientBudget.Stats().Connections != 0 || clientBudget.Stats().InFlightBytes != 0 || replicaBudget.Stats().Connections != 0 || replicaBudget.Stats().InFlightBytes != 0 {
			t.Error("shutdown leaked admission reservations")
		}
		for _, admission := range []*Admission{clientBudget, replicaBudget} {
			if stats := admission.Stats(); stats.Decoded.Used != 0 || stats.Fetch.Used != 0 {
				t.Errorf("shutdown leaked decoder/fetch memory: %+v", stats)
			}
		}
		receive, err := m.ReplicationResponseBudget()
		if err != nil || receive.Stats().Used != 0 {
			t.Errorf("follower receive reservation leaked: %v", err)
		}
	})
	client := admissionClient(t, public)
	writeProduceWithoutReading(t, client, body)
	waitLifecycle(t, func() bool { return p.GetLogEndOffset() == 1 })
	if stats := clientBudget.Stats(); stats.Connections != 1 || stats.InFlightBytes != int64(8+len(body)) {
		t.Fatalf("client limits were not fully occupied: %+v", stats)
	}
	rejected := admissionClient(t, public)
	assertAdmissionClosed(t, rejected)
	// Exhaust the additional public pools without consuming protected capacity.
	fetchMemory, err := clientBudget.fetch.Acquire(context.Background(), clientBudget.memoryLimits.FetchBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer fetchMemory.Release()
	remaining := clientBudget.memoryLimits.DecodedBudget - clientBudget.Stats().Decoded.Used
	decodeMemory, err := clientBudget.decoded.Acquire(context.Background(), remaining)
	if err != nil {
		t.Fatal(err)
	}
	defer decodeMemory.Release()
	if err := fm.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var size int32
	if err := binary.Read(client, binary.BigEndian, &size); err != nil {
		t.Fatal(err)
	}
	if size < 4 || size > 1024 {
		t.Fatal("invalid producer response size")
	}
	response := make([]byte, size)
	if _, err := io.ReadFull(client, response); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(response[:4]) != 101 || ackResponseCode(t, response[4:]) != 0 {
		t.Fatal("producer did not complete despite reserved replication capacity")
	}
	if p.GetHighWatermark() != 1 || p.GetReplicaLEO(2) != 1 || replicaBudget.Stats().PeakInFlightBytes == 0 {
		t.Fatal("follower did not make progress through the protected listener")
	}
	waitLifecycle(t, func() bool { return clientBudget.Stats().InFlightBytes == 0 })
}

func TestListenerRoleCannotBeSelectedByClientReplicaID(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	m.Brokers = broker.NewRegistry(1)
	m.Brokers.Register(&broker.Broker{ID: 1})
	m.Brokers.Register(&broker.Broker{ID: 2})
	p := &testutil.CreateTopic(t, m, "events", 1).Partitions[0]
	if _, err := p.AppendRecords(ackBatch(t), p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	public := admissionListener(t)
	a := testAdmission(t, 2, 1024, time.Second)
	limitedServer(t, a, m, public)
	client := admissionClient(t, public)
	request := kmsg.NewPtrFetchRequest()
	request.Version = 0
	request.ReplicaID = 2
	part := kmsg.NewFetchRequestTopicPartition()
	part.FetchOffset = 1
	part.PartitionMaxBytes = 1024
	request.Topics = []kmsg.FetchRequestTopic{{Topic: "events", Partitions: []kmsg.FetchRequestTopicPartition{part}}}
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 1)); err != nil {
		t.Fatal(err)
	}
	assertAdmissionClosed(t, client)
	if p.GetHighWatermark() != 0 || p.GetReplicaLEO(2) != 0 {
		t.Fatal("public client spoofed replication progress")
	}

	internal := admissionListener(t)
	replicas, err := NewReplicationAdmission(2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := coordinator.NewCoordinator()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := ServeReplication(ctx, internal, m, c, 1, replicas); err != nil {
			t.Errorf("internal: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		c.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("internal server did not stop")
		}
	})
	for _, consumerRequest := range []kmsg.Request{kmsg.NewPtrApiVersionsRequest(), request} {
		request.ReplicaID = -1
		conn := admissionClient(t, internal)
		if _, err := conn.Write(kmsg.NewRequestFormatter().AppendRequest(nil, consumerRequest, 2)); err != nil {
			t.Fatal(err)
		}
		assertAdmissionClosed(t, conn)
	}
}

func TestPublicAndReplicationAdmissionCannotBeInterchanged(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	defer c.Close()
	private, err := NewReplicationAdmission(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := ServeWithAdmission(context.Background(), admissionListener(t), m, c, 1, private); err == nil {
		t.Fatal("public traffic was allowed to consume protected capacity")
	}
	public := testAdmission(t, 1, 1024, time.Second)
	if err := ServeReplication(context.Background(), admissionListener(t), m, c, 1, public); err == nil {
		t.Fatal("replication was allowed to share the client admission budget")
	}
	wildcard, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := ServeReplication(context.Background(), wildcard, m, c, 1, private); err == nil {
		t.Fatal("replication listener was exposed beyond loopback")
	}
}
