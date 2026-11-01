package network

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/memory"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMemoryProfileDefaultsAndValidation(t *testing.T) {
	m := DefaultMemoryLimits()
	if m.MaxDecodedBytes != 16<<20 || m.DecodedBudget != 128<<20 || m.FetchMaxBytes != 16<<20 || m.FetchBudget != 512<<20 || m.MaxEntries != 10000 {
		t.Fatalf("unexpected memory profile: %+v", m)
	}
	for _, edit := range []func(*MemoryLimits){
		func(m *MemoryLimits) { m.DecodedBudget = m.MaxDecodedBytes - 1 },
		func(m *MemoryLimits) { m.MaxDecodedBytes = 1023 },
		func(m *MemoryLimits) { m.MaxEntries = 0 },
		func(m *MemoryLimits) { m.FetchMaxBytes = 0 },
		func(m *MemoryLimits) { m.FetchBudget = 100 << 20 },
	} {
		invalid := m
		edit(&invalid)
		if _, err := NewAdmissionWithMemory(DefaultLimits(), invalid); err == nil {
			t.Fatalf("invalid profile accepted: %+v", invalid)
		}
	}
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	first, _ := a.decoded.Acquire(context.Background(), 64<<20)
	second, _ := a.decoded.Acquire(context.Background(), 64<<20)
	if a.Stats().Decoded.Used != 128<<20 {
		t.Fatal("decoder pool did not enforce the selected exact budget")
	}
	if _, err := a.decoded.Acquire(context.Background(), (128<<20)+1); !errors.Is(err, memory.ErrTooLarge) {
		t.Fatal("decoder pool accepted more than 128 MiB")
	}
	first.Release()
	second.Release()
	fetch, _ := a.fetch.Acquire(context.Background(), 512<<20)
	if a.Stats().Fetch.Used != 512<<20 {
		t.Fatal("fetch pool budget mismatch")
	}
	if _, err := a.fetch.Acquire(context.Background(), (512<<20)+1); !errors.Is(err, memory.ErrTooLarge) {
		t.Fatal("fetch pool accepted more than 512 MiB")
	}
	fetch.Release()
}

func TestFetchAndDecoderReservationsSurviveBlockedWrite(t *testing.T) {
	m, p := ackManager(t)
	if _, err := p.AppendRecords(ackBatch(t), p.LogDirForBroker(1)); err != nil {
		t.Fatal(err)
	}
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	conn := &observedWriteConn{Conn: server, writing: make(chan struct{})}
	listener := &pipeListener{conn: conn, closed: make(chan struct{})}
	cancel, done := limitedServer(t, a, m, listener)
	request := kmsg.NewPtrFetchRequest()
	request.Version = 16
	request.MaxWaitMillis = 0
	request.MinBytes = 0
	request.MaxBytes = 1024
	part := kmsg.NewFetchRequestTopicPartition()
	part.PartitionMaxBytes = 1024
	request.Topics = []kmsg.FetchRequestTopic{{TopicID: m.GetTopic("events").ID, Partitions: []kmsg.FetchRequestTopicPartition{part}}}
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 42)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.writing:
	case <-time.After(time.Second):
		t.Fatal("response did not reach blocked write")
	}
	stats := a.Stats()
	if stats.InFlightBytes == 0 || stats.Decoded.Used == 0 || stats.Fetch.Used == 0 {
		t.Fatalf("reservations released before delivery: %+v", stats)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled response did not stop")
	}
	stats = a.Stats()
	if stats.Connections != 0 || stats.InFlightBytes != 0 || stats.Decoded.Used != 0 || stats.Fetch.Used != 0 {
		t.Fatalf("memory leak on cancelled write: %+v", stats)
	}
}

func TestDecodedLimitRejectionReleasesAllReservations(t *testing.T) {
	m, _ := ackManager(t)
	limits := DefaultMemoryLimits()
	limits.MaxEntries = 1
	a, err := NewAdmissionWithMemory(DefaultLimits(), limits)
	if err != nil {
		t.Fatal(err)
	}
	listener := admissionListener(t)
	limitedServer(t, a, m, listener)
	client := admissionClient(t, listener)
	writeProduceWithoutReading(t, client, ackProduceBody(1, 1000, "events", ackBatch(t)))
	assertAdmissionClosed(t, client)
	waitLifecycle(t, func() bool { return a.Stats().Connections == 0 })
	stats := a.Stats()
	if stats.DecodeLimitRejections != 1 || stats.InFlightBytes != 0 || stats.Decoded.Used != 0 {
		t.Fatalf("decode limit accounting: %+v", stats)
	}
}

func TestDecoderAdmissionCancellationBeforeParsing(t *testing.T) {
	m, _ := ackManager(t)
	a, err := NewAdmission(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	held, _ := a.decoded.Acquire(context.Background(), 128<<20)
	defer held.Release()
	listener := admissionListener(t)
	cancel, done := limitedServer(t, a, m, listener)
	client := admissionClient(t, listener)
	request := kmsg.NewPtrApiVersionsRequest()
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 1)); err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, func() bool { return a.Stats().Decoded.Waiting == 1 })
	cancel()
	<-done
	assertAdmissionClosed(t, client)
	if stats := a.Stats(); stats.InFlightBytes != 0 || stats.Decoded.Used != 128<<20 || stats.Decoded.Waiting != 0 {
		t.Fatalf("cancelled decoding leaked: %+v", stats)
	}
}
