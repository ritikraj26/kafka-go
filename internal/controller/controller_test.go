package controller

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/metadata"
)

func TestController_DetectsDeadBrokerAndElectsLeader(t *testing.T) {
	metaMgr := metadata.NewManager()
	reg := broker.NewRegistry(1)

	// Broker 1: listening on a real port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	port1 := listener.Addr().(*net.TCPAddr).Port

	// Broker 2: no listener (simulates dead broker)
	port2 := freePort(t)

	reg.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(port1)})
	reg.Register(&broker.Broker{ID: 2, Host: "127.0.0.1", Port: int32(port2)})
	metaMgr.Brokers = reg

	// Create a topic with leader=2 (the dead broker), replicas=[2,1], ISR=[2,1]
	topic := metaMgr.CreateTopic("test-topic", 1)
	topic.Partitions[0].LeaderID = 2
	topic.Partitions[0].ReplicaNodes = []int32{2, 1}
	topic.Partitions[0].ISRNodes = []int32{2, 1}

	ctrl := NewController(metaMgr, reg, 100*time.Millisecond)
	ctrl.failureThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl.Start(ctx)

	// Wait enough time for health checks to detect failure
	time.Sleep(500 * time.Millisecond)
	cancel()
	ctrl.Wait()

	if ctrl.IsAlive(2) {
		t.Error("broker 2 should be marked dead")
	}
	if !ctrl.IsAlive(1) {
		t.Error("broker 1 should be alive")
	}

	p := &topic.Partitions[0]
	if p.LeaderID == 2 {
		t.Errorf("leader should have changed from 2, got %d", p.LeaderID)
	}
	if p.LeaderID != 1 {
		t.Errorf("leader should be 1, got %d", p.LeaderID)
	}
	if p.LeaderEpoch < 1 {
		t.Errorf("LeaderEpoch should be >= 1, got %d", p.LeaderEpoch)
	}
	for _, id := range p.ISRNodes {
		if id == 2 {
			t.Error("dead broker 2 should have been removed from ISR")
		}
	}
}

func TestController_HealthyBrokersNoElection(t *testing.T) {
	metaMgr := metadata.NewManager()
	reg := broker.NewRegistry(1)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l2.Close()

	port1 := l1.Addr().(*net.TCPAddr).Port
	port2 := l2.Addr().(*net.TCPAddr).Port

	reg.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(port1)})
	reg.Register(&broker.Broker{ID: 2, Host: "127.0.0.1", Port: int32(port2)})
	metaMgr.Brokers = reg

	topic := metaMgr.CreateTopic("test-topic", 1)
	origLeader := topic.Partitions[0].LeaderID

	ctrl := NewController(metaMgr, reg, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl.Start(ctx)

	time.Sleep(400 * time.Millisecond)
	cancel()
	ctrl.Wait()

	if topic.Partitions[0].LeaderID != origLeader {
		t.Errorf("leader changed unexpectedly from %d to %d", origLeader, topic.Partitions[0].LeaderID)
	}
	if !ctrl.IsAlive(1) || !ctrl.IsAlive(2) {
		t.Error("both brokers should be alive")
	}
}

func TestController_BrokerRecovery(t *testing.T) {
	metaMgr := metadata.NewManager()
	reg := broker.NewRegistry(1)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l1.Close()
	port1 := l1.Addr().(*net.TCPAddr).Port

	port2 := freePort(t)

	reg.Register(&broker.Broker{ID: 1, Host: "127.0.0.1", Port: int32(port1)})
	reg.Register(&broker.Broker{ID: 2, Host: "127.0.0.1", Port: int32(port2)})
	metaMgr.Brokers = reg

	ctrl := NewController(metaMgr, reg, 100*time.Millisecond)
	ctrl.failureThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl.Start(ctx)

	// Wait for broker 2 to be declared dead
	time.Sleep(400 * time.Millisecond)
	if ctrl.IsAlive(2) {
		t.Fatal("broker 2 should be dead by now")
	}

	// Start a listener on broker 2's port to simulate recovery
	l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port2))
	if err != nil {
		t.Fatalf("listen on recovery port: %v", err)
	}
	defer l2.Close()

	// Wait for controller to detect recovery
	time.Sleep(300 * time.Millisecond)
	cancel()
	ctrl.Wait()

	if !ctrl.IsAlive(2) {
		t.Error("broker 2 should be alive after recovery")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}
