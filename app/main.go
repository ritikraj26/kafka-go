package main

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ritiraj/kafka-go/internal/broker"
	"github.com/ritiraj/kafka-go/internal/controller"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/network"
	"github.com/ritiraj/kafka-go/internal/replication"
)

func main() {
	// Graceful shutdown on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Broker configuration from env (defaults: 3 simulated brokers, ID=1, RF=3)
	brokerID := int32(envInt("BROKER_ID", 1))
	numBrokers := envInt("NUM_BROKERS", 3)
	replicationFactor := envInt("REPLICATION_FACTOR", 3)

	// Create broker registry with simulated brokers
	reg := broker.NewRegistry(brokerID)
	for i := 0; i < numBrokers; i++ {
		reg.Register(&broker.Broker{
			ID:   int32(i + 1),
			Host: "localhost",
			Port: int32(9092 + i),
		})
	}

	// Create metadata manager
	metaMgr := metadata.NewManager()
	metaMgr.Brokers = reg
	metaMgr.ReplicationFactor = replicationFactor

	// Create consumer group coordinator
	coord := coordinator.NewCoordinator()
	coord.RebalanceDelay = 3 * time.Second // hold join round open so all members can arrive

	// Parse server.properties if provided
	logDir := "/tmp/kraft-broker-logs" // Default log directory

	if len(os.Args) > 1 {
		propsFile := os.Args[1]
		if dir, err := parseLogDir(propsFile); err == nil && dir != "" {
			logDir = dir
		}
	}

	// Load existing topics from disk
	if err := metaMgr.LoadTopicsFromDisk(logDir); err != nil {
		logger.L.Warn("failed to load topics from disk", "err", err)
	}

	// Load committed offsets from disk
	coord.LoadOffsets(logDir)

	// Start ISR maintenance (evaluates ISR every 1s)
	lagTimeMaxMs := int64(envInt("REPLICA_LAG_TIME_MAX_MS", 10000))
	isrMgr := replication.NewISRManager(metaMgr, 1*time.Second, lagTimeMaxMs)
	isrMgr.Start(ctx)

	// Start controller for broker health monitoring and automatic leader failover
	ctrl := controller.NewController(metaMgr, reg, 2*time.Second)
	ctrl.Start(ctx)

	// Start a FollowerManager + TCP listener per broker.
	// Each broker replicates partitions where it is a follower from the leader.
	var followers []*replication.FollowerManager
	for _, b := range reg.All() {
		fm := replication.NewFollowerManager(metaMgr, b.ID, 100*time.Millisecond)
		fm.Start(ctx)
		followers = append(followers, fm)

		// The primary broker (brokerID) listens on the main goroutine below.
		// All other brokers listen in background goroutines.
		if b.ID != brokerID {
			go network.StartOnPort(ctx, b.Port, metaMgr, coord, b.ID)
		}
	}

	// Start primary broker listener (blocks until ctx is cancelled)
	network.Start(ctx, metaMgr, coord, brokerID)

	// Wait for all followers and ISR manager to finish
	for _, fm := range followers {
		fm.Wait()
	}
	isrMgr.Wait()
	ctrl.Wait()
}

// parseLogDir reads server.properties and extracts log.dirs property
func parseLogDir(propsFile string) (string, error) {
	file, err := os.Open(propsFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Look for log.dirs property
		if strings.HasPrefix(line, "log.dirs=") {
			value := strings.TrimPrefix(line, "log.dirs=")
			return strings.TrimSpace(value), nil
		}
	}

	return "", scanner.Err()
}

// envInt reads an integer from an environment variable with a default.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
