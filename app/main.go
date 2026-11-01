package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
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

const (
	rebalanceDelay              = 3 * time.Second
	isrCheckInterval            = time.Second
	controllerHealthInterval    = 2 * time.Second
	followerReplicationInterval = 100 * time.Millisecond
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil && !(errors.Is(err, context.Canceled) && ctx.Err() != nil) {
		logger.L.Error("broker failed", "err", err)
		os.Exit(1)
	}
}

type runtimeConfig struct {
	brokerID                              int32
	numBrokers, replicationFactor, minISR int
	lagTimeMaxMs                          int64
	logDir                                string
	limits                                network.Limits
	memoryLimits                          network.MemoryLimits
}

type listenFunc func(context.Context, string, string) (net.Listener, error)

func run(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	return runWithConfig(ctx, cfg, (&net.ListenConfig{}).Listen)
}

func runWithConfig(parent context.Context, cfg runtimeConfig, listen listenFunc) error {
	clientAdmission, err := network.NewAdmissionWithMemory(cfg.limits, cfg.memoryLimits)
	if err != nil {
		return fmt.Errorf("client admission configuration: %w", err)
	}
	replicaAdmission, err := network.NewReplicationAdmission(cfg.numBrokers)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	reg := broker.NewRegistry(cfg.brokerID)
	for i := 0; i < cfg.numBrokers; i++ {
		reg.Register(&broker.Broker{ID: int32(i + 1), Host: "localhost", Port: int32(9092 + i)})
	}
	m := metadata.NewManager()
	m.Brokers, m.ReplicationFactor, m.MinInSyncReplicas = reg, cfg.replicationFactor, cfg.minISR
	c := coordinator.NewCoordinator()
	c.RebalanceDelay = rebalanceDelay
	isr := replication.NewISRManager(m, isrCheckInterval, cfg.lagTimeMaxMs)
	ctrl := controller.NewController(m, reg, controllerHealthInterval)
	var followers []*replication.FollowerManager
	var listeners map[int32]net.Listener
	var replicaListeners map[int32]net.Listener
	var servers sync.WaitGroup
	defer func() {
		cancel()
		closeListeners(listeners)
		closeListeners(replicaListeners)
		m.Schemas.Close()
		c.Close()
		servers.Wait()
		for _, fm := range followers {
			fm.Wait()
		}
		isr.Wait()
		ctrl.Wait()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.LoadTopicsFromDisk(cfg.logDir); err != nil {
		return fmt.Errorf("failed to recover partition logs; files were preserved: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.LoadOffsets(cfg.logDir); err != nil {
		return fmt.Errorf("failed to recover consumer offsets; files were preserved: %w", err)
	}
	listeners, err = bindListeners(ctx, reg, listen)
	if err != nil {
		return err
	}
	replicaListeners, err = bindReplicationListeners(ctx, reg, listen)
	if err != nil {
		return err
	}
	logger.L.Info("client admission configured", "limits", cfg.limits)
	results := make(chan error, len(listeners)+len(replicaListeners))
	for id, listener := range listeners {
		servers.Add(1)
		go func() {
			defer servers.Done()
			results <- network.ServeWithAdmission(ctx, listener, m, c, id, clientAdmission)
		}()
	}
	for id, listener := range replicaListeners {
		servers.Add(1)
		go func() {
			defer servers.Done()
			results <- network.ServeReplication(ctx, listener, m, c, id, replicaAdmission)
		}()
	}
	if err := isr.Start(ctx); err != nil {
		return err
	}
	if err := ctrl.Start(ctx); err != nil {
		return err
	}
	for _, b := range reg.All() {
		fm := replication.NewFollowerManager(m, b.ID, followerReplicationInterval)
		if err := fm.Start(ctx); err != nil {
			return err
		}
		followers = append(followers, fm)
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-results:
		return err
	}
}

func bindListeners(ctx context.Context, reg *broker.Registry, listen listenFunc) (map[int32]net.Listener, error) {
	listeners := make(map[int32]net.Listener)
	ids := reg.IDs()
	slices.Sort(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			closeListeners(listeners)
			return nil, err
		}
		b := reg.Get(id)
		listener, err := listen(ctx, "tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(int(b.Port))))
		if err != nil {
			closeListeners(listeners)
			return nil, fmt.Errorf("bind broker %d on port %d: %w", id, b.Port, err)
		}
		listeners[id] = listener
		if addr, ok := listener.Addr().(*net.TCPAddr); ok {
			b.Port = int32(addr.Port)
		}
	}
	return listeners, nil
}

func closeListeners(listeners map[int32]net.Listener) {
	for id, listener := range listeners {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.L.Warn("failed to close broker listener", "broker", id, "err", err)
		}
	}
}

func bindReplicationListeners(ctx context.Context, reg *broker.Registry, listen listenFunc) (map[int32]net.Listener, error) {
	listeners := make(map[int32]net.Listener)
	ids := reg.IDs()
	slices.Sort(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			closeListeners(listeners)
			return nil, err
		}
		listener, err := listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			closeListeners(listeners)
			return nil, fmt.Errorf("bind replication listener %d: %w", id, err)
		}
		listeners[id] = listener
		addr, ok := listener.Addr().(*net.TCPAddr)
		if !ok || !addr.IP.IsLoopback() {
			closeListeners(listeners)
			return nil, fmt.Errorf("replication endpoint must be loopback")
		}
		reg.Get(id).ReplicationAddress = listener.Addr().String()
	}
	return listeners, nil
}

func loadConfig(args []string) (runtimeConfig, error) {
	var cfg runtimeConfig
	var err error
	if len(args) > 1 {
		return cfg, fmt.Errorf("expected at most one server.properties path")
	}
	cfg.logDir = "/tmp/kraft-broker-logs"
	if len(args) == 1 {
		dir, err := parseLogDir(args[0])
		if err != nil {
			return cfg, fmt.Errorf("read server properties: %w", err)
		}
		if dir != "" {
			cfg.logDir = dir
		}
	}
	cfg.numBrokers, err = envIntInRange("NUM_BROKERS", 3, 1, 65535-9092+1)
	if err != nil {
		return cfg, err
	}
	id, err := envIntInRange("BROKER_ID", 1, 1, cfg.numBrokers)
	if err != nil {
		return cfg, err
	}
	cfg.brokerID = int32(id)
	cfg.replicationFactor, err = envIntInRange("REPLICATION_FACTOR", 3, 1, cfg.numBrokers)
	if err != nil {
		return cfg, err
	}
	cfg.minISR, err = minInSyncReplicasFromEnv()
	if err != nil {
		return cfg, fmt.Errorf("invalid minimum ISR configuration: %w", err)
	}
	lag, err := envIntInRange("REPLICA_LAG_TIME_MAX_MS", 10000, 1, math.MaxInt32)
	if err != nil {
		return cfg, err
	}
	cfg.lagTimeMaxMs = int64(lag)
	cfg.limits = network.DefaultLimits()
	cfg.limits.MaxConnections, err = envIntInRange("MAX_CONNECTIONS", cfg.limits.MaxConnections, 1, math.MaxInt32)
	if err != nil {
		return cfg, err
	}
	maxFrame, err := envIntInRange("MAX_REQUEST_BYTES", int(cfg.limits.MaxRequestBytes), 8, int(cfg.limits.MaxRequestBytes))
	if err != nil {
		return cfg, err
	}
	cfg.limits.MaxRequestBytes = int64(maxFrame)
	budget, err := envIntInRange("MAX_INFLIGHT_REQUEST_BYTES", int(cfg.limits.MaxInFlightBytes), maxFrame, math.MaxInt)
	if err != nil {
		return cfg, err
	}
	cfg.limits.MaxInFlightBytes = int64(budget)
	wait, err := envIntInRange("ADMISSION_WAIT_MS", int(cfg.limits.AdmissionWait/time.Millisecond), 1, 30000)
	if err != nil {
		return cfg, err
	}
	cfg.limits.AdmissionWait = time.Duration(wait) * time.Millisecond
	cfg.memoryLimits = network.DefaultMemoryLimits()
	decoded, err := envIntInRange("MAX_DECODED_REQUEST_BYTES", int(cfg.memoryLimits.MaxDecodedBytes), 1024, 100<<20)
	if err != nil {
		return cfg, err
	}
	cfg.memoryLimits.MaxDecodedBytes = int64(decoded)
	decodeBudget, err := envIntInRange("MAX_INFLIGHT_DECODE_BYTES", int(cfg.memoryLimits.DecodedBudget), decoded, math.MaxInt)
	if err != nil {
		return cfg, err
	}
	cfg.memoryLimits.DecodedBudget = int64(decodeBudget)
	cfg.memoryLimits.MaxEntries, err = envIntInRange("MAX_REQUEST_ENTRIES", cfg.memoryLimits.MaxEntries, 1, 100000)
	if err != nil {
		return cfg, err
	}
	cfg.memoryLimits.FetchMaxBytes, err = envIntInRange("FETCH_MAX_BYTES", cfg.memoryLimits.FetchMaxBytes, 1, 100<<20)
	if err != nil {
		return cfg, err
	}
	fetchBudget, err := envIntInRange("MAX_INFLIGHT_FETCH_BYTES", int(cfg.memoryLimits.FetchBudget), 1, math.MaxInt)
	if err != nil {
		return cfg, err
	}
	cfg.memoryLimits.FetchBudget = int64(fetchBudget)
	if err := cfg.memoryLimits.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func parseLogDir(propsFile string) (string, error) {
	file, err := os.Open(propsFile)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "log.dirs=") {
			return strings.TrimSpace(strings.TrimPrefix(line, "log.dirs=")), nil
		}
	}
	return "", scanner.Err()
}

func envIntInRange(key string, def, min, max int) (int, error) {
	value, present := os.LookupEnv(key)
	if !present {
		value = strconv.Itoa(def)
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be between %d and %d, got %q", key, min, max, value)
	}
	return n, nil
}

func minInSyncReplicasFromEnv() (int, error) {
	return envIntInRange("MIN_INSYNC_REPLICAS", metadata.DefaultMinInSyncReplicas, 1, math.MaxInt32)
}
