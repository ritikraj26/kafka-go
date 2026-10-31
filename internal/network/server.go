package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
)

// Start uses the local broker's advertised port, not a hard-coded primary port.
func Start(ctx context.Context, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32) error {
	if m.Brokers == nil || m.Brokers.Get(brokerID) == nil {
		return fmt.Errorf("local broker %d is not registered", brokerID)
	}
	return StartOnPort(ctx, m.Brokers.Get(brokerID).Port, m, c, brokerID)
}

func StartOnPort(ctx context.Context, port int32, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(int(port))))
	if err != nil {
		return fmt.Errorf("bind broker %d: %w", brokerID, err)
	}
	return Serve(ctx, listener, m, c, brokerID)
}

// Serve owns its listener and accepted connections, but not the shared metadata
// manager or coordinator. Cancellation closes sockets and waits for every handler.
func Serve(parent context.Context, listener net.Listener, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var mu sync.Mutex
	active := make(map[net.Conn]struct{})
	var handlers sync.WaitGroup
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		<-ctx.Done()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.L.Warn("failed to close listener", "broker", brokerID, "err", err)
		}
		mu.Lock()
		defer mu.Unlock()
		for conn := range active {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				logger.L.Debug("failed to close active connection", "err", err)
			}
		}
	}()
	logger.L.Info("broker listening", "broker", brokerID, "address", listener.Addr())
	var serveErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				serveErr = fmt.Errorf("accept broker %d: %w", brokerID, err)
			}
			break
		}
		mu.Lock()
		if ctx.Err() != nil {
			mu.Unlock()
			conn.Close()
			break
		}
		active[conn] = struct{}{}
		handlers.Add(1)
		mu.Unlock()
		go func() {
			defer handlers.Done()
			defer func() {
				mu.Lock()
				delete(active, conn)
				mu.Unlock()
			}()
			handleConnection(ctx, conn, m, c, brokerID)
		}()
	}
	cancel()
	<-closed
	handlers.Wait()
	logger.L.Info("broker stopped", "broker", brokerID)
	return serveErr
}
