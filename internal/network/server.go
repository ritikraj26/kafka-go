package network

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// Start binds to port 9092 and accepts incoming Kafka client connections.
// It blocks until ctx is cancelled, then drains in-flight connections.
func Start(ctx context.Context, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		logger.L.Error("failed to bind to port 9092", "err", err)
		return
	}
	logger.L.Info("broker listening", "port", 9092)
	Serve(ctx, listener, metaMgr, coord, localBrokerID)
}

// StartOnPort binds to the given port and accepts incoming Kafka client connections.
// It blocks until ctx is cancelled, then drains in-flight connections.
func StartOnPort(ctx context.Context, port int32, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L.Error("failed to bind", "port", port, "err", err)
		return
	}
	logger.L.Info("broker listening", "port", port)
	Serve(ctx, listener, metaMgr, coord, localBrokerID)
}

// Serve accepts connections on an existing listener until ctx is cancelled.
// Exported for use in integration tests with a custom listener.
func Serve(ctx context.Context, listener net.Listener, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	var wg sync.WaitGroup

	// Close the listener when ctx is cancelled so Accept() unblocks.
	go func() {
		<-ctx.Done()
		logger.L.Info("shutting down broker")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Expected error after listener.Close() during shutdown.
			if ctx.Err() != nil {
				break
			}
			logger.L.Error("failed to accept connection", "err", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConnection(conn, metaMgr, coord, localBrokerID)
		}()
	}

	logger.L.Info("waiting for in-flight connections to drain")
	wg.Wait()
	logger.L.Info("broker stopped")
}
