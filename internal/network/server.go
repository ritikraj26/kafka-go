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
	admission, err := NewAdmission(DefaultLimits())
	if err != nil {
		listener.Close()
		return err
	}
	return ServeWithAdmission(parent, listener, m, c, brokerID, admission)
}

// ServeWithAdmission shares client limits across all listeners using this controller.
func ServeWithAdmission(parent context.Context, listener net.Listener, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32, admission *Admission) error {
	if admission != nil && admission.replication {
		listener.Close()
		return fmt.Errorf("public listener cannot use protected replication capacity")
	}
	return serve(parent, listener, m, c, brokerID, admission, false)
}

// ServeReplication accepts only replica Fetch v0 traffic on a loopback listener.
// Its admission controller must be distinct from the public-client controller.
func ServeReplication(parent context.Context, listener net.Listener, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32, admission *Admission) error {
	if admission == nil || !admission.replication {
		listener.Close()
		return fmt.Errorf("internal listener requires a protected replication admission controller")
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !addr.IP.IsLoopback() {
		listener.Close()
		return fmt.Errorf("replication listener must be bound to loopback")
	}
	return serve(parent, listener, m, c, brokerID, admission, true)
}

func serve(parent context.Context, listener net.Listener, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32, admission *Admission, replication bool) error {
	if admission == nil {
		listener.Close()
		return fmt.Errorf("admission controller is required")
	}
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
	logger.L.Info("broker listening", "broker", brokerID, "address", listener.Addr(), "replication", replication)
	var serveErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				serveErr = fmt.Errorf("accept broker %d: %w", brokerID, err)
			}
			break
		}
		release, admitted := admission.tryConnection()
		if !admitted {
			conn.Close()
			if admission.logRejection() {
				logger.L.Warn("connection admission limit reached", "broker", brokerID, "replication", replication, "stats", admission.Stats())
			}
			continue
		}
		mu.Lock()
		if ctx.Err() != nil {
			mu.Unlock()
			conn.Close()
			release()
			break
		}
		active[conn] = struct{}{}
		handlers.Add(1)
		mu.Unlock()
		go func() {
			defer handlers.Done()
			defer release()
			defer func() {
				mu.Lock()
				delete(active, conn)
				mu.Unlock()
			}()
			handleLimitedConnection(ctx, conn, m, c, brokerID, admission, replication)
		}()
	}
	cancel()
	<-closed
	handlers.Wait()
	logger.L.Info("broker stopped", "broker", brokerID)
	return serveErr
}
