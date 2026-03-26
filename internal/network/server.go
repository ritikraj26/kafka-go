package network

import (
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// Start binds to port 9092 and accepts incoming Kafka client connections.
func Start(metaMgr *metadata.Manager) {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		logger.L.Error("failed to bind to port 9092", "err", err)
		os.Exit(1)
	}
	defer listener.Close()
	logger.L.Info("broker listening", "port", 9092)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.L.Error("failed to accept connection", "err", err)
			continue
		}

		// Handle each connection in a separate goroutine (concurrent clients)
		go handleConnection(conn, metaMgr)
	}
}
