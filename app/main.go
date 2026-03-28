package main

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/network"
)

func main() {
	// Graceful shutdown on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create metadata manager
	metaMgr := metadata.NewManager()

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

	// Start server (blocks until ctx is cancelled)
	network.Start(ctx, metaMgr)
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
