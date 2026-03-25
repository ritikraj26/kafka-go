package network

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
)

// Start binds to port 9092 and accepts incoming Kafka client connections.
func Start(metaMgr *metadata.Manager) {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on port 9092...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
			continue
		}

		// Handle each connection in a separate goroutine (concurrent clients)
		go handleConnection(conn, metaMgr)
	}
}
