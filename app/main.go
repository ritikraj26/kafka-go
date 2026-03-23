package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on port 9092...")

	for {
		// Accept incoming connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
			continue
		}

		// Handle each connection in a separate go routine
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Prepare the response
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 0) // message_size: 0
	binary.BigEndian.PutUint32(response[4:8], 7) // correlation_id: 7

	// Send the response
	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error writing response: ", err)
	}
}
