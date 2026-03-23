package network

import (
	"encoding/binary"
	"fmt"
	"net"
)

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
