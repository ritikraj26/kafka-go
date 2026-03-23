package network

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Buffer to read the request
	requestHeader := protocol.NewRequestHeader()
	if err := requestHeader.ReadFrom(conn); err != nil {
		fmt.Println("Error reading request header: ", err)
		return
	}

	// Prepare the response
	response := protocol.NewResponse(requestHeader.CorrelationID())
	serializedResponse, err := response.Serialize()
	if err != nil {
		fmt.Println("Error serializing response: ", err)
		return
	}

	// Send the response
	_, err = conn.Write(serializedResponse)
	if err != nil {
		fmt.Println("Error writing response: ", err)
	}
}
