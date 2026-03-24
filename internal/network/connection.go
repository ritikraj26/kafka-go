package network

import (
	"fmt"
	"net"

	apiversions "github.com/codecrafters-io/kafka-starter-go/internal/api/api_versions"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		requestHeader := protocol.NewRequestHeader()
		if err := requestHeader.ReadFrom(conn); err != nil {
			fmt.Println("Error reading request header: ", err)
			return
		}

		body := apiversions.BuildBody(requestHeader.GetErrorCode())
		response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)

		serializedResponse, err := response.Serialize()
		if err != nil {
			fmt.Println("Error serializing response: ", err)
			return
		}

		_, err = conn.Write(serializedResponse)
		if err != nil {
			fmt.Println("Error writing response: ", err)
		}
	}
}
