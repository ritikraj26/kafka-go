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

		var body []byte
		switch requestHeader.GetAPIKey() {
		case protocol.APIKeyApiVersions:
			body = apiversions.BuildBody(requestHeader.GetErrorCode())
		case protocol.APIKeyProduce:
			// TODO: implement Produce handler
			fmt.Printf("Produce API not yet implemented (api_key=%d)\n", protocol.APIKeyProduce)
			return
		case protocol.APIKeyFetch:
			// TODO: implement Fetch handler
			fmt.Printf("Fetch API not yet implemented (api_key=%d)\n", protocol.APIKeyFetch)
			return
		case protocol.APIKeyDescribeTopicPartitions:
			// TODO: implement DescribeTopicPartitions handler
			fmt.Printf("DescribeTopicPartitions API not yet implemented (api_key=%d)\n", protocol.APIKeyDescribeTopicPartitions)
			return
		default:
			fmt.Printf("Unknown API key: %d\n", requestHeader.GetAPIKey())
			return
		}

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
