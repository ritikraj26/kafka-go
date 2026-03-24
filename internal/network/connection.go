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

		// Parse the request into the appropriate type
		request, err := protocol.ParseRequest(requestHeader)
		if err != nil {
			fmt.Printf("Error parsing request: %v\n", err)
			return
		}

		var body []byte
		switch req := request.(type) {
		case *protocol.ApiVersionsRequest:
			// Handle ApiVersions request
			body = apiversions.BuildBody(requestHeader.GetErrorCode())

		case *protocol.ProduceRequest:
			// TODO: implement Produce handler
			fmt.Printf("Produce API not yet implemented (api_key=%d)\n", req.GetAPIKey())
			return

		case *protocol.FetchRequest:
			// TODO: implement Fetch handler
			fmt.Printf("Fetch API not yet implemented (api_key=%d)\n", req.GetAPIKey())
			return

		case *protocol.DescribeTopicPartitionsRequest:
			// TODO: implement DescribeTopicPartitions handler
			fmt.Printf("DescribeTopicPartitions API not yet implemented (api_key=%d)\n", req.GetAPIKey())
			return

		default:
			fmt.Printf("Unknown request type: %T\n", req)
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
