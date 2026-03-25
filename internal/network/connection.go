package network

import (
	"fmt"
	"net"

	apiversions "github.com/codecrafters-io/kafka-starter-go/internal/api/api_versions"
	describetopics "github.com/codecrafters-io/kafka-starter-go/internal/api/describe_topics"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func handleConnection(conn net.Conn, metaMgr *metadata.Manager) {
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
		var serializedResponse []byte

		switch req := request.(type) {
		case *protocol.ApiVersionsRequest:
			// Handle ApiVersions request (uses response header v0)
			body = apiversions.BuildBody(requestHeader.GetErrorCode())
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.ProduceRequest:
			// TODO: implement Produce handler
			fmt.Printf("Produce API not yet implemented (api_key=%d)\n", req.GetAPIKey())
			return

		case *protocol.FetchRequest:
			// TODO: implement Fetch handler
			fmt.Printf("Fetch API not yet implemented (api_key=%d)\n", req.GetAPIKey())
			return

		case *protocol.DescribeTopicPartitionsRequest:
			// Handle DescribeTopicPartitions request (uses response header v1)
			body = describetopics.BuildBody(req, metaMgr)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		default:
			fmt.Printf("Unknown request type: %T\n", req)
			return
		}

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
