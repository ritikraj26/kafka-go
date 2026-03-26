package network

import (
	"log/slog"
	"net"

	apiversions "github.com/codecrafters-io/kafka-starter-go/internal/api/api_versions"
	describetopics "github.com/codecrafters-io/kafka-starter-go/internal/api/describe_topics"
	"github.com/codecrafters-io/kafka-starter-go/internal/api/fetch"
	apimetadata "github.com/codecrafters-io/kafka-starter-go/internal/api/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/api/produce"
	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

func handleConnection(conn net.Conn, metaMgr *metadata.Manager) {
	defer conn.Close()
	for {
		requestHeader := protocol.NewRequestHeader()
		if err := requestHeader.ReadFrom(conn); err != nil {
			logger.L.Debug("connection closed", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		// Parse the request into the appropriate type
		request, err := protocol.ParseRequest(requestHeader)
		if err != nil {
			logger.L.Error("failed to parse request", "api_key", requestHeader.GetAPIKey(), "err", err)
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
			// Handle Produce request (uses response header v1)
			body = produce.BuildBody(req, metaMgr)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.FetchRequest:
			// Handle Fetch request (uses response header v1)
			body = fetch.BuildBody(req, metaMgr)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.DescribeTopicPartitionsRequest:
			// Handle DescribeTopicPartitions request (uses response header v1)
			body = describetopics.BuildBody(req, metaMgr)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.MetadataRequest:
			// Handle Metadata request (uses response header v1 — flexible)
			body = apimetadata.BuildBody(req, metaMgr)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		default:
			logger.L.Warn("unhandled request type", "type", slog.AnyValue(req))
			return
		}

		if err != nil {
			logger.L.Error("failed to serialize response", "err", err)
			return
		}

		if _, err = conn.Write(serializedResponse); err != nil {
			logger.L.Error("failed to write response", "remote", conn.RemoteAddr(), "err", err)
		}
	}
}
