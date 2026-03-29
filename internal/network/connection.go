package network

import (
	"log/slog"
	"net"
	"time"

	apiversions "github.com/codecrafters-io/kafka-starter-go/internal/api/api_versions"
	describetopics "github.com/codecrafters-io/kafka-starter-go/internal/api/describe_topics"
	"github.com/codecrafters-io/kafka-starter-go/internal/api/fetch"
	findcoordinator "github.com/codecrafters-io/kafka-starter-go/internal/api/find_coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/api/heartbeat"
	joingroup "github.com/codecrafters-io/kafka-starter-go/internal/api/join_group"
	leavegroup "github.com/codecrafters-io/kafka-starter-go/internal/api/leave_group"
	listoffsets "github.com/codecrafters-io/kafka-starter-go/internal/api/list_offsets"
	apimetadata "github.com/codecrafters-io/kafka-starter-go/internal/api/metadata"
	offsetcommit "github.com/codecrafters-io/kafka-starter-go/internal/api/offset_commit"
	offsetfetch "github.com/codecrafters-io/kafka-starter-go/internal/api/offset_fetch"
	"github.com/codecrafters-io/kafka-starter-go/internal/api/produce"
	syncgroup "github.com/codecrafters-io/kafka-starter-go/internal/api/sync_group"
	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

const connectionReadTimeout = 30 * time.Second

func handleConnection(conn net.Conn, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	defer conn.Close()
	for {
		// Set read deadline to prevent zombie connections from holding goroutines
		conn.SetReadDeadline(time.Now().Add(connectionReadTimeout))

		requestHeader := protocol.NewRequestHeader()
		if err := requestHeader.ReadFrom(conn); err != nil {
			logger.L.Error("error reading request header", "err", err)
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
			body = produce.BuildBody(req, metaMgr, localBrokerID)
			response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.FetchRequest:
			// Handle Fetch request — v0 uses non-flexible response, v16 uses flexible
			if requestHeader.GetAPIVersion() == 0 {
				body = fetch.BuildBodyV0(req, metaMgr)
				response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
				serializedResponse, err = response.Serialize()
			} else {
				body = fetch.BuildBody(req, metaMgr)
				response := protocol.NewResponseV1(requestHeader.GetCorrelationID(), body)
				serializedResponse, err = response.Serialize()
			}

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

		// --- Consumer group APIs (v0, response header v0) ---

		case *protocol.FindCoordinatorRequest:
			body = findcoordinator.BuildBody(req, metaMgr.Brokers)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.JoinGroupRequest:
			body = joingroup.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.SyncGroupRequest:
			body = syncgroup.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.HeartbeatRequest:
			body = heartbeat.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.LeaveGroupRequest:
			body = leavegroup.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.OffsetCommitRequest:
			body = offsetcommit.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.OffsetFetchRequest:
			body = offsetfetch.BuildBody(req, coord)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		case *protocol.ListOffsetsRequest:
			body = listoffsets.BuildBody(req, metaMgr)
			response := protocol.NewResponse(requestHeader.GetCorrelationID(), body)
			serializedResponse, err = response.Serialize()

		default:
			logger.L.Warn("unhandled request type", "type", slog.AnyValue(req))
			return
		}

		if err != nil {
			logger.L.Error("error serializing response", "err", err)
			return
		}

		_, err = conn.Write(serializedResponse)
		if err != nil {
			logger.L.Error("error writing response", "err", err)
			return
		}
	}
}
