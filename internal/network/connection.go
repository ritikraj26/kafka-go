package network

import (
	"errors"
	"io"
	"net"
	"time"

	apiversions "github.com/ritiraj/kafka-go/internal/api/api_versions"
	describetopics "github.com/ritiraj/kafka-go/internal/api/describe_topics"
	"github.com/ritiraj/kafka-go/internal/api/fetch"
	findcoordinator "github.com/ritiraj/kafka-go/internal/api/find_coordinator"
	"github.com/ritiraj/kafka-go/internal/api/heartbeat"
	joingroup "github.com/ritiraj/kafka-go/internal/api/join_group"
	leavegroup "github.com/ritiraj/kafka-go/internal/api/leave_group"
	listoffsets "github.com/ritiraj/kafka-go/internal/api/list_offsets"
	apimetadata "github.com/ritiraj/kafka-go/internal/api/metadata"
	offsetcommit "github.com/ritiraj/kafka-go/internal/api/offset_commit"
	offsetfetch "github.com/ritiraj/kafka-go/internal/api/offset_fetch"
	"github.com/ritiraj/kafka-go/internal/api/produce"
	syncgroup "github.com/ritiraj/kafka-go/internal/api/sync_group"
	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

const connectionReadTimeout = 30 * time.Second

func handleConnection(conn net.Conn, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	defer conn.Close()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(connectionReadTimeout)); err != nil {
			logger.L.Error("failed to set request deadline", "err", err)
			return
		}
		header := protocol.NewRequestHeader()
		if err := header.ReadFrom(conn); err != nil {
			if errors.Is(err, io.EOF) {
				logger.L.Debug("client disconnected")
			} else {
				logger.L.Warn("failed to read request frame", "err", err)
			}
			return
		}
		var body []byte
		if header.GetAPIKey() == protocol.APIKeyApiVersions && header.GetErrorCode() != protocol.ErrNone {
			// Kafka's bootstrap fallback always uses ApiVersions response v0.
			body = apiversions.BuildBodyVersion(0, protocol.ErrUnsupportedVersion)
		} else {
			request, err := protocol.ParseRequest(header)
			if err != nil {
				logger.L.Warn("rejecting request", "api_key", header.GetAPIKey(), "version", header.GetAPIVersion(), "err", err)
				return
			}
			switch req := request.(type) {
			case *protocol.ApiVersionsRequest:
				body = apiversions.BuildBodyVersion(header.GetAPIVersion(), protocol.ErrNone)
			case *protocol.ProduceRequest:
				var failed bool
				body, failed = produce.Process(req, metaMgr, localBrokerID)
				if req.Acks == 0 {
					if failed {
						return
					}
					continue
				}
			case *protocol.FetchRequest:
				if header.GetAPIVersion() == 0 {
					body = fetch.BuildBodyV0(req, metaMgr, localBrokerID)
				} else {
					body = fetch.BuildBody(req, metaMgr, localBrokerID)
				}
			case *protocol.DescribeTopicPartitionsRequest:
				body = describetopics.BuildBody(req, metaMgr)
			case *protocol.MetadataRequest:
				body = apimetadata.BuildBody(req, metaMgr)
			case *protocol.FindCoordinatorRequest:
				body = findcoordinator.BuildBody(req, metaMgr.Brokers)
			case *protocol.JoinGroupRequest:
				body = joingroup.BuildBody(req, coord)
			case *protocol.SyncGroupRequest:
				body = syncgroup.BuildBody(req, coord)
			case *protocol.HeartbeatRequest:
				body = heartbeat.BuildBody(req, coord)
			case *protocol.LeaveGroupRequest:
				body = leavegroup.BuildBody(req, coord)
			case *protocol.OffsetCommitRequest:
				body = offsetcommit.BuildBody(req, coord)
			case *protocol.OffsetFetchRequest:
				body = offsetfetch.BuildBody(req, coord)
			case *protocol.ListOffsetsRequest:
				body = listoffsets.BuildBody(req, metaMgr, localBrokerID)
			default:
				logger.L.Error("unhandled request", "api_key", header.GetAPIKey())
				return
			}
		}
		response := protocol.NewResponse(header.GetCorrelationID(), body)
		if protocol.ResponseHeaderVersion(header.GetAPIKey(), header.GetAPIVersion()) == 1 {
			response = protocol.NewResponseV1(header.GetCorrelationID(), body)
		}
		data, err := response.Serialize()
		if err != nil {
			logger.L.Error("failed to serialize response", "err", err)
			return
		}
		if _, err := conn.Write(data); err != nil {
			logger.L.Warn("failed to write response", "err", err)
			return
		}
	}
}
