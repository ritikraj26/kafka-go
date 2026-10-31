package network

import (
	"context"
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
const connectionWriteTimeout = 30 * time.Second

func handleConnection(ctx context.Context, conn net.Conn, metaMgr *metadata.Manager, coord *coordinator.Coordinator, localBrokerID int32) {
	defer conn.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(connectionReadTimeout)); err != nil {
			logger.L.Error("failed to set request deadline", "err", err)
			return
		}
		header := protocol.NewRequestHeader()
		if err := header.ReadFrom(conn); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, io.EOF) {
				logger.L.Debug("client disconnected")
			} else {
				logger.L.Warn("failed to read request frame", "err", err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		var body []byte
		var requestErr error
		if header.GetAPIKey() == protocol.APIKeyApiVersions && header.GetErrorCode() != protocol.ErrNone {
			// Kafka's bootstrap fallback always uses ApiVersions response v0.
			body = apiversions.BuildBodyVersion(0, protocol.ErrUnsupportedVersion)
		} else {
			request, err := protocol.ParseRequest(header)
			if err != nil {
				logger.L.Warn("rejecting request", "api_key", header.GetAPIKey(), "version", header.GetAPIVersion(), "err", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
			switch req := request.(type) {
			case *protocol.ApiVersionsRequest:
				body = apiversions.BuildBodyVersion(header.GetAPIVersion(), protocol.ErrNone)
			case *protocol.ProduceRequest:
				var failed bool
				body, failed = produce.ProcessContext(ctx, req, metaMgr, localBrokerID)
				if req.Acks == 0 {
					if failed {
						return
					}
					continue
				}
			case *protocol.FetchRequest:
				if header.GetAPIVersion() == 0 {
					body, requestErr = fetch.BuildBodyV0Context(ctx, req, metaMgr, localBrokerID)
				} else {
					body, requestErr = fetch.BuildBodyContext(ctx, req, metaMgr, localBrokerID)
				}
			case *protocol.DescribeTopicPartitionsRequest:
				body = describetopics.BuildBody(req, metaMgr)
			case *protocol.MetadataRequest:
				body = apimetadata.BuildBody(req, metaMgr)
			case *protocol.FindCoordinatorRequest:
				body = findcoordinator.BuildBody(req, metaMgr.Brokers)
			case *protocol.JoinGroupRequest:
				body = joingroup.BuildBodyContext(ctx, req, coord)
			case *protocol.SyncGroupRequest:
				body = syncgroup.BuildBodyContext(ctx, req, coord)
			case *protocol.HeartbeatRequest:
				body = heartbeat.BuildBody(req, coord)
			case *protocol.LeaveGroupRequest:
				body = leavegroup.BuildBody(req, coord)
			case *protocol.OffsetCommitRequest:
				body = offsetcommit.BuildBodyContext(ctx, req, coord)
			case *protocol.OffsetFetchRequest:
				body = offsetfetch.BuildBody(req, coord)
			case *protocol.ListOffsetsRequest:
				body = listoffsets.BuildBody(req, metaMgr, localBrokerID)
			default:
				logger.L.Error("unhandled request", "api_key", header.GetAPIKey())
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		if requestErr != nil {
			logger.L.Warn("request cancelled or failed", "api_key", header.GetAPIKey(), "err", requestErr)
			return
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
		if err := conn.SetWriteDeadline(time.Now().Add(connectionWriteTimeout)); err != nil {
			logger.L.Error("failed to set response deadline", "err", err)
			return
		}
		if _, err := conn.Write(data); err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.L.Warn("failed to write response", "err", err)
			return
		}
	}
}
