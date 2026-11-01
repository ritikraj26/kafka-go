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

func handleConnection(ctx context.Context, conn net.Conn, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32) {
	admission, err := NewAdmission(DefaultLimits())
	if err != nil {
		logger.L.Error("invalid connection admission defaults", "err", err)
		conn.Close()
		return
	}
	handleLimitedConnection(ctx, conn, m, c, brokerID, admission, false)
}

func handleLimitedConnection(ctx context.Context, conn net.Conn, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32, admission *Admission, replication bool) {
	defer conn.Close()
	for ctx.Err() == nil {
		if !handleNextRequest(ctx, conn, m, c, brokerID, admission, replication) {
			return
		}
	}
}

// Scope the reservation to exactly one request, including acks=0 and error exits.
func handleNextRequest(ctx context.Context, conn net.Conn, m *metadata.Manager, c *coordinator.Coordinator, brokerID int32, admission *Admission, replication bool) bool {
	if err := conn.SetReadDeadline(time.Now().Add(connectionReadTimeout)); err != nil {
		logger.L.Error("failed to set request deadline", "err", err)
		return false
	}
	header := protocol.NewRequestHeader()
	size, err := header.ReadSize(conn)
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, io.EOF) {
			logger.L.Warn("failed to read frame size", "err", err)
		}
		return false
	}
	release, err := admission.Reserve(ctx, size)
	if err != nil {
		if ctx.Err() == nil && admission.logRejection() {
			logger.L.Warn("request admission rejected; closing connection", "bytes", size, "replication", replication, "err", err, "stats", admission.Stats())
		}
		return false
	}
	defer release()
	defer header.ReleaseFrame()
	if err := ctx.Err(); err != nil {
		return false
	}
	if err := header.ReadFrame(conn); err != nil {
		if ctx.Err() == nil {
			logger.L.Warn("failed to read request frame", "err", err)
		}
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	if replication && (header.GetAPIKey() != protocol.APIKeyFetch || header.GetAPIVersion() != 0) {
		logger.L.Warn("non-replication API on internal listener", "api_key", header.GetAPIKey())
		return false
	}
	var body []byte
	var requestErr error
	if header.GetAPIKey() == protocol.APIKeyApiVersions && header.GetErrorCode() != protocol.ErrNone {
		body = apiversions.BuildBodyVersion(0, protocol.ErrUnsupportedVersion)
	} else {
		request, err := protocol.ParseRequest(header)
		if err != nil {
			logger.L.Warn("rejecting request", "api_key", header.GetAPIKey(), "version", header.GetAPIVersion(), "err", err)
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		switch req := request.(type) {
		case *protocol.ApiVersionsRequest:
			body = apiversions.BuildBodyVersion(header.GetAPIVersion(), protocol.ErrNone)
		case *protocol.ProduceRequest:
			var failed bool
			body, failed = produce.ProcessContext(ctx, req, m, brokerID)
			if req.Acks == 0 {
				return !failed && ctx.Err() == nil
			}
		case *protocol.FetchRequest:
			if (req.ReplicaID >= 0) != replication {
				logger.L.Warn("fetch role does not match listener", "replica_id", req.ReplicaID, "replication_listener", replication)
				return false
			}
			if header.GetAPIVersion() == 0 {
				body, requestErr = fetch.BuildBodyV0Context(ctx, req, m, brokerID)
			} else {
				body, requestErr = fetch.BuildBodyContext(ctx, req, m, brokerID)
			}
		case *protocol.DescribeTopicPartitionsRequest:
			body = describetopics.BuildBody(req, m)
		case *protocol.MetadataRequest:
			body = apimetadata.BuildBody(req, m)
		case *protocol.FindCoordinatorRequest:
			body = findcoordinator.BuildBody(req, m.Brokers)
		case *protocol.JoinGroupRequest:
			body = joingroup.BuildBodyContext(ctx, req, c)
		case *protocol.SyncGroupRequest:
			body = syncgroup.BuildBodyContext(ctx, req, c)
		case *protocol.HeartbeatRequest:
			body = heartbeat.BuildBody(req, c)
		case *protocol.LeaveGroupRequest:
			body = leavegroup.BuildBody(req, c)
		case *protocol.OffsetCommitRequest:
			body = offsetcommit.BuildBodyContext(ctx, req, c)
		case *protocol.OffsetFetchRequest:
			body = offsetfetch.BuildBody(req, c)
		case *protocol.ListOffsetsRequest:
			body = listoffsets.BuildBody(req, m, brokerID)
		default:
			logger.L.Error("unhandled request", "api_key", header.GetAPIKey())
			return false
		}
	}
	if ctx.Err() != nil {
		return false
	}
	if requestErr != nil {
		logger.L.Warn("request cancelled or failed", "api_key", header.GetAPIKey(), "err", requestErr)
		return false
	}
	response := protocol.NewResponse(header.GetCorrelationID(), body)
	if protocol.ResponseHeaderVersion(header.GetAPIKey(), header.GetAPIVersion()) == 1 {
		response = protocol.NewResponseV1(header.GetCorrelationID(), body)
	}
	data, err := response.Serialize()
	if err != nil {
		logger.L.Error("failed to serialize response", "err", err)
		return false
	}
	if err := conn.SetWriteDeadline(time.Now().Add(connectionWriteTimeout)); err != nil {
		logger.L.Error("failed to set response deadline", "err", err)
		return false
	}
	if _, err := conn.Write(data); err != nil {
		if ctx.Err() == nil {
			logger.L.Warn("failed to write response", "err", err)
		}
		return false
	}
	return true
}
