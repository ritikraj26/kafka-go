package network

import (
	"bytes"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIndependentCodecGroupLifecycle(t *testing.T) {
	address, _, _ := interopBroker(t)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	find := kmsg.NewPtrFindCoordinatorRequest()
	find.CoordinatorKey = "codec-group"
	found := kmsg.NewPtrFindCoordinatorResponse()
	wireInterop(t, conn, find, found, 0)
	if found.ErrorCode != 0 || found.NodeID != 1 || found.Port == 0 {
		t.Fatalf("find coordinator: %+v", found)
	}

	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = "codec-group", "consumer", 10000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("opaque subscription")}}
	joined := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, join, joined, 0)
	if joined.ErrorCode != 0 || joined.MemberID == "" || joined.LeaderID != joined.MemberID {
		t.Fatalf("join: %+v", joined)
	}
	// A member may heartbeat while the leader computes assignments, or retry a
	// Join whose response was lost. Neither should restart this generation.
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.Generation, heartbeat.MemberID = join.Group, joined.Generation, joined.MemberID
	alive := kmsg.NewPtrHeartbeatResponse()
	wireInterop(t, conn, heartbeat, alive, 0)
	if alive.ErrorCode != 0 {
		t.Fatalf("heartbeat before sync: %+v", alive)
	}
	join.MemberID = joined.MemberID
	retried := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, join, retried, 0)
	if !reflect.DeepEqual(retried, joined) {
		t.Fatalf("unchanged join retry: got %+v, want %+v", retried, joined)
	}

	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation, sync.MemberID = join.Group, joined.Generation, joined.MemberID
	assignment := []byte("opaque assignment")
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: joined.MemberID, MemberAssignment: assignment}}
	synced := kmsg.NewPtrSyncGroupResponse()
	wireInterop(t, conn, sync, synced, 0)
	if synced.ErrorCode != 0 || !bytes.Equal(synced.MemberAssignment, assignment) {
		t.Fatalf("sync: %+v", synced)
	}

	wireInterop(t, conn, heartbeat, alive, 0)
	if alive.ErrorCode != 0 {
		t.Fatalf("heartbeat: %+v", alive)
	}

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.Generation, commit.MemberID = join.Group, joined.Generation, joined.MemberID
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "events", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 3, Metadata: nil}}}}
	committed := kmsg.NewPtrOffsetCommitResponse()
	wireInterop(t, conn, commit, committed, 2)
	if len(committed.Topics) != 1 || len(committed.Topics[0].Partitions) != 1 || committed.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %+v", committed)
	}

	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = join.Group
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "events", Partitions: []int32{0}}}
	fetched := kmsg.NewPtrOffsetFetchResponse()
	wireInterop(t, conn, fetch, fetched, 1)
	if len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 1 || fetched.Topics[0].Partitions[0].Offset != 3 || fetched.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("offset fetch: %+v", fetched)
	}
	heartbeat.Generation++
	wireInterop(t, conn, heartbeat, alive, 0)
	if alive.ErrorCode != 22 {
		t.Fatalf("stale generation was not fenced: %+v", alive)
	}

	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = join.Group, joined.MemberID
	left := kmsg.NewPtrLeaveGroupResponse()
	wireInterop(t, conn, leave, left, 0)
	if left.ErrorCode != 0 {
		t.Fatalf("leave: %+v", left)
	}
}

func TestIndependentCodecRejectsIncompatibleGroupProtocols(t *testing.T) {
	address, _, _ := interopBroker(t)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = "negotiation", "consumer", 60000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
	joined := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, join, joined, 0)
	if joined.ErrorCode != 0 {
		t.Fatalf("initial join: %+v", joined)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation, sync.MemberID = join.Group, joined.Generation, joined.MemberID
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: joined.MemberID, MemberAssignment: []byte("assignment")}}
	synced := kmsg.NewPtrSyncGroupResponse()
	wireInterop(t, conn, sync, synced, 0)
	if synced.ErrorCode != 0 {
		t.Fatalf("initial sync: %+v", synced)
	}
	for _, tc := range []struct {
		name, protocolType string
		protocols          []kmsg.JoinGroupRequestProtocol
	}{
		{"disjoint assignors", "consumer", []kmsg.JoinGroupRequestProtocol{{Name: "roundrobin"}}},
		{"different type", "other", []kmsg.JoinGroupRequestProtocol{{Name: "range"}}},
		{"empty type", "", []kmsg.JoinGroupRequestProtocol{{Name: "range"}}},
		{"empty protocols", "consumer", nil},
		{"empty name", "consumer", []kmsg.JoinGroupRequestProtocol{{Name: ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := kmsg.NewPtrJoinGroupRequest()
			request.Group, request.ProtocolType, request.SessionTimeoutMillis = join.Group, tc.protocolType, 60000
			request.Protocols = tc.protocols
			response := kmsg.NewPtrJoinGroupResponse()
			wireInterop(t, conn, request, response, 0)
			if response.ErrorCode != 23 { // INCONSISTENT_GROUP_PROTOCOL
				t.Fatalf("join response=%+v, want error 23", response)
			}
		})
	}
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.Generation, heartbeat.MemberID = join.Group, joined.Generation, joined.MemberID
	alive := kmsg.NewPtrHeartbeatResponse()
	wireInterop(t, conn, heartbeat, alive, 0)
	if alive.ErrorCode != 0 {
		t.Fatalf("incompatible joins disrupted the original member: %+v", alive)
	}
}
