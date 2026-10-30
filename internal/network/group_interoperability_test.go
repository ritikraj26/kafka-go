package network

import (
	"bytes"
	"net"
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

	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation, sync.MemberID = join.Group, joined.Generation, joined.MemberID
	assignment := []byte("opaque assignment")
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: joined.MemberID, MemberAssignment: assignment}}
	synced := kmsg.NewPtrSyncGroupResponse()
	wireInterop(t, conn, sync, synced, 0)
	if synced.ErrorCode != 0 || !bytes.Equal(synced.MemberAssignment, assignment) {
		t.Fatalf("sync: %+v", synced)
	}

	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.Generation, heartbeat.MemberID = join.Group, joined.Generation, joined.MemberID
	alive := kmsg.NewPtrHeartbeatResponse()
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
