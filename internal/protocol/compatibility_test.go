package protocol

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSharedHeadersWithIndependentRequests(t *testing.T) {
	find := kmsg.NewPtrFindCoordinatorRequest()
	find.CoordinatorKey = "group"
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = "group", "consumer", 1000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: nil}}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID = "group", "member"
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.MemberID = "group", "member"
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "group", "member"
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.MemberID, commit.Version = "group", "member", 2
	offsetFetch := kmsg.NewPtrOffsetFetchRequest()
	offsetFetch.Group, offsetFetch.Version = "group", 1
	list := kmsg.NewPtrListOffsetsRequest()
	list.Version = 1
	meta := kmsg.NewPtrMetadataRequest()
	meta.Version = 12
	meta.UnknownTags.Set(99, []byte{1, 2, 3})
	describe := kmsg.NewPtrDescribeTopicPartitionsRequest()
	produce := kmsg.NewPtrProduceRequest()
	produce.Version = 11
	fetch := kmsg.NewPtrFetchRequest()
	fetch.Version = 16
	fetch.UnknownTags.Set(99, []byte("unknown"))
	for _, request := range []kmsg.Request{find, join, sync, heartbeat, leave, commit, offsetFetch, list, meta, describe, produce, fetch} {
		t.Run(kmsg.NameForKey(request.Key()), func(t *testing.T) {
			for _, formatter := range []*kmsg.RequestFormatter{
				kmsg.NewRequestFormatter(),
				kmsg.NewRequestFormatter(kmsg.FormatterClientID(string(bytes.Repeat([]byte("x"), 300)))),
			} {
				frame := formatter.AppendRequest(nil, request, 42)
				header := makeRequestHeader(request.Key(), request.GetVersion(), frame[12:])
				if _, err := ParseRequest(header); err != nil {
					t.Fatal(err)
				}
				if header.clientID != nil && len(*header.clientID) != 300 {
					t.Fatal("client ID parsed incorrectly")
				}
				for length := 0; length < len(frame[12:]); length++ {
					truncated := makeRequestHeader(request.Key(), request.GetVersion(), frame[12:12+length])
					if _, err := ParseRequest(truncated); err == nil {
						t.Fatalf("accepted truncation at byte %d", length)
					}
				}
				trailing := append(bytes.Clone(frame[12:]), 0)
				if _, err := ParseRequest(makeRequestHeader(request.Key(), request.GetVersion(), trailing)); err == nil {
					t.Fatal("accepted trailing bytes")
				}
			}
		})
	}
}

func TestRequestFrameBoundaries(t *testing.T) {
	for _, size := range []int32{-1, 0, 7, maxRequestSize + 1} {
		server, client := net.Pipe()
		go func() { defer client.Close(); binary.Write(client, binary.BigEndian, size) }()
		if err := NewRequestHeader().ReadFrom(server); err == nil {
			t.Fatalf("accepted frame length %d", size)
		}
		server.Close()
	}
	request := kmsg.NewPtrApiVersionsRequest()
	request.Version = 0
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, request, 42)
	server, client := net.Pipe()
	defer server.Close()
	go func() { defer client.Close(); client.Write(append(bytes.Clone(frame), frame...)) }()
	for i := 0; i < 2; i++ {
		h := NewRequestHeader()
		if err := h.ReadFrom(server); err != nil {
			t.Fatal(err)
		}
		if _, err := ParseRequest(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := NewRequestHeader().ReadFrom(server); err == nil {
		t.Fatal("expected EOF after two frames")
	}
}

func TestInvalidArrayCountsAndNullStrings(t *testing.T) {
	for _, count := range []int32{-1, math.MaxInt32} {
		e := NewEncoder()
		e.WriteNullableString(nil)
		e.WriteString("group")
		e.WriteInt32(1000)
		e.WriteString("")
		e.WriteString("consumer")
		e.WriteInt32(count)
		if _, err := ParseJoinGroupRequest(makeRequestHeader(APIKeyJoinGroup, 0, e.Bytes())); err == nil {
			t.Fatal("invalid array count accepted")
		}
	}
	if _, err := NewDecoder([]byte{0xff, 0xff}).ReadString(); err == nil {
		t.Fatal("null non-nullable string accepted")
	}
	if _, err := NewDecoder([]byte{0}).ReadCompactString(); err == nil {
		t.Fatal("null compact non-nullable string accepted")
	}
	if _, err := NewDecoder([]byte{0xff, 0xfe}).ReadNullableString(); err == nil {
		t.Fatal("invalid nullable string length accepted")
	}
}

func FuzzRequestDispatch(f *testing.F) {
	for _, api := range SupportedAPIs() {
		e := NewEncoder()
		e.WriteNullableString(nil)
		if FlexibleRequest(api.Key, api.MaxVersion) {
			e.WriteTagBuffer()
		}
		f.Add(api.Key, api.MaxVersion, e.Bytes())
	}
	f.Fuzz(func(t *testing.T, key, version int16, body []byte) {
		if len(body) > 1<<20 {
			return
		}
		_, _ = ParseRequest(makeRequestHeader(key, version, body))
	})
}
