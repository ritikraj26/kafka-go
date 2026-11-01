package network

import (
	"context"
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/coordinator"
	"github.com/ritiraj/kafka-go/internal/testutil"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIndependentGroupQuotaErrorAndEmptyGroupReuse(t *testing.T) {
	m := testutil.NewMetadataManager(t)
	c := coordinator.NewCoordinator()
	limits := coordinator.DefaultStateLimits()
	limits.MaxGroups = 1
	if err := c.SetStateLimits(limits); err != nil {
		t.Fatal(err)
	}
	listener := admissionListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := Serve(ctx, listener, m, c, 1); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		c.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})
	conn := admissionClient(t, listener)
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group, req.ProtocolType, req.SessionTimeoutMillis = "first", "consumer", 30000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
	first := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, req, first, 0)
	if first.ErrorCode != 0 {
		t.Fatal(first.ErrorCode)
	}
	req.Group = "second"
	second := kmsg.NewPtrJoinGroupResponse()
	wireInterop(t, conn, req, second, 0)
	if second.ErrorCode != 89 {
		t.Fatalf("expected THROTTLING_QUOTA_EXCEEDED, got %d", second.ErrorCode)
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "first", first.MemberID
	left := kmsg.NewPtrLeaveGroupResponse()
	wireInterop(t, conn, leave, left, 0)
	wireInterop(t, conn, req, second, 0)
	if second.ErrorCode != 0 || c.StateStats().Groups != 1 {
		t.Fatal("empty membership quota was not reusable")
	}
}
