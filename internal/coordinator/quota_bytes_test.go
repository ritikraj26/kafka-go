package coordinator

import (
	"testing"

	"github.com/ritiraj/kafka-go/internal/protocol"
)

func TestGlobalGroupByteCeilingAndReuse(t *testing.T) {
	protocols := []MemberProtocol{{Name: "range"}}
	one := groupCharge("a") + memberCharge("m", protocols, nil) + 2*int64(len("consumer"))
	limits := DefaultStateLimits()
	limits.PerGroupBytes = one * 2
	limits.GroupBytes = one * 2
	c := quotaCoordinator(t, limits)
	for _, name := range []string{"a", "b"} {
		if result := c.JoinGroup(name, "m", 30000, "consumer", protocols); result.ErrorCode != 0 {
			t.Fatal(result.ErrorCode)
		}
	}
	if c.StateStats().GroupBytes != limits.GroupBytes {
		t.Fatal("global group-byte threshold was not exact")
	}
	if result := c.JoinGroup("a", "m", 30000, "consumer", []MemberProtocol{{Name: "range", Metadata: []byte{1}}}); result.ErrorCode != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("group update exceeded the global byte ceiling")
	}
	if c.StateStats().GroupBytes != limits.GroupBytes {
		t.Fatal("rejected update changed global charges")
	}
	c.LeaveGroup("a", "m")
	if result := c.JoinGroup("c", "m", 30000, "consumer", protocols); result.ErrorCode != 0 {
		t.Fatal("released bytes were not reusable")
	}
	if c.StateStats().GroupBytes != limits.GroupBytes {
		t.Fatal("reclaimed group bytes were accounted incorrectly")
	}
}

func TestOffsetByteQuotaAndSmallerReplacement(t *testing.T) {
	note := "checkpoint"
	limits := DefaultStateLimits()
	limits.OffsetBytes = offsetCharge("g", "events", &note)
	c := quotaCoordinator(t, limits)
	member := c.JoinGroup("g", "", 30000, "consumer", []MemberProtocol{{Name: "range"}})
	commit := func(meta *string) int16 {
		return c.CommitOffsetWithMetadata("g", member.GenerationID, member.MemberID, "events", 0, 3, meta)
	}
	if code := commit(&note); code != 0 {
		t.Fatal(code)
	}
	if c.StateStats().OffsetBytes != limits.OffsetBytes {
		t.Fatal("offset-byte threshold was not exact")
	}
	larger := note + "x"
	if code := commit(&larger); code != protocol.ErrThrottlingQuotaExceeded {
		t.Fatal("offset metadata growth exceeded quota")
	}
	if code := commit(nil); code != 0 {
		t.Fatal("smaller offset replacement was rejected")
	}
	if c.StateStats().OffsetBytes != offsetCharge("g", "events", nil) {
		t.Fatal("smaller metadata did not release bytes")
	}
}
