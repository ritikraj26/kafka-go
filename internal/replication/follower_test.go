package replication

import (
	"testing"
	"time"

	"github.com/ritiraj/kafka-go/internal/metadata"
)

func TestFollowerManager_EncodeFetchV0(t *testing.T) {
	metaMgr := metadata.NewManager()
	topic := metaMgr.CreateTopic("test-topic", 1)
	p := &topic.Partitions[0]
	p.LogDir = t.TempDir()

	fm := NewFollowerManager(metaMgr, 2, 100*time.Millisecond)

	parts := []partInfo{
		{topic: topic, partition: p, leo: 0},
	}

	data := fm.encodeFetchV0(1, parts)
	// Should produce a valid byte frame: 4-byte message_size header + body
	if len(data) < 4 {
		t.Fatalf("encoded data too short: %d bytes", len(data))
	}

	// message_size should equal len(data) - 4
	msgSize := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	if msgSize != len(data)-4 {
		t.Errorf("message_size = %d, want %d", msgSize, len(data)-4)
	}
}

func TestFollowerManager_ReplicateAllSkipsLeader(t *testing.T) {
	metaMgr := metadata.NewManager()
	topic := metaMgr.CreateTopic("test-topic", 1)
	p := &topic.Partitions[0]
	p.LeaderID = 1 // This broker is the leader
	p.LogDir = t.TempDir()

	// FollowerManager with localID=1 (same as leader) should skip this partition
	fm := NewFollowerManager(metaMgr, 1, 100*time.Millisecond)

	// replicateAll should not panic or attempt connections
	fm.replicateAll()
	// If we get here without panic, the test passes
}
