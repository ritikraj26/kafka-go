package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/metadata"
	"github.com/ritiraj/kafka-go/internal/protocol"
)

// FollowerManager replicates data from leader partitions via real TCP Fetch v0 requests.
// For each partition where the local broker is a follower, it periodically sends a Fetch
// request to the leader and writes the returned records to the follower's local log directory.
type FollowerManager struct {
	metaMgr  *metadata.Manager
	localID  int32
	interval time.Duration
	wg       sync.WaitGroup

	// Per-leader persistent TCP connections (leader broker ID → conn)
	mu    sync.Mutex
	conns map[int32]net.Conn

	correlationSeq int32
}

// partInfo groups a topic/partition with its current LEO for fetch batching.
type partInfo struct {
	topic     *metadata.Topic
	partition *metadata.Partition
	leo       int64
}

// NewFollowerManager creates a new follower manager.
func NewFollowerManager(metaMgr *metadata.Manager, localID int32, interval time.Duration) *FollowerManager {
	return &FollowerManager{
		metaMgr:  metaMgr,
		localID:  localID,
		interval: interval,
		conns:    make(map[int32]net.Conn),
	}
}

// Start begins the background replication loop. Call with a cancellable context.
func (fm *FollowerManager) Start(ctx context.Context) {
	fm.wg.Add(1)
	go func() {
		defer fm.wg.Done()
		ticker := time.NewTicker(fm.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				fm.closeAll()
				return
			case <-ticker.C:
				fm.replicateAll()
			}
		}
	}()
}

// Wait blocks until the replication loop exits.
func (fm *FollowerManager) Wait() {
	fm.wg.Wait()
}

// closeAll closes all persistent connections.
func (fm *FollowerManager) closeAll() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	for id, c := range fm.conns {
		c.Close()
		delete(fm.conns, id)
	}
}

// getConn returns a persistent connection to the given leader broker, dialing if necessary.
func (fm *FollowerManager) getConn(leaderID int32) (net.Conn, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if c, ok := fm.conns[leaderID]; ok {
		return c, nil
	}

	b := fm.metaMgr.Brokers.Get(leaderID)
	if b == nil {
		return nil, fmt.Errorf("unknown leader broker %d", leaderID)
	}
	addr := net.JoinHostPort(b.Host, fmt.Sprintf("%d", b.Port))
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	fm.conns[leaderID] = c
	return c, nil
}

// dropConn closes and removes a connection when it encounters an error.
func (fm *FollowerManager) dropConn(leaderID int32) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if c, ok := fm.conns[leaderID]; ok {
		c.Close()
		delete(fm.conns, leaderID)
	}
}

// nextCorrelationID returns a monotonically increasing correlation ID.
func (fm *FollowerManager) nextCorrelationID() int32 {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.correlationSeq++
	return fm.correlationSeq
}

// replicateAll iterates over all topics/partitions and fetches from leaders via TCP.
func (fm *FollowerManager) replicateAll() {
	leaderParts := make(map[int32][]partInfo)

	for _, topicName := range fm.metaMgr.ListTopics() {
		topic := fm.metaMgr.GetTopic(topicName)
		if topic == nil {
			continue
		}
		for i := range topic.Partitions {
			p := &topic.Partitions[i]
			if p.LeaderID == fm.localID {
				continue // we are the leader, nothing to fetch
			}
			// Check if this broker is a replica
			isReplica := false
			for _, r := range p.ReplicaNodes {
				if r == fm.localID {
					isReplica = true
					break
				}
			}
			if !isReplica {
				continue
			}
			// Use this broker's own LEO as the fetch offset (not the leader's LEO).
			// ReplicaLEO is set by the leader's fetch handler after each successful fetch.
			// Initially 0 — follower starts replicating from the beginning.
			leo := p.GetReplicaLEO(fm.localID)
			leaderParts[p.LeaderID] = append(leaderParts[p.LeaderID], partInfo{
				topic: topic, partition: p, leo: leo,
			})
		}
	}

	for leaderID, parts := range leaderParts {
		fm.fetchFromLeader(leaderID, parts)
	}
}

// fetchFromLeader sends a Fetch v0 request for all partitions led by leaderID
// and processes the response.
func (fm *FollowerManager) fetchFromLeader(leaderID int32, parts []partInfo) {
	conn, err := fm.getConn(leaderID)
	if err != nil {
		logger.L.Warn("follower: cannot connect to leader", "leader", leaderID, "err", err)
		return
	}

	corrID := fm.nextCorrelationID()
	reqBytes := fm.encodeFetchV0(corrID, parts)

	// Set write + read deadline
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := conn.Write(reqBytes); err != nil {
		logger.L.Warn("follower: write failed", "leader", leaderID, "err", err)
		fm.dropConn(leaderID)
		return
	}

	// Read response: message_size (4 bytes) + rest
	var msgSize int32
	if err := binary.Read(conn, binary.BigEndian, &msgSize); err != nil {
		logger.L.Warn("follower: read msg_size failed", "leader", leaderID, "err", err)
		fm.dropConn(leaderID)
		return
	}
	if msgSize <= 0 || msgSize > 100*1024*1024 {
		logger.L.Warn("follower: invalid msg_size", "leader", leaderID, "size", msgSize)
		fm.dropConn(leaderID)
		return
	}

	respBuf := make([]byte, msgSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		logger.L.Warn("follower: read response failed", "leader", leaderID, "err", err)
		fm.dropConn(leaderID)
		return
	}

	// Clear deadline
	conn.SetDeadline(time.Time{})

	fm.decodeFetchV0Response(respBuf, parts)
}

// encodeFetchV0 builds a Fetch v0 wire-format request.
func (fm *FollowerManager) encodeFetchV0(correlationID int32, parts []partInfo) []byte {
	enc := protocol.NewEncoder()

	// Request header: api_key, api_version, correlation_id, client_id
	enc.WriteInt16(protocol.APIKeyFetch) // api_key = 1
	enc.WriteInt16(0)                    // api_version = 0
	enc.WriteInt32(correlationID)
	enc.WriteString(fmt.Sprintf("follower-%d", fm.localID)) // client_id

	// Fetch v0 body
	enc.WriteInt32(fm.localID) // replica_id
	enc.WriteInt32(500)        // max_wait_ms
	enc.WriteInt32(1)          // min_bytes

	// Group by topic
	type topicGroup struct {
		name  string
		parts []struct {
			index  int32
			offset int64
		}
	}
	topicMap := make(map[string]*topicGroup)
	var topicOrder []string
	for _, p := range parts {
		name := p.topic.Name
		tg, ok := topicMap[name]
		if !ok {
			tg = &topicGroup{name: name}
			topicMap[name] = tg
			topicOrder = append(topicOrder, name)
		}
		tg.parts = append(tg.parts, struct {
			index  int32
			offset int64
		}{p.partition.Index, p.leo})
	}

	enc.WriteArrayLen(len(topicOrder))
	for _, name := range topicOrder {
		tg := topicMap[name]
		enc.WriteString(tg.name) // topic name (STRING)
		enc.WriteArrayLen(len(tg.parts))
		for _, pp := range tg.parts {
			enc.WriteInt32(pp.index)  // partition
			enc.WriteInt64(pp.offset) // fetch_offset
			enc.WriteInt32(1 << 20)   // max_bytes per partition (1MB)
		}
	}

	body := enc.Bytes()
	// Prepend message_size (INT32)
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(body)))
	copy(frame[4:], body)
	return frame
}

// decodeFetchV0Response parses a Fetch v0 response and writes records to follower logs.
// Response layout after message_size: correlation_id(4) | topics(ARRAY[topic_name(STRING),
//
//	partitions(ARRAY[index(INT32), error_code(INT16), hw(INT64), record_set(BYTES)])])
func (fm *FollowerManager) decodeFetchV0Response(data []byte, parts []partInfo) {
	dec := protocol.NewDecoder(data)

	// correlation_id (4 bytes)
	_, err := dec.ReadInt32()
	if err != nil {
		logger.L.Warn("follower: decode correlationID failed", "err", err)
		return
	}

	numTopics, err := dec.ReadArrayLen()
	if err != nil {
		logger.L.Warn("follower: decode numTopics failed", "err", err)
		return
	}

	for i := int32(0); i < numTopics; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			logger.L.Warn("follower: decode topicName failed", "err", err)
			return
		}

		numPartitions, err := dec.ReadArrayLen()
		if err != nil {
			logger.L.Warn("follower: decode numPartitions failed", "err", err)
			return
		}

		for j := int32(0); j < numPartitions; j++ {
			partIdx, err := dec.ReadInt32()
			if err != nil {
				return
			}
			errCode, err := dec.ReadInt16()
			if err != nil {
				return
			}
			_, err = dec.ReadInt64() // high_watermark
			if err != nil {
				return
			}
			recordSet, err := dec.ReadBytes()
			if err != nil {
				return
			}

			if errCode != int16(protocol.ErrNone) {
				logger.L.Warn("follower: leader returned error",
					"topic", topicName, "partition", partIdx, "errCode", errCode)
				continue
			}

			if len(recordSet) == 0 {
				continue
			}

			// Find the matching partition to write records
			for _, p := range parts {
				if p.topic.Name == topicName && p.partition.Index == partIdx {
					replicaLogDir := fm.metaMgr.PartitionLogDir(fm.localID, topicName, partIdx)
					if writeErr := p.partition.AppendReplicaRecords(recordSet, replicaLogDir, p.leo); writeErr != nil {
						logger.L.Error("follower: write records failed",
							"topic", topicName, "partition", partIdx, "err", writeErr)
					} else {
						logger.L.Debug("follower: replicated records",
							"topic", topicName, "partition", partIdx, "bytes", len(recordSet))
					}
					break
				}
			}
		}
	}
}
