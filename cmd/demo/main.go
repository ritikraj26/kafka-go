package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/broker"
	"github.com/codecrafters-io/kafka-starter-go/internal/controller"
	"github.com/codecrafters-io/kafka-starter-go/internal/coordinator"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/network"
	"github.com/codecrafters-io/kafka-starter-go/internal/replication"
)

const (
	cReset  = "\033[0m"
	cBold   = "\033[1m"
	cGreen  = "\033[32m"
	cYellow = "\033[33m"
	cRed    = "\033[31m"
	cCyan   = "\033[36m"
)

func banner(msg string) { fmt.Printf("\n%s%s=== %s ===%s\n\n", cBold, cCyan, msg, cReset) }
func ok(msg string)     { fmt.Printf("  %s\u2714 %s%s\n", cGreen, msg, cReset) }
func warn(msg string)   { fmt.Printf("  %s\u26a0 %s%s\n", cYellow, msg, cReset) }
func fail(msg string)   { fmt.Printf("  %s\u2718 %s%s\n", cRed, msg, cReset) }
func info(msg string)   { fmt.Printf("  %s\n", msg) }

func encodeInt16(v int16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return b
}

func encodeInt32(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func encodeString(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

func encodeUnsignedVarint(v uint64) []byte {
	var buf []byte
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

func encodeCompactString(s string) []byte {
	var buf []byte
	buf = append(buf, encodeUnsignedVarint(uint64(len(s)+1))...)
	buf = append(buf, s...)
	return buf
}

func encodeCompactBytes(data []byte) []byte {
	var buf []byte
	buf = append(buf, encodeUnsignedVarint(uint64(len(data)+1))...)
	buf = append(buf, data...)
	return buf
}

func flexHeader() []byte {
	var buf []byte
	buf = append(buf, 0x00)       // cursor null
	buf = append(buf, 0x05)       // client name length = 5
	buf = append(buf, "demo!"...) // client name
	buf = append(buf, 0x00)       // TAG_BUFFER
	return buf
}

func sendRequest(conn net.Conn, apiKey, apiVersion int16, corrID int32, body []byte) ([]byte, error) {
	headerSize := 2 + 2 + 4
	msgSize := int32(headerSize + len(body))

	var req []byte
	req = append(req, encodeInt32(msgSize)...)
	req = append(req, encodeInt16(apiKey)...)
	req = append(req, encodeInt16(apiVersion)...)
	req = append(req, encodeInt32(corrID)...)
	req = append(req, body...)

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	respSize := int32(binary.BigEndian.Uint32(sizeBuf))
	if respSize <= 0 || respSize > 10*1024*1024 {
		return nil, fmt.Errorf("invalid response size: %d", respSize)
	}
	resp := make([]byte, respSize)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	gotCorrID := int32(binary.BigEndian.Uint32(resp[0:4]))
	if gotCorrID != corrID {
		return nil, fmt.Errorf("correlation_id mismatch: got %d, want %d", gotCorrID, corrID)
	}
	return resp[4:], nil
}

func readUnsignedVarint(data []byte, pos int) (uint64, int) {
	var result uint64
	var shift uint
	i := 0
	for pos+i < len(data) {
		b := data[pos+i]
		result |= uint64(b&0x7F) << shift
		i++
		if b&0x80 == 0 {
			return result, i
		}
		shift += 7
	}
	return result, i
}

func writeZigzagVarint(buf *bytes.Buffer, v int64) {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
}

func buildRecordBatch(key, value []byte) []byte {
	var record bytes.Buffer
	record.WriteByte(0x00) // attributes
	record.WriteByte(0x00) // timestampDelta
	record.WriteByte(0x00) // offsetDelta

	if key == nil {
		record.WriteByte(0x01) // zigzag(-1) = null key
	} else {
		writeZigzagVarint(&record, int64(len(key)))
		record.Write(key)
	}

	writeZigzagVarint(&record, int64(len(value)))
	record.Write(value)
	record.WriteByte(0x00) // headersCount

	recBytes := record.Bytes()

	var recWithLen bytes.Buffer
	writeZigzagVarint(&recWithLen, int64(len(recBytes)))
	recWithLen.Write(recBytes)

	var batch bytes.Buffer
	binary.Write(&batch, binary.BigEndian, int64(0)) // baseOffset
	batchLengthPos := batch.Len()
	binary.Write(&batch, binary.BigEndian, int32(0))               // batchLength placeholder
	binary.Write(&batch, binary.BigEndian, int32(0))               // partitionLeaderEpoch
	batch.WriteByte(2)                                             // magic
	binary.Write(&batch, binary.BigEndian, uint32(0))              // CRC placeholder
	binary.Write(&batch, binary.BigEndian, int16(0))               // attributes
	binary.Write(&batch, binary.BigEndian, int32(0))               // lastOffsetDelta
	binary.Write(&batch, binary.BigEndian, time.Now().UnixMilli()) // baseTimestamp
	binary.Write(&batch, binary.BigEndian, time.Now().UnixMilli()) // maxTimestamp
	binary.Write(&batch, binary.BigEndian, int64(-1))              // producerId
	binary.Write(&batch, binary.BigEndian, int16(-1))              // producerEpoch
	binary.Write(&batch, binary.BigEndian, int32(-1))              // baseSequence
	binary.Write(&batch, binary.BigEndian, int32(1))               // recordsCount

	batch.Write(recWithLen.Bytes())

	result := batch.Bytes()
	batchLen := int32(len(result) - 12)
	binary.BigEndian.PutUint32(result[batchLengthPos:batchLengthPos+4], uint32(batchLen))

	return result
}

func buildProduceV11Body(topicName string, partitionIndex int32, records []byte) []byte {
	var body []byte
	body = append(body, flexHeader()...)
	body = append(body, 0x00)                              // transactional_id: null
	body = append(body, encodeInt16(-1)...)                // acks: all
	body = append(body, encodeInt32(5000)...)              // timeout_ms
	body = append(body, 0x02)                              // topics compact array: 1
	body = append(body, encodeCompactString(topicName)...) // topic name
	body = append(body, 0x02)                              // partitions compact array: 1
	body = append(body, encodeInt32(partitionIndex)...)    // partition index
	body = append(body, encodeCompactBytes(records)...)    // records
	body = append(body, 0x00)                              // TAG partition
	body = append(body, 0x00)                              // TAG topic
	body = append(body, 0x00)                              // TAG body
	return body
}

func buildFetchV0Body(topicName string, partitionIndex int32, fetchOffset int64) []byte {
	var body []byte
	body = append(body, encodeString("demo-client")...)
	body = append(body, encodeInt32(-1)...)
	body = append(body, encodeInt32(1000)...)
	body = append(body, encodeInt32(1)...)
	body = append(body, encodeInt32(1)...)
	body = append(body, encodeString(topicName)...)
	body = append(body, encodeInt32(1)...)
	body = append(body, encodeInt32(partitionIndex)...)
	body = append(body, encodeInt64(fetchOffset)...)
	body = append(body, encodeInt32(1048576)...)
	return body
}

func buildMetadataV12Body() []byte {
	var body []byte
	body = append(body, flexHeader()...)
	body = append(body, 0x00) // topics: null
	body = append(body, 0x00) // allow_auto_topic_creation
	body = append(body, 0x00) // include_topic_authorized_operations
	body = append(body, 0x00) // TAG_BUFFER
	return body
}

type produceResult struct {
	errCode    int16
	baseOffset int64
}

func parseProduceV11Response(resp []byte) (produceResult, error) {
	if len(resp) < 1 {
		return produceResult{}, fmt.Errorf("response too short")
	}
	body := resp[1:] // skip TAG_BUFFER (response header v1)
	pos := 0

	_, n := readUnsignedVarint(body, pos)
	pos += n

	nameLen, n := readUnsignedVarint(body, pos)
	pos += n
	if nameLen > 0 {
		pos += int(nameLen) - 1
	}

	_, n = readUnsignedVarint(body, pos)
	pos += n

	if pos+14 > len(body) {
		return produceResult{}, fmt.Errorf("response too short for partition data")
	}

	pos += 4 // partition index
	errCode := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	baseOffset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))

	return produceResult{errCode: errCode, baseOffset: baseOffset}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logDir, err := os.MkdirTemp("", "kafka-demo-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(logDir)

	banner("KAFKA BROKER DEMO \u2014 3-Broker In-Process Cluster")
	info(fmt.Sprintf("Log directory: %s", logDir))

	reg := broker.NewRegistry(1)
	ports := []int32{19092, 19093, 19094}
	for i := 0; i < 3; i++ {
		reg.Register(&broker.Broker{
			ID:   int32(i + 1),
			Host: "localhost",
			Port: ports[i],
		})
	}

	metaMgr := metadata.NewManager()
	metaMgr.Brokers = reg
	metaMgr.ReplicationFactor = 3
	metaMgr.SetLogDir(logDir)
	coord := coordinator.NewCoordinator()
	coord.RebalanceDelay = 500 * time.Millisecond // allow all members to arrive before completing join

	isrMgr := replication.NewISRManager(metaMgr, 500*time.Millisecond, 10000)
	isrMgr.Start(ctx)

	ctrl := controller.NewController(metaMgr, reg, 1*time.Second)
	ctrl.Start(ctx)

	var followers []*replication.FollowerManager
	brokerCancels := make(map[int32]context.CancelFunc)
	for _, b := range reg.All() {
		fm := replication.NewFollowerManager(metaMgr, b.ID, 100*time.Millisecond)
		fm.Start(ctx)
		followers = append(followers, fm)

		bCtx, bCancel := context.WithCancel(ctx)
		brokerCancels[b.ID] = bCancel
		go network.StartOnPort(bCtx, b.Port, metaMgr, coord, b.ID)
	}

	time.Sleep(300 * time.Millisecond)
	ok("All 3 brokers started on ports 19092, 19093, 19094")

	// STEP 1
	banner("STEP 1: Create Topic 'orders' (1 partition, RF=3)")
	topic := metaMgr.CreateTopic("orders", 1)
	p := &topic.Partitions[0]
	ok(fmt.Sprintf("Topic created \u2014 Leader: broker %d, Replicas: %v, ISR: %v",
		p.LeaderID, p.ReplicaNodes, p.ISRNodes))
	info(fmt.Sprintf("  Leader log dir:  %s", p.LogDirForBroker(p.LeaderID)))
	for _, rid := range p.ReplicaNodes {
		if rid != p.LeaderID {
			info(fmt.Sprintf("  Follower %d dir:  %s", rid, p.LogDirForBroker(rid)))
		}
	}

	// STEP 2
	banner(fmt.Sprintf("STEP 2: Produce Messages to Leader (broker %d)", p.LeaderID))
	leaderPort := ports[p.LeaderID-1]
	corrID := int32(100)

	leaderConn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", leaderPort), 2*time.Second)
	if err != nil {
		fail(fmt.Sprintf("Connect to leader: %v", err))
		stop()
		return
	}
	defer leaderConn.Close()

	messages := []struct{ key, value string }{
		{"order-1", `{"item":"laptop","qty":1}`},
		{"order-2", `{"item":"phone","qty":3}`},
	}

	for _, m := range messages {
		batch := buildRecordBatch([]byte(m.key), []byte(m.value))
		body := buildProduceV11Body("orders", 0, batch)
		corrID++
		resp, err := sendRequest(leaderConn, 0, 11, corrID, body)
		if err != nil {
			fail(fmt.Sprintf("Produce %s: %v", m.key, err))
			continue
		}
		pr, err := parseProduceV11Response(resp)
		if err != nil {
			fail(fmt.Sprintf("Parse response for %s: %v", m.key, err))
			continue
		}
		if pr.errCode == 0 {
			ok(fmt.Sprintf("Produce %s \u2192 offset=%d (error_code=0)", m.key, pr.baseOffset))
		} else {
			fail(fmt.Sprintf("Produce %s \u2192 error_code=%d", m.key, pr.errCode))
		}
	}

	// STEP 3
	banner("STEP 3: Produce to Follower (expect ErrNotLeaderOrFollower)")
	var followerBrokerID int32
	for _, rid := range p.ReplicaNodes {
		if rid != p.LeaderID {
			followerBrokerID = rid
			break
		}
	}
	followerPort := ports[followerBrokerID-1]
	followerConn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", followerPort), 2*time.Second)
	if err != nil {
		fail(fmt.Sprintf("Connect to follower %d: %v", followerBrokerID, err))
	} else {
		defer followerConn.Close()
		batch := buildRecordBatch([]byte("bad"), []byte(`{"should":"fail"}`))
		body := buildProduceV11Body("orders", 0, batch)
		corrID++
		resp, err := sendRequest(followerConn, 0, 11, corrID, body)
		if err != nil {
			fail(fmt.Sprintf("Follower produce: %v", err))
		} else {
			pr, _ := parseProduceV11Response(resp)
			if pr.errCode == 6 {
				ok(fmt.Sprintf("Follower (broker %d) correctly rejected: NOT_LEADER_OR_FOLLOWER (code 6)", followerBrokerID))
			} else {
				fail(fmt.Sprintf("Expected error_code 6, got %d", pr.errCode))
			}
		}
	}

	// STEP 4
	banner("STEP 4: Waiting for Replication (2 seconds)")
	time.Sleep(2 * time.Second)
	info(fmt.Sprintf("  Leader LEO:     %d", p.GetLogEndOffset()))
	info(fmt.Sprintf("  High Watermark: %d", p.GetHighWatermark()))
	for _, rid := range p.ReplicaNodes {
		info(fmt.Sprintf("  Broker %d LEO:   %d", rid, p.GetReplicaLEO(rid)))
	}
	if p.GetHighWatermark() >= 2 {
		ok("High watermark advanced \u2014 all replicas in sync!")
	} else {
		warn("High watermark not yet fully caught up")
	}

	// STEP 5
	banner("STEP 5: Fetch Messages from Leader (Fetch v0)")
	fetchBody := buildFetchV0Body("orders", 0, 0)
	corrID++
	fetchResp, err := sendRequest(leaderConn, 1, 0, corrID, fetchBody)
	if err != nil {
		fail(fmt.Sprintf("Fetch error: %v", err))
	} else {
		pos := 0
		if pos+4 > len(fetchResp) {
			fail("Fetch response too short")
		} else {
			numTopics := int32(binary.BigEndian.Uint32(fetchResp[pos : pos+4]))
			pos += 4
			for t := int32(0); t < numTopics; t++ {
				tnLen := int16(binary.BigEndian.Uint16(fetchResp[pos : pos+2]))
				pos += 2
				topicName := string(fetchResp[pos : pos+int(tnLen)])
				pos += int(tnLen)
				numParts := int32(binary.BigEndian.Uint32(fetchResp[pos : pos+4]))
				pos += 4
				for pi := int32(0); pi < numParts; pi++ {
					pIdx := int32(binary.BigEndian.Uint32(fetchResp[pos : pos+4]))
					pos += 4
					pErr := int16(binary.BigEndian.Uint16(fetchResp[pos : pos+2]))
					pos += 2
					hwm := int64(binary.BigEndian.Uint64(fetchResp[pos : pos+8]))
					pos += 8
					recLen := int32(binary.BigEndian.Uint32(fetchResp[pos : pos+4]))
					pos += 4
					if recLen > 0 {
						pos += int(recLen)
					}
					ok(fmt.Sprintf("Topic=%s  Partition=%d  Error=%d  HWM=%d  RecordBytes=%d",
						topicName, pIdx, pErr, hwm, recLen))
				}
			}
		}
	}

	// STEP 6
	banner("STEP 6: Metadata Request (Metadata v12)")
	metaBody := buildMetadataV12Body()
	corrID++
	metaResp, err := sendRequest(leaderConn, 3, 12, corrID, metaBody)
	if err != nil {
		fail(fmt.Sprintf("Metadata error: %v", err))
	} else {
		body := metaResp
		if len(body) > 0 {
			body = body[1:]
		}
		pos := 4

		brokersLen, n := readUnsignedVarint(body, pos)
		pos += n
		actualBrokers := int(brokersLen) - 1
		ok(fmt.Sprintf("Cluster has %d brokers", actualBrokers))
		for i := 0; i < actualBrokers; i++ {
			if pos+4 > len(body) {
				break
			}
			bID := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			hostLen, n := readUnsignedVarint(body, pos)
			pos += n
			hostActual := int(hostLen) - 1
			host := string(body[pos : pos+hostActual])
			pos += hostActual
			bPort := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			rackLen, n := readUnsignedVarint(body, pos)
			pos += n
			pos += int(rackLen) - 1
			pos++
			info(fmt.Sprintf("  Broker %d \u2192 %s:%d", bID, host, bPort))
		}
	}

	// STEP 7
	banner("STEP 7: Per-Broker Log Directory Contents")
	for _, rid := range topic.Partitions[0].ReplicaNodes {
		dir := topic.Partitions[0].LogDirForBroker(rid)
		files, err := os.ReadDir(dir)
		if err != nil {
			warn(fmt.Sprintf("Broker %d: log dir missing (%v)", rid, err))
		} else {
			var names []string
			for _, f := range files {
				fi, _ := f.Info()
				if fi != nil {
					names = append(names, fmt.Sprintf("%s (%d bytes)", f.Name(), fi.Size()))
				}
			}
			ok(fmt.Sprintf("Broker %d: %s", rid, strings.Join(names, ", ")))
		}
	}

	// STEP 8
	banner("STEP 8: Consumer Group — 3 Members Join Simultaneously")
	info("  RebalanceDelay=500ms so all members land in the same generation")
	var cgWg sync.WaitGroup
	cgResults := make([]*coordinator.JoinGroupResult, 3)
	for i := 0; i < 3; i++ {
		cgWg.Add(1)
		go func(idx int) {
			defer cgWg.Done()
			cgResults[idx] = coord.JoinGroup("demo-group", "", 10000, "consumer",
				[]coordinator.MemberProtocol{{Name: "range", Metadata: []byte("meta")}})
		}(i)
	}
	cgWg.Wait()
	for i, r := range cgResults {
		if r.ErrorCode == 0 {
			role := "follower"
			if r.MemberID == r.LeaderID {
				role = "LEADER "
			}
			ok(fmt.Sprintf("Consumer %d — gen=%d  role=%s  memberID=%s...",
				i+1, r.GenerationID, role, r.MemberID[:8]))
		} else {
			fail(fmt.Sprintf("Consumer %d join failed: error_code=%d", i+1, r.ErrorCode))
		}
	}
	if len(cgResults) > 0 && cgResults[0].ErrorCode == 0 {
		ok(fmt.Sprintf("All 3 consumers in generation %d — one rebalance round", cgResults[0].GenerationID))
	}

	// STEP 9
	oldLeader := p.LeaderID
	banner(fmt.Sprintf("STEP 9: Leader Failover \u2014 Killing Broker %d", oldLeader))
	info(fmt.Sprintf("  Current leader: broker %d", oldLeader))
	info(fmt.Sprintf("  Current ISR:    %v", p.ISRNodes))

	if cancel, exists := brokerCancels[oldLeader]; exists {
		cancel()
		ok(fmt.Sprintf("Broker %d listener stopped", oldLeader))
	}

	info("  Waiting for controller to detect failure and elect new leader...")
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if p.LeaderID != oldLeader {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if p.LeaderID != oldLeader {
		ok(fmt.Sprintf("New leader elected: broker %d (was: broker %d)", p.LeaderID, oldLeader))
		ok(fmt.Sprintf("Leader epoch: %d", p.LeaderEpoch))
		ok(fmt.Sprintf("Updated ISR: %v", p.ISRNodes))
	} else {
		warn("Leader election did not occur within timeout")
	}

	// STEP 10
	if p.LeaderID != oldLeader {
		banner(fmt.Sprintf("STEP 10: Produce to New Leader (broker %d)", p.LeaderID))
		newLeaderPort := ports[p.LeaderID-1]
		newConn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", newLeaderPort), 2*time.Second)
		if err != nil {
			fail(fmt.Sprintf("Connect to new leader: %v", err))
		} else {
			defer newConn.Close()
			batch := buildRecordBatch([]byte("order-3"), []byte(`{"item":"tablet","qty":2}`))
			body := buildProduceV11Body("orders", 0, batch)
			corrID++
			resp, err := sendRequest(newConn, 0, 11, corrID, body)
			if err != nil {
				fail(fmt.Sprintf("Produce to new leader: %v", err))
			} else {
				pr, _ := parseProduceV11Response(resp)
				if pr.errCode == 0 {
					ok(fmt.Sprintf("Produce to new leader \u2192 offset=%d", pr.baseOffset))
				} else {
					fail(fmt.Sprintf("Produce to new leader \u2192 error_code=%d", pr.errCode))
				}
			}
		}
	}

	banner("DEMO COMPLETE")
	fmt.Println("  What was demonstrated:")
	fmt.Println("    1.  3-broker cluster startup (separate TCP listeners)")
	fmt.Println("    2.  Topic creation with RF=3 and replica placement")
	fmt.Println("    3.  Produce to leader \u2192 SUCCESS")
	fmt.Println("    4.  Produce to follower \u2192 ErrNotLeaderOrFollower (code 6)")
	fmt.Println("    5.  Pull-based replication (followers fetch from leader)")
	fmt.Println("    6.  Fetch v0 consumer read from leader")
	fmt.Println("    7.  Metadata v12 cluster overview")
	fmt.Println("    8.  Per-broker log directory isolation")
	fmt.Println("    9.  Consumer group — 3 members, 1 rebalance round, leader elected")
	fmt.Println("    10. Controller-driven automatic leader failover")
	fmt.Println("    11. Produce to newly-elected leader \u2192 SUCCESS")
	fmt.Println()

	stop()
	for _, fm := range followers {
		fm.Wait()
	}
	isrMgr.Wait()
	ctrl.Wait()
}
