package fetch

import (
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/logger"
	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

type partResult struct {
	index   int32
	errCode int16
	hwm     int64
	leo     int64 // LogEndOffset — only meaningful for replica fetch responses
	records []byte
}

type topicResult struct {
	id           [16]byte
	name         string // populated for v0 requests
	unknownTopic bool
	parts        []partResult
}

// resolveTopic finds the topic either by UUID (v13+) or by name (v0-v12).
func resolveTopic(reqTopic protocol.FetchTopic, metaMgr *metadata.Manager) *metadata.Topic {
	if reqTopic.TopicName != "" {
		return metaMgr.GetTopic(reqTopic.TopicName)
	}
	return metaMgr.GetTopicByID(reqTopic.TopicID)
}

// collectData reads record batches for all requested partitions, respecting maxBytes.
// When isReplicaFetch is true, data up to LogEndOffset is served (not limited to HW).
func collectData(req *protocol.FetchRequest, metaMgr *metadata.Manager, maxBytes int, localBrokerID int32) ([]topicResult, int) {
	isReplicaFetch := req.ReplicaID >= 0
	totalBytes := 0
	bytesWritten := 0
	results := make([]topicResult, 0, len(req.Topics))

	for _, reqTopic := range req.Topics {
		tr := topicResult{id: reqTopic.TopicID, name: reqTopic.TopicName}
		topic := resolveTopic(reqTopic, metaMgr)

		if topic == nil {
			tr.unknownTopic = true
			tr.parts = []partResult{{index: 0, errCode: protocol.ErrUnknownTopicID, hwm: -1, leo: -1}}
		} else {
			for _, reqPart := range reqTopic.Partitions {
				pr := partResult{index: reqPart.PartitionIndex}
				var partition *metadata.Partition
				for i := range topic.Partitions {
					if topic.Partitions[i].Index == reqPart.PartitionIndex {
						partition = &topic.Partitions[i]
						break
					}
				}
				if partition == nil {
					pr.errCode = protocol.ErrUnknownTopicOrPartition
					pr.hwm = -1
					pr.leo = -1
				} else {
					pr.hwm = partition.HighWatermark
					pr.leo = partition.LogEndOffset
					pr.errCode = protocol.ErrNone

					// LeaderEpoch validation — only when requester is tracking epochs (-1 = not tracking)
					if reqPart.CurrentLeaderEpoch >= 0 {
						if reqPart.CurrentLeaderEpoch < partition.LeaderEpoch {
							// Requester (follower or consumer) is behind — stale epoch
							pr.errCode = protocol.ErrFencedLeaderEpoch
							tr.parts = append(tr.parts, pr)
							continue
						}
						if !isReplicaFetch && reqPart.CurrentLeaderEpoch > partition.LeaderEpoch {
							// Consumer thinks a newer election happened — this broker is stale
							pr.errCode = protocol.ErrNotLeaderOrFollower
							tr.parts = append(tr.parts, pr)
							continue
						}
					}

					// Determine read limit: LEO for replica fetches, HW for consumers
					readLimit := partition.HighWatermark
					if isReplicaFetch {
						readLimit = partition.LogEndOffset
					}

					if reqPart.FetchOffset < readLimit && bytesWritten < maxBytes {
						logDir := partition.LogDirForBroker(localBrokerID)
						bytePos, seekErr := partition.SeekToOffset(reqPart.FetchOffset, logDir)
						if seekErr != nil {
							logger.L.Error("SeekToOffset failed", "partition", partition.Index, "err", seekErr)
							pr.errCode = protocol.ErrUnknownServerError
						} else {
							records, readErr := metadata.ReadPartitionLogFrom(partition, bytePos, logDir)
							if readErr != nil {
								logger.L.Error("ReadPartitionLogFrom failed", "partition", partition.Index, "err", readErr)
								pr.errCode = protocol.ErrUnknownServerError
							} else {
								remaining := maxBytes - bytesWritten
								if len(records) > remaining {
									records = records[:remaining]
								}
								pr.records = records
								bytesWritten += len(records)
								totalBytes += len(records)
							}
						}
					}
				}
				tr.parts = append(tr.parts, pr)
			}
		}
		results = append(results, tr)
	}
	return results, totalBytes
}

// BuildBody builds a Fetch v16 response, honouring MaxBytes, MinBytes, and MaxWaitMs.
// After a replica fetch, it updates ReplicaLEO/ReplicaLastSeen and advances HW.
func BuildBody(req *protocol.FetchRequest, metaMgr *metadata.Manager, localBrokerID ...int32) []byte {
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1<<31 - 1
	}
	minBytes := int(req.MinBytes)
	if minBytes <= 0 {
		minBytes = 1
	}
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)

	var brokerID int32 = 1
	if len(localBrokerID) > 0 {
		brokerID = localBrokerID[0]
	}

	// Poll until we have at least minBytes of data or the deadline is exceeded.
	var topics []topicResult
	var totalBytes int
	for {
		topics, totalBytes = collectData(req, metaMgr, maxBytes, brokerID)
		if totalBytes >= minBytes || !time.Now().Before(deadline) {
			break
		}
		remaining := time.Until(deadline)
		sleep := 5 * time.Millisecond
		if sleep > remaining {
			sleep = remaining
		}
		time.Sleep(sleep)
	}

	// For replica fetches, update the replica's tracked state on the leader side.
	if req.ReplicaID >= 0 {
		updateReplicaState(req, topics, metaMgr)
	}

	encoder := protocol.NewEncoder()
	encoder.WriteInt32(0)                                // throttle_time_ms
	encoder.WriteInt16(protocol.ErrNone)                 // error_code
	encoder.WriteInt32(req.SessionID)                    // session_id
	encoder.WriteUnsignedVarint(uint64(len(topics) + 1)) // responses COMPACT_ARRAY

	for _, tr := range topics {
		encoder.WriteUUID(tr.id)

		if tr.unknownTopic {
			encoder.WriteUnsignedVarint(2) // 1 partition + 1
			encoder.WriteInt32(0)
			encoder.WriteInt16(protocol.ErrUnknownTopicID)
			encoder.WriteInt64(-1)  // high_watermark
			encoder.WriteInt64(-1)  // last_stable_offset
			encoder.WriteInt64(-1)  // log_start_offset
			encoder.WriteByte(0x00) // aborted_transactions null
			encoder.WriteInt32(-1)  // preferred_read_replica
			encoder.WriteByte(0x00) // records null
			encoder.WriteTagBuffer()
		} else {
			encoder.WriteUnsignedVarint(uint64(len(tr.parts) + 1))
			for _, pr := range tr.parts {
				encoder.WriteInt32(pr.index)
				encoder.WriteInt16(pr.errCode)
				encoder.WriteInt64(pr.hwm) // high_watermark
				if pr.errCode != protocol.ErrNone {
					encoder.WriteInt64(-1)  // last_stable_offset
					encoder.WriteInt64(-1)  // log_start_offset
					encoder.WriteByte(0x00) // aborted_transactions null
					encoder.WriteInt32(-1)  // preferred_read_replica
					encoder.WriteByte(0x00) // records null
				} else {
					encoder.WriteInt64(pr.hwm) // last_stable_offset
					encoder.WriteInt64(0)      // log_start_offset
					encoder.WriteByte(0x00)    // aborted_transactions null
					encoder.WriteInt32(-1)     // preferred_read_replica
					if len(pr.records) == 0 {
						encoder.WriteByte(0x00)
					} else {
						encoder.WriteCompactBytes(pr.records)
					}
				}
				encoder.WriteTagBuffer()
			}
		}
		encoder.WriteTagBuffer()
	}

	encoder.WriteTagBuffer()
	return encoder.Bytes()
}

// BuildBodyV0 builds a Fetch v0 response (non-flexible, used for inter-broker replication).
// Format: [TopicName(STRING), Partitions(ARRAY[Index(INT32), ErrorCode(INT16), HW(INT64), RecordSet(BYTES)])]
func BuildBodyV0(req *protocol.FetchRequest, metaMgr *metadata.Manager, localBrokerID ...int32) []byte {
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1<<31 - 1
	}
	minBytes := int(req.MinBytes)
	if minBytes <= 0 {
		minBytes = 1
	}
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)

	var brokerID int32 = 1
	if len(localBrokerID) > 0 {
		brokerID = localBrokerID[0]
	}

	var topics []topicResult
	var totalBytes int
	for {
		topics, totalBytes = collectData(req, metaMgr, maxBytes, brokerID)
		if totalBytes >= minBytes || !time.Now().Before(deadline) {
			break
		}
		remaining := time.Until(deadline)
		sleep := 5 * time.Millisecond
		if sleep > remaining {
			sleep = remaining
		}
		time.Sleep(sleep)
	}

	if req.ReplicaID >= 0 {
		updateReplicaState(req, topics, metaMgr)
	}

	encoder := protocol.NewEncoder()
	// v0 response: ARRAY of topic responses
	encoder.WriteArrayLen(len(topics))
	for _, tr := range topics {
		encoder.WriteString(tr.name)
		encoder.WriteArrayLen(len(tr.parts))
		for _, pr := range tr.parts {
			encoder.WriteInt32(pr.index)
			encoder.WriteInt16(pr.errCode)
			encoder.WriteInt64(pr.hwm)
			encoder.WriteBytes(pr.records) // BYTES (INT32-length-prefixed)
		}
	}
	return encoder.Bytes()
}

// updateReplicaState records the replica's fetch progress on the leader and advances HW.
func updateReplicaState(req *protocol.FetchRequest, topics []topicResult, metaMgr *metadata.Manager) {
	nowMs := time.Now().UnixMilli()
	for _, tr := range topics {
		topic := resolveTopic(protocol.FetchTopic{TopicID: tr.id, TopicName: tr.name}, metaMgr)
		if topic == nil {
			continue
		}
		for _, pr := range tr.parts {
			if pr.errCode != protocol.ErrNone {
				continue
			}
			for i := range topic.Partitions {
				p := &topic.Partitions[i]
				if p.Index != pr.index {
					continue
				}
				// The replica has fetched up to pr.leo (the partition's LEO at fetch time).
				// If it sent records, we assume it will apply them and its LEO will be pr.leo.
				p.UpdateReplicaState(req.ReplicaID, pr.leo, nowMs)
				p.RunUnderLock(func() {
					p.AdvanceHighWatermark()
				})
				break
			}
		}
	}
}
