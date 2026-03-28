package fetch

import (
	"time"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

type partResult struct {
	index   int32
	errCode int16
	hwm     int64
	records []byte
}

type topicResult struct {
	id           [16]byte
	unknownTopic bool
	parts        []partResult
}

// collectData reads record batches for all requested partitions, respecting maxBytes.
// Returns the collected results and total number of record bytes gathered.
func collectData(req *protocol.FetchRequest, metaMgr *metadata.Manager, maxBytes int) ([]topicResult, int) {
	totalBytes := 0
	bytesWritten := 0
	results := make([]topicResult, 0, len(req.Topics))

	for _, reqTopic := range req.Topics {
		tr := topicResult{id: reqTopic.TopicID}
		topic := metaMgr.GetTopicByID(reqTopic.TopicID)

		if topic == nil {
			tr.unknownTopic = true
			tr.parts = []partResult{{index: 0, errCode: protocol.ErrUnknownTopicID, hwm: -1}}
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
				} else {
					pr.hwm = partition.NextOffset
					pr.errCode = protocol.ErrNone
					if reqPart.FetchOffset < partition.NextOffset && bytesWritten < maxBytes {
						bytePos, _ := partition.SeekToOffset(reqPart.FetchOffset)
						records, _ := metadata.ReadPartitionLogFrom(partition, bytePos)
						remaining := maxBytes - bytesWritten
						if len(records) > remaining {
							records = records[:remaining]
						}
						pr.records = records
						bytesWritten += len(records)
						totalBytes += len(records)
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
func BuildBody(req *protocol.FetchRequest, metaMgr *metadata.Manager) []byte {
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1<<31 - 1
	}
	minBytes := int(req.MinBytes)
	if minBytes <= 0 {
		minBytes = 1
	}
	deadline := time.Now().Add(time.Duration(req.MaxWaitMs) * time.Millisecond)

	// Poll until we have at least minBytes of data or the deadline is exceeded.
	var topics []topicResult
	var totalBytes int
	for {
		topics, totalBytes = collectData(req, metaMgr, maxBytes)
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
