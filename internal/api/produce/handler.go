package produce

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/codecrafters-io/kafka-starter-go/internal/metadata"
	"github.com/codecrafters-io/kafka-starter-go/internal/protocol"
)

// BuildBody builds a Produce v11 response
func BuildBody(req *protocol.ProduceRequest, metaMgr *metadata.Manager) []byte {
	encoder := protocol.NewEncoder()

	// responses (COMPACT_ARRAY) - comes FIRST in Produce v11
	topicCount := len(req.Topics)
	encoder.WriteUnsignedVarint(uint64(topicCount + 1))

	// Write each topic response
	for _, reqTopic := range req.Topics {
		// name (COMPACT_STRING) - echo back the topic name
		encoder.WriteCompactString(reqTopic.Name)

		// partitions (COMPACT_ARRAY)
		partitionCount := len(reqTopic.Partitions)
		encoder.WriteUnsignedVarint(uint64(partitionCount + 1))

		// Look up the topic in metadata
		topic := metaMgr.GetTopic(reqTopic.Name)

		// Write each partition response
		for _, reqPartition := range reqTopic.Partitions {
			// index (INT32) - echo back the partition index
			encoder.WriteInt32(reqPartition.Index)

			// Validate topic and partition exist
			var errorCode int16 = protocol.ErrUnknownTopicOrPartition
			var baseOffset int64 = -1
			var logAppendTime int64 = -1
			var logStartOffset int64 = -1

			if topic != nil {
				// Topic exists, check if partition exists
				partitionExists := false
				var partitionLogDir string
				for _, partition := range topic.Partitions {
					if partition.Index == reqPartition.Index {
						partitionExists = true
						partitionLogDir = partition.LogDir
						break
					}
				}

				if partitionExists {
					// Valid topic and partition - write record to disk
					if len(reqPartition.Records) > 0 {
						err := writeRecordsToDisk(partitionLogDir, reqPartition.Records)
						if err != nil {
							fmt.Printf("Error writing records to disk: %v\n", err)
							errorCode = protocol.ErrUnknownTopicOrPartition
						} else {
							// Success
							errorCode = protocol.ErrNone
							baseOffset = 0
							logAppendTime = -1
							logStartOffset = 0
						}
					} else {
						// No records to write - still success
						errorCode = protocol.ErrNone
						baseOffset = 0
						logAppendTime = -1
						logStartOffset = 0
					}
				}
			}

			// error_code (INT16)
			encoder.WriteInt16(errorCode)

			// base_offset (INT64)
			encoder.WriteInt64(baseOffset)

			// log_append_time_ms (INT64)
			encoder.WriteInt64(logAppendTime)

			// log_start_offset (INT64)
			encoder.WriteInt64(logStartOffset)

			// record_errors (COMPACT_ARRAY): null
			encoder.WriteByte(0x00)

			// error_message (COMPACT_NULLABLE_STRING): null
			encoder.WriteByte(0x00)

			// TAG_BUFFER for partition
			encoder.WriteTagBuffer()
		}

		// TAG_BUFFER for topic
		encoder.WriteTagBuffer()
	}

	// throttle_time_ms (INT32): 0 - comes AFTER responses in Produce v11
	encoder.WriteInt32(0)

	// TAG_BUFFER for response body
	encoder.WriteTagBuffer()

	return encoder.Bytes()
}

// writeRecordsToDisk writes record batch data to the partition's log file
func writeRecordsToDisk(logDir string, records []byte) error {
	// Ensure the log directory exists
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Write to the log file (00000000000000000000.log)
	logFilePath := filepath.Join(logDir, "00000000000000000000.log")

	// Open file for appending (create if not exists)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Write the record batch data
	_, err = file.Write(records)
	if err != nil {
		return fmt.Errorf("failed to write records: %w", err)
	}

	return nil
}
