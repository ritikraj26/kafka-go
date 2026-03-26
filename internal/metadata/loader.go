package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
)

// LoadTopicsFromDisk scans the log directory and loads existing topics
func (m *Manager) LoadTopicsFromDisk(logDir string) error {
	// Check if directory exists
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return nil
	}

	// Read all entries in the log directory
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	// First pass: collect all topic names from directories
	topicNames := make(map[string]bool)
	topicsFound := make(map[string]*Topic)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName, _, err := parseTopicDirName(entry.Name())
		if err != nil {
			continue
		}
		topicNames[topicName] = true
	}

	// Read cluster metadata to get topic UUIDs for the topics we found
	clusterMetadataPath := logDir + "/__cluster_metadata-0/00000000000000000000.log"
	topicUUIDs, err := parseClusterMetadataForTopics(clusterMetadataPath, topicNames)
	if err != nil {
		fmt.Printf("Warning: Failed to parse cluster metadata: %v\n", err)
		topicUUIDs = make(map[string]uuid.UUID)
	}

	// Second pass: create topics with their UUIDs and partitions
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dirName := entry.Name()

		// Parse the directory name to extract topic name and partition
		topicName, partitionIndex, err := parseTopicDirName(dirName)
		if err != nil {
			// Skip directories that don't match the pattern
			fmt.Printf("Skipping directory %s: %v\n", dirName, err)
			continue
		}

		fmt.Printf("Parsed: topic=%s, partition=%d\n", topicName, partitionIndex)

		// Get or create topic entry
		topic, exists := topicsFound[topicName]
		if !exists {
			// Get UUID from cluster metadata, or generate new one
			topicID, hasUUID := topicUUIDs[topicName]
			if !hasUUID {
				topicID = uuid.New()
				fmt.Printf("Warning: No UUID found for topic %s, generated new one: %s\n", topicName, topicID)
			} else {
				fmt.Printf("Found UUID for topic %s: %s\n", topicName, topicID)
			}

			topic = &Topic{
				Name:       topicName,
				ID:         topicID,
				IsInternal: false,
				Partitions: []Partition{},
			}
			topicsFound[topicName] = topic
		}

		// Add partition (ensure partitions array is large enough)
		for len(topic.Partitions) <= partitionIndex {
			partIdx := int32(len(topic.Partitions))
			partLogDir := fmt.Sprintf("%s/%s-%d", logDir, topicName, partIdx)
			nextOffset := recoverNextOffset(partLogDir)
			topic.Partitions = append(topic.Partitions, Partition{
				Index:           partIdx,
				LeaderID:        1,
				ReplicaNodes:    []int32{1},
				ISRNodes:        []int32{1},
				OfflineReplicas: []int32{},
				LeaderEpoch:     0,
				PartitionEpoch:  0,
				LogDir:          partLogDir,
				NextOffset:      nextOffset,
			})
		}
	}

	// Add all loaded topics to the manager
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, topic := range topicsFound {
		m.topics[topic.Name] = topic
	}

	return nil
}

// parseTopicDirName parses a topic directory name
// Format: topicname-partition
// Returns: topicName, partitionIndex, error
func parseTopicDirName(dirName string) (string, int, error) {
	// Split by the last hyphen to get partition index
	lastHyphen := strings.LastIndex(dirName, "-")
	if lastHyphen == -1 {
		return "", 0, fmt.Errorf("no hyphen found in directory name")
	}

	topicName := dirName[:lastHyphen]
	partitionStr := dirName[lastHyphen+1:]

	// Parse partition index
	var partitionIndex int
	_, err := fmt.Sscanf(partitionStr, "%d", &partitionIndex)
	if err != nil {
		return "", 0, fmt.Errorf("invalid partition index: %w", err)
	}

	if topicName == "" {
		return "", 0, fmt.Errorf("empty topic name")
	}

	// Skip internal topics
	if strings.HasPrefix(topicName, "__") {
		return "", 0, fmt.Errorf("internal topic: %s", topicName)
	}

	return topicName, partitionIndex, nil
}

// recoverNextOffset counts RecordBatches in the existing log to recover NextOffset.
// Each RecordBatch corresponds to one offset increment (as we track batches, not records).
func recoverNextOffset(logDir string) int64 {
	logFile := logDir + "/00000000000000000000.log"
	data, err := os.ReadFile(logFile)
	if err != nil {
		return 0
	}
	var count int64
	pos := 0
	for pos+12 <= len(data) {
		batchLength := int(binary.BigEndian.Uint32(data[pos+8 : pos+12]))
		batchEnd := pos + 12 + batchLength
		if batchEnd > len(data) {
			break
		}
		count++
		pos = batchEnd
	}
	return count
}

// parseClusterMetadataForTopics reads __cluster_metadata log using proper
// RecordBatch format and extracts topic UUIDs from TOPIC_RECORD entries (type 2).
func parseClusterMetadataForTopics(logPath string, _ map[string]bool) (map[string]uuid.UUID, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster metadata log: %w", err)
	}

	topicUUIDs := make(map[string]uuid.UUID)
	pos := 0

	for pos < len(data) {
		// RecordBatch header (minimum 61 bytes):
		//   baseOffset        INT64   8
		//   batchLength       INT32   4  ← length of everything after this field
		//   partitionLeaderEpoch INT32 4
		//   magic             INT8    1
		//   crc               UINT32  4
		//   attributes        INT16   2
		//   lastOffsetDelta   INT32   4
		//   baseTimestamp     INT64   8
		//   maxTimestamp      INT64   8
		//   producerId        INT64   8
		//   producerEpoch     INT16   2
		//   baseSequence      INT32   4
		//   recordsCount      INT32   4   ← total fixed header = 61 bytes
		if pos+12 > len(data) {
			break
		}
		// baseOffset (8) + batchLength (4)
		batchLength := int(binary.BigEndian.Uint32(data[pos+8 : pos+12]))
		batchEnd := pos + 12 + batchLength
		if batchEnd > len(data) {
			break
		}

		// Parse records inside this batch starting at the records array.
		// Fixed batch header after batchLength = 49 bytes, then recordsCount INT32 = 4 bytes
		// Total offset to first record = 8+4+49 = 61
		if pos+61 > len(data) {
			pos = batchEnd
			continue
		}
		recordsCount := int(binary.BigEndian.Uint32(data[pos+57 : pos+61]))
		recordPos := pos + 61

		for r := 0; r < recordsCount && recordPos < batchEnd; r++ {
			// Each Record:
			//   length        VARINT   (signed zigzag)
			//   attributes    INT8     1
			//   timestampDelta VARINT
			//   offsetDelta   VARINT
			//   keyLength     VARINT  (-1 = null)
			//   key           bytes
			//   valueLength   VARINT
			//   value         bytes
			//   headers       VARINT count + entries

			recLen, n := readSignedVarint(data, recordPos)
			if n == 0 {
				break
			}
			recEnd := recordPos + n + int(recLen)
			if recEnd > len(data) {
				break
			}

			cursor := recordPos + n
			cursor++ // attributes INT8

			// skip timestampDelta and offsetDelta varints
			_, dn := readSignedVarint(data, cursor)
			cursor += dn
			_, dn = readSignedVarint(data, cursor)
			cursor += dn

			// keyLength VARINT (signed; -1 = null)
			keyLen, dn := readSignedVarint(data, cursor)
			cursor += dn
			if keyLen > 0 {
				cursor += int(keyLen) // skip key bytes
			}

			// valueLength VARINT
			valLen, dn := readSignedVarint(data, cursor)
			cursor += dn
			if valLen < 0 || cursor+int(valLen) > len(data) {
				recordPos = recEnd
				continue
			}
			valueBytes := data[cursor : cursor+int(valLen)]

			// Metadata record value layout (KIP-631):
			//   frameVersion  INT8   1
			//   type          INT8   1   (2 = TOPIC_RECORD, 3 = PARTITION_RECORD)
			//   version       INT8   1
			//   ... fields depend on type
			if len(valueBytes) >= 3 {
				recordType := valueBytes[1]
				if recordType == 2 { // TOPIC_RECORD
					// After frameVersion(1)+type(1)+version(1):
					// name: COMPACT_STRING (varint length+1, then bytes)
					// topicId: UUID 16 bytes
					vpos := 3
					nameLen, dn := readUvarint(valueBytes, vpos)
					vpos += dn
					if nameLen > 0 {
						actualNameLen := int(nameLen) - 1 // compact encoding
						if vpos+actualNameLen+16 <= len(valueBytes) {
							name := string(valueBytes[vpos : vpos+actualNameLen])
							vpos += actualNameLen
							id, err := uuid.FromBytes(valueBytes[vpos : vpos+16])
							if err == nil {
								topicUUIDs[name] = id
							}
						}
					}
				}
			}

			recordPos = recEnd
		}

		pos = batchEnd
	}

	return topicUUIDs, nil
}

// readSignedVarint reads a zigzag-encoded signed varint from data[pos].
// Returns the value and number of bytes consumed (0 on error).
func readSignedVarint(data []byte, pos int) (int64, int) {
	uval, n := readUvarint(data, pos)
	// zigzag decode
	return int64((uval >> 1) ^ -(uval & 1)), n
}

// readUvarint reads an unsigned varint from data[pos].
// Returns the value and number of bytes consumed (0 on error).
func readUvarint(data []byte, pos int) (uint64, int) {
	var val uint64
	var shift uint
	for i := pos; i < len(data) && i < pos+10; i++ {
		b := data[i]
		val |= uint64(b&0x7F) << shift
		shift += 7
		if b&0x80 == 0 {
			return val, i - pos + 1
		}
	}
	return 0, 0
}

// ReadPartitionLog reads the log file for a partition and returns the raw bytes
// from the given file position onwards (0 = from beginning).
func ReadPartitionLog(partition *Partition) ([]byte, error) {
	return ReadPartitionLogFrom(partition, 0)
}

// ReadPartitionLogFrom reads the log file starting at byteOffset.
func ReadPartitionLogFrom(partition *Partition, byteOffset int64) ([]byte, error) {
	logFilePath := partition.LogDir + "/00000000000000000000.log"
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}
	if byteOffset > 0 && byteOffset < int64(len(data)) {
		return data[byteOffset:], nil
	}
	return data, nil
}
