package metadata

import (
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
			topic.Partitions = append(topic.Partitions, Partition{
				Index:           partIdx,
				LeaderID:        1,
				ReplicaNodes:    []int32{1},
				ISRNodes:        []int32{1},
				OfflineReplicas: []int32{},
				LeaderEpoch:     0,
				PartitionEpoch:  0,
				LogDir:          partLogDir,
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

// parseClusterMetadata reads the cluster metadata log and extracts topic UUIDs
func parseClusterMetadataForTopics(logPath string, topicNames map[string]bool) (map[string]uuid.UUID, error) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster metadata log: %w", err)
	}

	fmt.Printf("Reading cluster metadata from %s (%d bytes)\n", logPath, len(data))
	fmt.Printf("Looking for topics: %v\n", getKeys(topicNames))

	topicUUIDs := make(map[string]uuid.UUID)

	// For each topic name, search for it in the metadata
	for topicName := range topicNames {
		// Search for the topic name as a string in the data
		nameBytes := []byte(topicName)

		var bestUUID uuid.UUID
		bestScore := 0

		// Find all occurrences of the topic name
		for i := 0; i < len(data)-len(nameBytes); i++ {
			if string(data[i:i+len(nameBytes)]) == topicName {
				// Found the topic name, look for UUID nearby
				// Prefer UUIDs that are AFTER the topic name (more likely to be the topic ID)
				searchStart := i + len(nameBytes)
				searchEnd := searchStart + 40 // Look within 40 bytes after the name
				if searchEnd > len(data) {
					searchEnd = len(data)
				}

				// Look for a 16-byte UUID in this region
				for j := searchStart; j < searchEnd-16; j++ {
					uuidBytes := data[j : j+16]
					topicID, err := uuid.FromBytes(uuidBytes)
					if err == nil && topicID != uuid.Nil && !isInvalidUUID(topicID) {
						// Score based on how close to the topic name (closer is better)
						distance := j - (i + len(nameBytes))
						score := 1000 - distance

						if score > bestScore {
							bestScore = score
							bestUUID = topicID
						}
					}
				}
			}
		}

		if bestUUID != uuid.Nil {
			topicUUIDs[topicName] = bestUUID
			fmt.Printf("Found UUID for topic %s: %s (score: %d)\n", topicName, bestUUID, bestScore)
		}
	}

	return topicUUIDs, nil
}

// isInvalidUUID checks if a UUID looks invalid (all zeros, all FFs, mostly zeros, etc.)
func isInvalidUUID(id uuid.UUID) bool {
	bytes := id[:]
	allZeros := true
	allFFs := true
	zeroCount := 0

	for _, b := range bytes {
		if b != 0 {
			allZeros = false
		} else {
			zeroCount++
		}
		if b != 0xFF {
			allFFs = false
		}
	}

	// Reject if all zeros, all FFs, or more than 10 bytes are zero
	return allZeros || allFFs || zeroCount > 10
}

// getKeys returns the keys of a map as a slice
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ReadPartitionLog reads the log file for a partition and returns the raw bytes
// The log file is located at <partition.LogDir>/00000000000000000000.log
func ReadPartitionLog(partition *Partition) ([]byte, error) {
	logFilePath := partition.LogDir + "/00000000000000000000.log"

	data, err := os.ReadFile(logFilePath)
	if err != nil {
		// If file doesn't exist, return empty (no messages)
		if os.IsNotExist(err) {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	return data, nil
}
