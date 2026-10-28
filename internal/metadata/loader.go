package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/storage"
)

// LoadTopicsFromDisk must finish before starting listeners or background workers.
// No topics are published until the complete catalog and every replica validate.
func (m *Manager) LoadTopicsFromDisk(logDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loaded {
		return fmt.Errorf("metadata is already initialized")
	}
	if m.persistenceErr != nil {
		return fmt.Errorf("metadata writes fenced until restart: %w", m.persistenceErr)
	}
	if err := m.setLogDir(logDir); err != nil {
		return err
	}
	return m.loadTopics()
}

func (m *Manager) loadTopics() error {
	locations, err := discoverLogs(m.logDir)
	if err != nil {
		return err
	}
	catalog, err := m.readCatalog()
	legacy := os.IsNotExist(err)
	if err != nil && !legacy {
		return err
	}
	if legacy {
		catalog, err = m.importLegacy(locations)
		if err != nil {
			return err
		}
	}
	expected := make(map[string]bool)
	topics := make(map[string]*Topic, len(catalog))
	ids := make(map[uuid.UUID]bool)
	for name, definition := range catalog {
		if definition.ID == uuid.Nil || ids[definition.ID] {
			return fmt.Errorf("missing or duplicate topic ID for %s", name)
		}
		ids[definition.ID] = true
		if err := m.validateDefinition(definition); err != nil {
			return err
		}
		topic := &Topic{Name: name, ID: definition.ID, IsInternal: definition.IsInternal, Partitions: make([]Partition, len(definition.Partitions))}
		for i, saved := range definition.Partitions {
			p := &topic.Partitions[i]
			p.Index, p.LeaderID, p.LeaderEpoch = saved.Index, saved.LeaderID, saved.LeaderEpoch
			p.ReplicaNodes = slices.Clone(saved.Replicas)
			// ISR and HW are rebuilt conservatively from all assigned on-disk replicas.
			p.ISRNodes = slices.Clone(saved.Replicas)
			p.BrokerLogDirs = make(map[int32]string)
			p.ReplicaLEO = make(map[int32]int64)
			p.ReplicaLastSeen = make(map[int32]int64)
			for _, id := range saved.Replicas {
				relative := saved.LogDirs[id]
				if _, exists := locations[relative]; !exists {
					return fmt.Errorf("missing replica directory %s", relative)
				}
				expected[relative] = true
				dir := filepath.Join(m.logDir, relative)
				segs, err := listSegments(dir)
				if err != nil {
					return err
				}
				if !legacy && len(segs) == 0 {
					return fmt.Errorf("missing segments in saved replica %s", relative)
				}
				recovered, err := recoverLog(dir)
				if err != nil {
					return err
				}
				if recovered.epoch > saved.LeaderEpoch {
					return fmt.Errorf("log epoch %d exceeds saved leader epoch %d in %s", recovered.epoch, saved.LeaderEpoch, relative)
				}
				p.BrokerLogDirs[id] = dir
				p.ReplicaLEO[id] = recovered.end
			}
			p.NextOffset = p.ReplicaLEO[p.LeaderID]
			p.LogEndOffset = p.NextOffset
			for _, id := range saved.Replicas {
				if id == p.LeaderID {
					continue
				}
				if p.ReplicaLEO[id] > p.LogEndOffset {
					return fmt.Errorf("replica %d of %s-%d is ahead of the saved leader", id, name, i)
				}
				if err := verifyReplicaPrefix(p.BrokerLogDirs[p.LeaderID], p.BrokerLogDirs[id]); err != nil {
					return fmt.Errorf("%s-%d replica %d: %w", name, i, id, err)
				}
			}
			p.AdvanceHighWatermark()
		}
		topics[name] = topic
	}
	for path := range locations {
		if !expected[path] {
			return fmt.Errorf("untracked partition directory %s; explicit recovery is required", path)
		}
	}
	if legacy {
		for path := range locations {
			if err := initializeLog(filepath.Join(m.logDir, path)); err != nil {
				return err
			}
		}
		if err := m.persistCatalog(catalog); err != nil {
			return err
		}
	}
	m.catalog, m.topics, m.loaded = catalog, topics, true
	return nil
}

type logLocation struct {
	topic     string
	partition int
	broker    int32
}

func discoverLogs(root string) (map[string]logLocation, error) {
	locations := make(map[string]logLocation)
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return locations, nil
	}
	if err != nil {
		return nil, err
	}
	add := func(relative string, id int32, entry os.DirEntry) error {
		name, partition, err := parseTopicDirName(entry.Name())
		if err != nil {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinked partition directory is not supported: %s", relative)
		}
		if !entry.IsDir() {
			return nil
		}
		locations[relative] = logLocation{name, partition, id}
		return nil
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "broker-") {
			id, err := strconv.ParseInt(strings.TrimPrefix(name, "broker-"), 10, 32)
			if err == nil && id > 0 && name == fmt.Sprintf("broker-%d", id) {
				if !entry.IsDir() {
					return nil, fmt.Errorf("not a real broker directory: %s", name)
				}
				children, err := os.ReadDir(filepath.Join(root, name))
				if err != nil {
					return nil, err
				}
				for _, child := range children {
					if strings.HasSuffix(child.Name(), ".log") {
						return nil, fmt.Errorf("ambiguous broker/partition directory %s", name)
					}
					if err := add(filepath.Join(name, child.Name()), int32(id), child); err != nil {
						return nil, err
					}
				}
				continue
			}
		}
		if err := add(name, 1, entry); err != nil {
			return nil, err
		}
	}
	return locations, nil
}

func (m *Manager) importLegacy(locations map[string]logLocation) (map[string]topicMetadata, error) {
	byTopic := make(map[string]map[int]string)
	for path, location := range locations {
		partitions := byTopic[location.topic]
		if partitions == nil {
			partitions = make(map[int]string)
			byTopic[location.topic] = partitions
		}
		if prior, exists := partitions[location.partition]; exists {
			return nil, fmt.Errorf("ambiguous legacy ownership for %s-%d: %s and %s; preserve logs and supply authoritative metadata",
				location.topic, location.partition, prior, path)
		}
		partitions[location.partition] = path
	}
	ids, err := parseClusterMetadataForTopics(filepath.Join(m.logDir, "__cluster_metadata-0", segmentNameForOffset(0)))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	catalog := make(map[string]topicMetadata)
	for name, partitions := range byTopic {
		id := ids[name]
		if id == uuid.Nil {
			id = uuid.New()
			logger.L.Info("assigning persistent ID to legacy topic", "topic", name, "id", id)
		}
		definition := topicMetadata{Name: name, ID: id, Partitions: make([]partitionMetadata, len(partitions))}
		for i := range definition.Partitions {
			path, exists := partitions[i]
			if !exists {
				return nil, fmt.Errorf("missing partition %s-%d in legacy layout", name, i)
			}
			location := locations[path]
			recovered, err := recoverLog(filepath.Join(m.logDir, path))
			if err != nil {
				return nil, err
			}
			definition.Partitions[i] = partitionMetadata{
				Index: int32(i), LeaderID: location.broker, LeaderEpoch: recovered.epoch,
				Replicas: []int32{location.broker}, LogDirs: map[int32]string{location.broker: path},
			}
		}
		catalog[name] = definition
	}
	return catalog, nil
}

func parseTopicDirName(dirName string) (string, int, error) {
	lastHyphen := strings.LastIndex(dirName, "-")
	if lastHyphen < 1 {
		return "", 0, fmt.Errorf("invalid partition directory %q", dirName)
	}
	name := dirName[:lastHyphen]
	if err := validateTopicName(name); err != nil {
		return "", 0, err
	}
	index, err := strconv.Atoi(dirName[lastHyphen+1:])
	if err != nil || index < 0 || index >= maxTopicPartitions || strconv.Itoa(index) != dirName[lastHyphen+1:] {
		return "", 0, fmt.Errorf("invalid partition index in %q", dirName)
	}
	return name, index, nil
}

type recoveredLog struct {
	end   int64
	epoch int32
}

func recoverNextOffset(logDir string) (int64, error) {
	log, err := recoverLog(logDir)
	return log.end, err
}

// recoverLog validates every batch without modifying log or index files.
func recoverLog(logDir string) (recoveredLog, error) {
	segs, err := listSegments(logDir)
	if err != nil || len(segs) == 0 {
		return recoveredLog{}, err
	}
	log := recoveredLog{end: segs[0].baseOffset}
	for _, segment := range segs {
		if segment.baseOffset != log.end {
			return recoveredLog{}, fmt.Errorf("noncontiguous segment %s: expected offset %d", segment.name, log.end)
		}
		if err := recoverSegment(filepath.Join(logDir, segment.name), &log); err != nil {
			return recoveredLog{}, err
		}
	}
	return log, nil
}

func recoverSegment(path string, log *recoveredLog) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var position int64
	for {
		batch, err := storage.ReadRecordBatch(f)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recover %s at byte %d: %w", path, position, err)
		}
		if batch.BaseOffset != log.end || log.end > math.MaxInt64-int64(batch.RecordsCount) {
			return fmt.Errorf("recover %s at byte %d: invalid offset %d, expected %d", path, position, batch.BaseOffset, log.end)
		}
		epoch := int32(binary.BigEndian.Uint32(batch.Data[12:16]))
		if epoch > log.epoch {
			log.epoch = epoch
		}
		log.end += int64(batch.RecordsCount)
		position += int64(len(batch.Data))
	}
}

type segmentReader struct {
	dir      string
	segments []logSegment
	file     *os.File
}

func (r *segmentReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func (r *segmentReader) Read(b []byte) (int, error) {
	for {
		if r.file == nil {
			if len(r.segments) == 0 {
				return 0, io.EOF
			}
			f, err := os.Open(filepath.Join(r.dir, r.segments[0].name))
			if err != nil {
				return 0, err
			}
			r.file = f
			r.segments = r.segments[1:]
		}
		n, err := r.file.Read(b)
		if err != io.EOF {
			return n, err
		}
		if err := r.file.Close(); err != nil {
			return n, err
		}
		r.file = nil
		if n > 0 {
			return n, nil
		}
	}
}

func verifyReplicaPrefix(leaderDir, replicaDir string) error {
	leaderSegments, err := listSegments(leaderDir)
	if err != nil {
		return err
	}
	replicaSegments, err := listSegments(replicaDir)
	if err != nil {
		return err
	}
	if len(leaderSegments) == 0 || len(replicaSegments) == 0 ||
		leaderSegments[0].baseOffset != replicaSegments[0].baseOffset {
		return fmt.Errorf("replica and leader log start offsets differ")
	}
	leader := &segmentReader{dir: leaderDir, segments: leaderSegments}
	replica := &segmentReader{dir: replicaDir, segments: replicaSegments}
	defer leader.Close()
	defer replica.Close()
	a, b := make([]byte, 64*1024), make([]byte, 64*1024)
	for {
		n, err := replica.Read(b)
		if n > 0 {
			if _, readErr := io.ReadFull(leader, a[:n]); readErr != nil {
				return fmt.Errorf("replica is not a prefix of leader: %w", readErr)
			}
			if !bytes.Equal(a[:n], b[:n]) {
				return fmt.Errorf("replica data diverges from saved leader")
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Import topic IDs from the legacy KIP-631 topic-record log when available.
func parseClusterMetadataForTopics(path string) (map[string]uuid.UUID, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ids := make(map[string]uuid.UUID)
	for {
		batch, err := storage.ReadRecordBatch(f)
		if err == io.EOF {
			return ids, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read cluster metadata: %w", err)
		}
		for _, value := range batch.Values {
			if len(value) < 3 {
				return nil, fmt.Errorf("truncated cluster metadata record")
			}
			if value[1] != 2 {
				continue
			}
			if value[0] != 1 || value[2] != 0 {
				return nil, fmt.Errorf("unsupported topic metadata record version")
			}
			length, n := binary.Uvarint(value[3:])
			if n <= 0 || length == 0 || length > uint64(len(value)) {
				return nil, fmt.Errorf("invalid metadata topic name length")
			}
			end := 3 + n + int(length-1)
			if end+16 > len(value) {
				return nil, fmt.Errorf("truncated metadata topic ID")
			}
			name := string(value[3+n : end])
			id, err := uuid.FromBytes(value[end : end+16])
			if err != nil || id == uuid.Nil {
				return nil, fmt.Errorf("invalid metadata topic ID")
			}
			if previous, exists := ids[name]; exists && previous != id {
				return nil, fmt.Errorf("ambiguous historical IDs for topic %s", name)
			}
			ids[name] = id
		}
	}
}

// ReadPartitionLog reads the first segment of the partition log from the beginning.
func ReadPartitionLog(partition *Partition) ([]byte, error) {
	return ReadPartitionLogFrom(partition, "00000000000000000000.log", 0)
}

// ReadPartitionLogFrom reads a specific segment starting at byteOffset.
func ReadPartitionLogFrom(partition *Partition, segmentLogFile string, byteOffset int64, logDir ...string) ([]byte, error) {
	dir := partition.LogDirForBroker(partition.LeaderID)
	if len(logDir) > 0 && logDir[0] != "" {
		dir = logDir[0]
	}
	f, err := os.Open(filepath.Join(dir, segmentLogFile))
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer f.Close()
	if byteOffset > 0 {
		if _, err := f.Seek(byteOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek in log file: %w", err)
		}
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}
	return data, nil
}
