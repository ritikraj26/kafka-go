package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/ritiraj/kafka-go/internal/logger"
)

const (
	metadataFileName   = "broker-metadata.json"
	metadataVersion    = 1
	maxMetadataBytes   = 16 << 20
	maxTopicPartitions = 10000
)

type diskMetadata struct {
	Version int             `json:"version"`
	Topics  []topicMetadata `json:"topics"`
}

type topicMetadata struct {
	Name       string              `json:"name"`
	ID         uuid.UUID           `json:"id"`
	IsInternal bool                `json:"is_internal"`
	Partitions []partitionMetadata `json:"partitions"`
}

type partitionMetadata struct {
	Index       int32            `json:"index"`
	LeaderID    int32            `json:"leader_id"`
	LeaderEpoch int32            `json:"leader_epoch"`
	Replicas    []int32          `json:"replicas"`
	LogDirs     map[int32]string `json:"log_dirs"`
}

func validateTopicName(name string) error {
	if name == "" || name == "." || name == ".." || len(name) > 249 || strings.HasPrefix(name, "__") {
		return fmt.Errorf("invalid or reserved topic name %q", name)
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return fmt.Errorf("invalid topic name %q", name)
		}
	}
	return nil
}

func cloneCatalog(catalog map[string]topicMetadata) map[string]topicMetadata {
	next := make(map[string]topicMetadata, len(catalog))
	for name, topic := range catalog {
		next[name] = topic
	}
	return next
}

// Catalog definitions are immutable once published. Callers hold m.mu.
func (m *Manager) persistCatalog(catalog map[string]topicMetadata) error {
	disk := diskMetadata{Version: metadataVersion, Topics: make([]topicMetadata, 0, len(catalog))}
	for _, topic := range catalog {
		disk.Topics = append(disk.Topics, topic)
	}
	slices.SortFunc(disk.Topics, func(a, b topicMetadata) int { return strings.Compare(a.Name, b.Name) })
	data, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxMetadataBytes {
		return fmt.Errorf("metadata exceeds %d-byte limit", maxMetadataBytes)
	}
	if err := writeMetadata(m.logDir, data); err != nil {
		m.persistenceErr = err
		return fmt.Errorf("persist broker metadata: %w", err)
	}
	return nil
}

func writeMetadata(dir string, data []byte) error {
	if err := syncDirectoryTree(dir); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".broker-metadata-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil && !os.IsNotExist(err) {
			logger.L.Warn("failed to remove temporary metadata file", "path", f.Name(), "err", err)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, metadataFileName)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Newly created directory entries must be durable in their parents as well.
func syncDirectoryTree(dir string) error {
	// Configured ancestors such as /tmp may themselves be symlinks.
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("not a real directory: %s", dir)
		}
		return syncDirectory(dir)
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return err
	}
	if err := syncDirectoryTree(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func initializeLog(dir string) error {
	if err := syncDirectoryTree(dir); err != nil {
		return err
	}
	segments, err := listSegments(dir)
	if err != nil {
		return err
	}
	if len(segments) > 0 {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(dir, segmentNameForOffset(0)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (m *Manager) readCatalog() (map[string]topicMetadata, error) {
	path := filepath.Join(m.logDir, metadataFileName)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("metadata is not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxMetadataBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxMetadataBytes {
		return nil, fmt.Errorf("metadata exceeds %d-byte limit", maxMetadataBytes)
	}
	var disk diskMetadata
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&disk); err != nil {
		return nil, fmt.Errorf("decode broker metadata: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing data in broker metadata")
	}
	if disk.Version != metadataVersion || disk.Topics == nil {
		return nil, fmt.Errorf("unsupported or incomplete broker metadata version %d", disk.Version)
	}
	catalog := make(map[string]topicMetadata, len(disk.Topics))
	ids := make(map[uuid.UUID]bool)
	for _, topic := range disk.Topics {
		if _, exists := catalog[topic.Name]; exists || ids[topic.ID] || topic.ID == uuid.Nil {
			return nil, fmt.Errorf("duplicate topic name/ID or missing ID: %q", topic.Name)
		}
		if err := m.validateDefinition(topic); err != nil {
			return nil, err
		}
		catalog[topic.Name] = topic
		ids[topic.ID] = true
	}
	return catalog, nil
}

func (m *Manager) validateDefinition(topic topicMetadata) error {
	if err := validateTopicName(topic.Name); err != nil {
		return err
	}
	if len(topic.Partitions) == 0 || len(topic.Partitions) > maxTopicPartitions {
		return fmt.Errorf("invalid partition count for %s", topic.Name)
	}
	for i, p := range topic.Partitions {
		if p.Index != int32(i) || p.LeaderEpoch < 0 || len(p.Replicas) == 0 ||
			len(p.Replicas) != len(p.LogDirs) || !slices.Contains(p.Replicas, p.LeaderID) {
			return fmt.Errorf("invalid partition definition: %s-%d", topic.Name, p.Index)
		}
		seen := make(map[int32]bool)
		for _, id := range p.Replicas {
			if id <= 0 || seen[id] {
				return fmt.Errorf("invalid or duplicate replica %d for %s-%d", id, topic.Name, i)
			}
			if m.Brokers != nil && m.Brokers.Get(id) == nil {
				return fmt.Errorf("saved replica %d is absent from the configured broker registry", id)
			}
			seen[id] = true
			flat := fmt.Sprintf("%s-%d", topic.Name, i)
			canonical := filepath.Join(fmt.Sprintf("broker-%d", id), flat)
			path := p.LogDirs[id]
			if path != canonical && !(id == 1 && path == flat) {
				return fmt.Errorf("invalid saved log path %q for %s-%d replica %d", path, topic.Name, i, id)
			}
		}
	}
	return nil
}
