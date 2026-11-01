package coordinator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
)

const (
	offsetLogName          = "00000000000000000000.log"
	offsetLogVersion       = 1
	maxOffsetRecordBytes   = 256 * 1024
	maxOffsetMetadataBytes = 4096
)

var (
	offsetCRCTable            = crc32.MakeTable(crc32.Castagnoli)
	errOffsetMetadataTooLarge = errors.New("offset metadata exceeds 4096 bytes")
)

type committedOffset struct {
	Offset   int64
	Metadata *string
}

type offsetMap map[string]map[string]map[int32]committedOffset

type offsetRecord struct {
	Group     string  `json:"group"`
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	Metadata  *string `json:"metadata"`
}

type offsetEnvelope struct {
	Version *int            `json:"version"`
	Record  json.RawMessage `json:"record"`
	CRC     *uint32         `json:"crc32c"`
}

type offsetLogFile interface {
	io.Writer
	Sync() error
	Close() error
}

// LoadOffsets initializes persistence once, before any offset API is served.
// Recovery publishes nothing until the entire log validates. On failure, files
// are preserved and this coordinator cannot retry initialization in place.
func (c *Coordinator) LoadOffsets(logDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.offsetReady || c.offsetErr != nil {
		return fmt.Errorf("offset store is already initialized or fenced; restart with a new coordinator")
	}
	if logDir == "" {
		return fmt.Errorf("offset log directory must not be empty")
	}
	dir, err := filepath.Abs(filepath.Join(logDir, "__consumer_offsets-0"))
	if err != nil {
		return err
	}
	offsets, count, err := loadOffsetLog(dir, c.limits)
	if err != nil {
		c.offsetErr = err
		return fmt.Errorf("recover consumer offsets: %w", err)
	}
	c.offsetLogDir, c.offsets, c.offsetReady = dir, offsets, true
	c.offsetEntries, c.offsetBytes = offsetMapUsage(offsets)
	logger.L.Info("loaded committed offsets", "records", count)
	return nil
}

func loadOffsetLog(dir string, limits StateLimits) (offsetMap, int, error) {
	info, err := os.Lstat(dir)
	fresh := os.IsNotExist(err)
	if err != nil && !fresh {
		return nil, 0, err
	}
	if !fresh && !info.IsDir() {
		return nil, 0, fmt.Errorf("offset path is not a real directory: %s", dir)
	}
	if !fresh {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, 0, err
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".log") && entry.Name() != offsetLogName {
				return nil, 0, fmt.Errorf("unexpected offset log segment %s", entry.Name())
			}
		}
	}
	if fresh {
		if err := storage.MakeDirectory(dir); err != nil {
			return nil, 0, err
		}
	}
	path := filepath.Join(dir, offsetLogName)
	flags := os.O_RDWR
	if fresh {
		flags |= os.O_CREATE | os.O_EXCL
	} else if err := regularOffsetLog(path); err != nil {
		return nil, 0, err
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, 0, err
	}
	offsets, count, replayErr := replayOffsetsLimited(f, limits)
	if replayErr == nil {
		replayErr = f.Sync()
	}
	if err := errors.Join(replayErr, f.Close()); err != nil {
		return nil, 0, err
	}
	if err := storage.SyncDirectory(dir); err != nil {
		return nil, 0, err
	}
	return offsets, count, nil
}

func replayOffsets(r io.Reader) (offsetMap, int, error) {
	return replayOffsetsLimited(r, DefaultStateLimits())
}

func replayOffsetsLimited(r io.Reader, limits StateLimits) (offsetMap, int, error) {
	reader := bufio.NewReaderSize(r, maxOffsetRecordBytes+1)
	offsets := make(offsetMap)
	count := 0
	entries := 0
	var retained int64
	for {
		line, err := reader.ReadSlice('\n')
		if err == io.EOF && len(line) == 0 {
			return offsets, count, nil
		}
		if err != nil {
			return nil, 0, fmt.Errorf("offset record %d: incomplete or unreadable record: %w", count+1, err)
		}
		if len(line) > maxOffsetRecordBytes {
			return nil, 0, fmt.Errorf("offset record %d exceeds %d bytes", count+1, maxOffsetRecordBytes)
		}
		record, err := decodeOffsetRecord(line)
		if err != nil {
			return nil, 0, fmt.Errorf("offset record %d: %w", count+1, err)
		}
		nextEntries, nextBytes := nextOffsetUsage(offsets, entries, retained, record)
		if nextEntries > limits.MaxOffsets || nextBytes > limits.OffsetBytes {
			return nil, 0, fmt.Errorf("offset record %d exceeds retained offset quotas", count+1)
		}
		offsets.put(record)
		entries, retained = nextEntries, nextBytes
		count++
	}
}

func decodeOffsetJSON(data []byte, value any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("invalid UTF-8 in offset record")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON in offset record")
	}
	return nil
}

func decodeOffsetRecord(data []byte) (offsetRecord, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return offsetRecord{}, err
	}
	if _, wrapped := fields["version"]; wrapped {
		var envelope offsetEnvelope
		if err := decodeOffsetJSON(data, &envelope); err != nil {
			return offsetRecord{}, err
		}
		if envelope.Version == nil || *envelope.Version != offsetLogVersion || envelope.CRC == nil || envelope.Record == nil {
			return offsetRecord{}, fmt.Errorf("unsupported or incomplete offset envelope")
		}
		if crc32.Checksum(envelope.Record, offsetCRCTable) != *envelope.CRC {
			return offsetRecord{}, fmt.Errorf("offset CRC32C mismatch")
		}
		data = envelope.Record
	}
	// Pointers distinguish a legitimate zero offset/partition from a missing field.
	var wire struct {
		Group     *string `json:"group"`
		Topic     *string `json:"topic"`
		Partition *int32  `json:"partition"`
		Offset    *int64  `json:"offset"`
		Metadata  *string `json:"metadata"`
	}
	if err := decodeOffsetJSON(data, &wire); err != nil {
		return offsetRecord{}, err
	}
	if wire.Group == nil || wire.Topic == nil || wire.Partition == nil || wire.Offset == nil {
		return offsetRecord{}, fmt.Errorf("missing required offset field")
	}
	record := offsetRecord{Group: *wire.Group, Topic: *wire.Topic, Partition: *wire.Partition, Offset: *wire.Offset, Metadata: wire.Metadata}
	if err := validateOffsetRecord(record); err != nil {
		return offsetRecord{}, err
	}
	return record, nil
}

func validateOffsetRecord(record offsetRecord) error {
	if record.Group == "" || len(record.Group) > 32767 || !utf8.ValidString(record.Group) ||
		record.Topic == "" || len(record.Topic) > 249 || !utf8.ValidString(record.Topic) ||
		record.Partition < 0 || record.Offset < 0 {
		return fmt.Errorf("invalid group, topic, partition or committed offset")
	}
	if record.Metadata != nil {
		if len(*record.Metadata) > maxOffsetMetadataBytes {
			return errOffsetMetadataTooLarge
		}
		if !utf8.ValidString(*record.Metadata) {
			return fmt.Errorf("invalid UTF-8 in offset metadata")
		}
	}
	return nil
}

func encodeOffsetRecord(record offsetRecord) ([]byte, error) {
	if err := validateOffsetRecord(record); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	version, checksum := offsetLogVersion, crc32.Checksum(payload, offsetCRCTable)
	data, err := json.Marshal(offsetEnvelope{Version: &version, Record: payload, CRC: &checksum})
	if err != nil {
		return nil, err
	}
	if len(data)+1 > maxOffsetRecordBytes {
		return nil, fmt.Errorf("encoded offset record exceeds size limit")
	}
	return append(data, '\n'), nil
}

func (offsets offsetMap) put(record offsetRecord) {
	if offsets[record.Group] == nil {
		offsets[strings.Clone(record.Group)] = make(map[string]map[int32]committedOffset)
	}
	if offsets[record.Group][record.Topic] == nil {
		offsets[record.Group][strings.Clone(record.Topic)] = make(map[int32]committedOffset)
	}
	offsets[record.Group][record.Topic][record.Partition] = committedOffset{Offset: record.Offset, Metadata: copyOffsetMetadata(record.Metadata)}
}

func copyOffsetMetadata(value *string) *string {
	if value == nil {
		return nil
	}
	copy := strings.Clone(*value)
	return &copy
}

func regularOffsetLog(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("offset log is not a regular file: %s", path)
	}
	return nil
}

func openOffsetLogFile(path string) (offsetLogFile, error) {
	if err := regularOffsetLog(path); err != nil {
		return nil, err
	}
	// Do not silently recreate an initialized log that disappeared.
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
}

func (c *Coordinator) persistOffset(data []byte) error {
	f, err := c.openOffsetLog(filepath.Join(c.offsetLogDir, offsetLogName))
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// CommitOffset is the no-metadata form of CommitOffsetWithMetadata.
func (c *Coordinator) CommitOffset(groupID string, generationID int32, memberID, topic string, partition int32, offset int64) int16 {
	return c.CommitOffsetWithMetadata(groupID, generationID, memberID, topic, partition, offset, nil)
}

// CommitOffsetWithMetadata holds c.mu then g.mu through persistence. Membership
// cannot change between validation and the durable append; memory is published last.
func (c *Coordinator) CommitOffsetWithMetadata(groupID string, generationID int32, memberID, topic string, partition int32, offset int64, metadata *string) int16 {
	return c.CommitOffsetWithMetadataContext(context.Background(), groupID, generationID, memberID, topic, partition, offset, metadata)
}

func (c *Coordinator) CommitOffsetWithMetadataContext(ctx context.Context, groupID string, generationID int32, memberID, topic string, partition int32, offset int64, metadata *string) int16 {
	if ctx.Err() != nil {
		logger.L.Debug("offset commit cancelled before validation")
		return protocol.ErrRequestTimedOut
	}
	record := offsetRecord{Group: groupID, Topic: topic, Partition: partition, Offset: offset, Metadata: metadata}
	err := validateOffsetRecord(record)
	var data []byte
	if err == nil {
		record.Metadata = copyOffsetMetadata(record.Metadata)
		data, err = encodeOffsetRecord(record)
	}
	if err != nil {
		logger.L.Warn("rejecting offset commit", "group", groupID, "topic", topic, "partition", partition, "err", err)
		if errors.Is(err, errOffsetMetadataTooLarge) {
			return protocol.ErrOffsetMetadataTooLarge
		}
		return protocol.ErrInvalidRequest
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if code := c.offsetStoreError(); code != protocol.ErrNone {
		logger.L.Warn("offset commit unavailable", "group", groupID, "error_code", code)
		return code
	}
	g := c.groups[groupID]
	if g == nil {
		return protocol.ErrCoordinatorNotAvailable
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if ctx.Err() != nil {
		logger.L.Debug("offset commit cancelled before persistence")
		return protocol.ErrRequestTimedOut
	}
	if _, exists := g.Members[memberID]; !exists {
		return protocol.ErrUnknownMemberID
	}
	if generationID != g.GenerationID {
		return protocol.ErrIllegalGeneration
	}
	nextEntries, nextBytes := nextOffsetUsage(c.offsets, c.offsetEntries, c.offsetBytes, record)
	if nextEntries > c.limits.MaxOffsets || nextBytes > c.limits.OffsetBytes {
		logger.L.Warn("retained offset quota exceeded", "group", groupID, "topic", topic, "partition", partition)
		return protocol.ErrThrottlingQuotaExceeded
	}
	if err := c.persistOffset(data); err != nil {
		c.offsetErr = err
		logger.L.Error("offset persistence failed; store fenced until restart", "group", groupID, "topic", topic, "partition", partition, "err", err)
		return protocol.ErrKafkaStorageError
	}
	c.offsets.put(record)
	c.offsetEntries, c.offsetBytes = nextEntries, nextBytes
	return protocol.ErrNone
}

func (c *Coordinator) offsetStoreError() int16 {
	if c.closed {
		return protocol.ErrCoordinatorNotAvailable
	}
	if c.offsetErr != nil {
		return protocol.ErrKafkaStorageError
	}
	if !c.offsetReady {
		return protocol.ErrCoordinatorNotAvailable
	}
	return protocol.ErrNone
}

func (c *Coordinator) FetchOffset(groupID, topic string, partition int32) (int64, int16) {
	offset, _, code := c.FetchOffsetWithMetadata(groupID, topic, partition)
	return offset, code
}

// FetchOffsetWithMetadata returns a copy, preserving null versus empty metadata.
func (c *Coordinator) FetchOffsetWithMetadata(groupID, topic string, partition int32) (int64, *string, int16) {
	if groupID == "" || topic == "" || partition < 0 {
		logger.L.Warn("invalid offset fetch", "group", groupID, "topic", topic, "partition", partition)
		return -1, nil, protocol.ErrInvalidRequest
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if code := c.offsetStoreError(); code != protocol.ErrNone {
		logger.L.Warn("offset fetch unavailable", "group", groupID, "error_code", code)
		return -1, nil, code
	}
	if value, exists := c.offsets[groupID][topic][partition]; exists {
		return value.Offset, copyOffsetMetadata(value.Metadata), protocol.ErrNone
	}
	return -1, nil, protocol.ErrNone
}
