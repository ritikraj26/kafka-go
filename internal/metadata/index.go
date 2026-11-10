package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/storage"
)

type indexFile interface {
	segmentFile
	Stat() (os.FileInfo, error)
	ReadAt([]byte, int64) (int, error)
	Truncate(int64) error
}

// Like log I/O, index fault injection is local to a partition.
type indexIO struct {
	openFile func(string, int, os.FileMode) (indexFile, error)
}

func (fs *indexIO) open(path string) (indexFile, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR
	if fs != nil && fs.openFile != nil {
		return fs.openFile(path, flags, 0644)
	}
	return os.OpenFile(path, flags, 0644)
}

func offsetIndexPath(dir string, segment logSegment) string {
	return filepath.Join(dir, strings.TrimSuffix(segment.name, ".log")+".index")
}

// discardOffsetIndex only removes a regular cache file, never a directory or a
// symlink target. Failure to remove it must not prevent a validated log scan.
func discardOffsetIndex(path string) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logger.L.Warn("failed to discard offset index; using log scan", "path", path, "err", err)
	}
}

// appendIndexEntry appends an 8-byte relative-offset/byte-position entry. The
// cache may be sparse: after discarding a damaged tail, earlier offsets scan
// from the log's beginning until new entries become available.
func appendIndexEntry(path string, relativeOffset, filePos int32, fs *indexIO) (err error) {
	if relativeOffset < 0 || filePos < 0 {
		return fmt.Errorf("invalid offset index entry")
	}
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("offset index is not a regular file: %s", path)
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	f, err := fs.open(path)
	if err != nil {
		discardOffsetIndex(path)
		return err
	}
	defer func() {
		err = errors.Join(err, f.Close())
		if err != nil {
			discardOffsetIndex(path)
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("offset index is not a regular file: %s", path)
	}
	reset := info.Size()%8 != 0
	if !reset && info.Size() > 0 {
		var tail [8]byte
		if _, err := f.ReadAt(tail[:], info.Size()-8); err != nil {
			return err
		}
		lastOffset := int32(binary.BigEndian.Uint32(tail[:4]))
		lastPosition := int32(binary.BigEndian.Uint32(tail[4:]))
		reset = lastOffset < 0 || lastPosition < 0 || lastOffset >= relativeOffset || lastPosition >= filePos
	}
	if reset {
		if err := f.Truncate(0); err != nil {
			return err
		}
	}
	var entry [8]byte
	binary.BigEndian.PutUint32(entry[:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(entry[4:], uint32(filePos))
	if n, err := f.Write(entry[:]); err != nil {
		return err
	} else if n != len(entry) {
		return io.ErrShortWrite
	}
	return f.Sync()
}

// indexPosition reads only the entries needed by binary search. Its result is
// a cache hint; Fetch additionally checks it against the actual batch header.
func indexPosition(dir string, segment logSegment, offset int64) (int64, int64, error) {
	path := offsetIndexPath(dir, segment)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return 0, segment.baseOffset, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, 0, fmt.Errorf("offset index is not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size()%8 != 0 {
		return 0, 0, fmt.Errorf("incomplete offset index: %s", path)
	}
	lo, hi := int64(0), info.Size()/8-1
	position, base := int64(0), segment.baseOffset
	var entry [8]byte
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if _, err := f.ReadAt(entry[:], mid*8); err != nil {
			return 0, 0, err
		}
		relative := int64(int32(binary.BigEndian.Uint32(entry[:4])))
		pos := int64(int32(binary.BigEndian.Uint32(entry[4:])))
		if relative < 0 || pos < 0 || segment.baseOffset > math.MaxInt64-relative {
			return 0, 0, fmt.Errorf("invalid offset index entry: %s", path)
		}
		candidate := segment.baseOffset + relative
		if candidate <= offset {
			position, base = pos, candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return position, base, nil
}

// safeIndexPosition rejects unusable hints before planning any response spans,
// so fallback cannot duplicate spans or bypass normal fetch admission limits.
func safeIndexPosition(ctx context.Context, dir string, segment logSegment, offset int64, checkpoint *indexCheckpoint) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	position, base, err := indexPosition(dir, segment, offset)
	if err == nil && position == 0 && base == segment.baseOffset {
		return position, base, nil
	}
	if err == nil {
		matches, checkErr := indexMatchesBatch(ctx, dir, segment, position, base, checkpoint)
		if checkErr != nil {
			return 0, 0, checkErr
		}
		if matches {
			return position, base, nil
		}
	}
	discardOffsetIndex(offsetIndexPath(dir, segment))
	return 0, segment.baseOffset, nil
}

// One proven log boundary keeps forward fetches efficient without retaining a
// map of every segment or trusting cache bytes. Callers hold the partition lock.
type indexCheckpoint struct {
	path             string
	position, offset int64
	file             os.FileInfo
}

func indexMatchesBatch(ctx context.Context, dir string, segment logSegment, position, base int64, checkpoint *indexCheckpoint) (bool, error) {
	path := filepath.Join(dir, segment.name)
	f, err := os.Open(path)
	if err != nil {
		return false, nil
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, nil
	}
	if position < 0 || position > info.Size() {
		return false, nil
	}
	walk, next := int64(0), segment.baseOffset
	if checkpoint != nil && checkpoint.path == path && checkpoint.file != nil &&
		os.SameFile(checkpoint.file, info) && checkpoint.file.Size() <= info.Size() &&
		checkpoint.position <= position && checkpoint.offset <= base {
		walk, next = checkpoint.position, checkpoint.offset
	}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var header [27]byte
		if _, err := f.ReadAt(header[:], walk); err != nil {
			return false, nil
		}
		length := int64(int32(binary.BigEndian.Uint32(header[8:12])))
		delta := int64(int32(binary.BigEndian.Uint32(header[23:27])))
		if int64(binary.BigEndian.Uint64(header[:8])) != next || header[16] != 2 ||
			length < 49 || length > storage.MaxRecordBatchBytes-12 || delta < 0 ||
			next > math.MaxInt64-delta-1 || length+12 > info.Size()-walk {
			return false, nil
		}
		if walk == position {
			if next != base {
				return false, nil
			}
			if checkpoint != nil {
				*checkpoint = indexCheckpoint{path: path, position: walk, offset: next, file: info}
			}
			return true, nil
		}
		if length+12 > position-walk {
			// A plausible header inside a value is not a log batch boundary.
			return false, nil
		}
		walk += length + 12
		next += delta + 1
	}
}
