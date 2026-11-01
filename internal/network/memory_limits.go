package network

import (
	"fmt"
	"math"

	"github.com/ritiraj/kafka-go/internal/protocol"
	"github.com/ritiraj/kafka-go/internal/storage"
)

type MemoryLimits struct {
	MaxDecodedBytes int64
	DecodedBudget   int64
	MaxEntries      int
	FetchMaxBytes   int
	FetchBudget     int64
}

func DefaultMemoryLimits() MemoryLimits {
	return MemoryLimits{MaxDecodedBytes: 16 << 20, DecodedBudget: 128 << 20, MaxEntries: 10000, FetchMaxBytes: 16 << 20, FetchBudget: 512 << 20}
}

func (m MemoryLimits) Validate() error {
	if m.MaxDecodedBytes < 1024 || m.MaxDecodedBytes > protocol.MaxRequestSize || m.DecodedBudget < m.MaxDecodedBytes ||
		m.MaxEntries < 1 || m.MaxEntries > 100000 || m.FetchMaxBytes < 1 || m.FetchMaxBytes > storage.MaxRecordBatchBytes {
		return fmt.Errorf("invalid decoder/fetch limits")
	}
	// Covers request-derived response headers and bounded planning structures,
	// plus both record and encoded-body buffers for the first-batch exception.
	overhead := int64(2048) + int64(m.MaxEntries)*1152 + m.MaxDecodedBytes
	if overhead > math.MaxInt32-storage.MaxRecordBatchBytes || m.FetchBudget < 2*storage.MaxRecordBatchBytes+overhead {
		return fmt.Errorf("fetch budget must cover a full %d-byte batch, encoding and planning overhead", storage.MaxRecordBatchBytes)
	}
	return nil
}
