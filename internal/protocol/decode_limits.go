package protocol

import (
	"errors"
	"fmt"
)

var ErrDecodeLimit = errors.New("decoded request limit exceeded")

type DecodeLimits struct {
	MaxBytes   int64
	MaxEntries int
}

func DefaultDecodeLimits() DecodeLimits { return DecodeLimits{MaxBytes: 16 << 20, MaxEntries: 10000} }

// DecodeAllowance reserves a conservative expansion allowance before parsing.
// Large record payloads are borrowed from the separately charged raw frame.
func DecodeAllowance(frameBytes int64, limit int64) int64 {
	return min(limit, 1024+128*frameBytes)
}

type decodeBudget struct {
	limits  DecodeLimits
	bytes   int64
	entries int
}

func NewDecoderWithLimits(data []byte, limits DecodeLimits) (*Decoder, error) {
	if limits.MaxBytes < 1024 || limits.MaxEntries < 1 {
		return nil, fmt.Errorf("invalid decoder limits")
	}
	return &Decoder{data: data, budget: &decodeBudget{limits: limits, bytes: 1024}}, nil
}

func (d *Decoder) chargeBytes(n int) error {
	if d.budget == nil {
		return nil
	}
	cost := 2*int64(n) + 64
	if cost > d.budget.limits.MaxBytes-d.budget.bytes {
		return ErrDecodeLimit
	}
	d.budget.bytes += cost
	return nil
}

func (d *Decoder) ChargeEntries(n int64) error {
	if d.budget == nil {
		return nil
	}
	if n < 0 || n > int64(d.budget.limits.MaxEntries-d.budget.entries) {
		return ErrDecodeLimit
	}
	cost := n * 256
	if cost > d.budget.limits.MaxBytes-d.budget.bytes {
		return ErrDecodeLimit
	}
	d.budget.entries += int(n)
	d.budget.bytes += cost
	return nil
}

func (r *RequestHeader) SetDecodeLimits(limits DecodeLimits) { r.decodeLimits = &limits }
func (r *RequestHeader) DecodedBytes() int64 {
	if r.decoded == nil {
		return 1024
	}
	return r.decoded.bytes
}
