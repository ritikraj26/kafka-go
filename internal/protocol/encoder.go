package protocol

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ritiraj/kafka-go/internal/logger"
	"github.com/ritiraj/kafka-go/internal/memory"
)

type Encoder struct {
	buf         []byte
	fixed       bool
	err         error
	limit       int
	ctx         context.Context
	pool        *memory.Pool
	reservation *memory.Reservation
	discard     bool
}

func NewEncoder() *Encoder { return &Encoder{} }

const DefaultResponseBytes = 16 << 20

func NewResponseEncoder(ctx context.Context, pool *memory.Pool, limit int) *Encoder {
	if limit < 1 {
		return &Encoder{err: fmt.Errorf("response byte limit must be positive")}
	}
	return &Encoder{ctx: ctx, pool: pool, limit: limit}
}

func ResponseEncoder(encoders []*Encoder) *Encoder {
	if len(encoders) == 0 {
		return NewResponseEncoder(context.Background(), nil, DefaultResponseBytes)
	}
	if len(encoders) != 1 || encoders[0] == nil {
		return &Encoder{err: fmt.Errorf("invalid response encoder")}
	}
	return encoders[0]
}

func NewDiscardEncoder() *Encoder { return &Encoder{discard: true} }

func (e *Encoder) Release() {
	e.buf = nil
	if e.reservation != nil {
		e.reservation.Release()
	}
}

// NewFixedEncoder never grows beyond its reserved backing-buffer capacity.
func NewFixedEncoder(capacity int) *Encoder {
	if capacity < 0 {
		return &Encoder{fixed: true, err: fmt.Errorf("negative encoder capacity")}
	}
	return &Encoder{buf: make([]byte, 0, capacity), fixed: true}
}

func (e *Encoder) room(n int) bool {
	if e.discard {
		return false
	}
	if e.err != nil {
		return false
	}
	if n < 0 || (e.fixed && n > cap(e.buf)-len(e.buf)) {
		e.err = fmt.Errorf("response exceeds reserved encoder capacity")
		return false
	}
	if e.limit > 0 {
		if err := e.ctx.Err(); err != nil {
			e.err = err
			return false
		}
		if n > e.limit-len(e.buf) {
			e.err = fmt.Errorf("non-fetch response exceeds %d-byte limit", e.limit)
			return false
		}
		if n > cap(e.buf)-len(e.buf) {
			capacity := min(e.limit, max(len(e.buf)+n, max(4096, 2*cap(e.buf))))
			if e.pool != nil {
				if e.reservation == nil {
					var err error
					e.reservation, err = e.pool.Acquire(e.ctx, int64(capacity))
					if err != nil {
						e.err = err
						return false
					}
				} else if !e.reservation.Resize(int64(capacity) + int64(cap(e.buf))) {
					e.err = fmt.Errorf("non-fetch response growth exceeds available capacity")
					return false
				}
			}
			buf := make([]byte, len(e.buf), capacity)
			copy(buf, e.buf)
			e.buf = buf
			if e.reservation != nil {
				e.reservation.Resize(int64(capacity))
			}
		}
	}
	return true
}

func (e *Encoder) WriteInt16(v int16) {
	if e.room(2) {
		e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v))
	}
}
func (e *Encoder) WriteInt32(v int32) {
	if e.room(4) {
		e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v))
	}
}
func (e *Encoder) WriteInt64(v int64) {
	if e.room(8) {
		e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(v))
	}
}
func (e *Encoder) WriteByte(v byte) error {
	if e.room(1) {
		e.buf = append(e.buf, v)
	}
	return e.err
}
func (e *Encoder) WriteCompactString(s string) {
	e.WriteUnsignedVarint(uint64(len(s) + 1))
	if e.room(len(s)) {
		e.buf = append(e.buf, s...)
	}
}
func (e *Encoder) WriteString(s string) {
	e.WriteInt16(int16(len(s)))
	if e.room(len(s)) {
		e.buf = append(e.buf, s...)
	}
}
func (e *Encoder) WriteNullableString(s *string) {
	if s == nil {
		e.WriteInt16(-1)
	} else {
		e.WriteString(*s)
	}
}
func (e *Encoder) WriteBytes(data []byte) {
	if data == nil {
		e.WriteInt32(-1)
		return
	}
	e.WriteInt32(int32(len(data)))
	if e.room(len(data)) {
		e.buf = append(e.buf, data...)
	}
}
func (e *Encoder) WriteArrayLen(n int) { e.WriteInt32(int32(n)) }
func (e *Encoder) WriteCompactBytes(data []byte) {
	e.WriteUnsignedVarint(uint64(len(data) + 1))
	if e.room(len(data)) {
		e.buf = append(e.buf, data...)
	}
}
func (e *Encoder) WriteUnsignedVarint(v uint64) {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], v)
	if e.room(n) {
		e.buf = append(e.buf, scratch[:n]...)
	}
}
func (e *Encoder) WriteUUID(id [16]byte) {
	if e.room(16) {
		e.buf = append(e.buf, id[:]...)
	}
}
func (e *Encoder) WriteTagBuffer() { e.WriteByte(0) }
func (e *Encoder) Bytes() []byte {
	if e.err != nil {
		logger.L.Warn("response encoding failed", "err", e.err)
		return nil
	}
	return e.buf
}
func (e *Encoder) Err() error { return e.err }
