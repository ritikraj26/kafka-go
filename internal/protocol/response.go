package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Response struct {
	correlationID int32
	body          []byte
	headerVersion int
}

func NewResponse(correlationID int32, body []byte) *Response {
	return &Response{correlationID: correlationID, body: body}
}

func NewResponseV1(correlationID int32, body []byte) *Response {
	return &Response{correlationID: correlationID, body: body, headerVersion: 1}
}

func (r *Response) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	if _, err := r.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteTo avoids a second full-size response buffer. The caller retains its
// memory reservation until both the header and existing body have been sent.
func (r *Response) WriteTo(w io.Writer) (int64, error) {
	headerBytes := 8 + r.headerVersion
	if int64(len(r.body))+int64(headerBytes-4) > math.MaxInt32 {
		return 0, fmt.Errorf("response frame too large")
	}
	var header [9]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(r.body)+headerBytes-4))
	binary.BigEndian.PutUint32(header[4:8], uint32(r.correlationID))
	n, err := w.Write(header[:headerBytes])
	total := int64(n)
	if err != nil {
		return total, err
	}
	if n != headerBytes {
		return total, io.ErrShortWrite
	}
	n, err = w.Write(r.body)
	total += int64(n)
	if err == nil && n != len(r.body) {
		err = io.ErrShortWrite
	}
	return total, err
}
