// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// TODO:// move this into package encoding.

// Package buffer implements a custom buffer based on a reusable binary array.
package buffer

import (
	"bytes"
	"encoding/binary"
)

// Writer writes data into a buffer.
type Writer struct {
	buf *bytes.Buffer

	scratch [binary.MaxVarintLen64]byte
}

// NewBufferWriter returns a new Writer.
func NewBufferWriter(buf *bytes.Buffer) *Writer {
	return &Writer{
		buf: buf,
	}
}

// Write binaries to the buffer.
func (w *Writer) Write(p []byte) {
	_, _ = w.buf.Write(p)
}

// WriteTo writes data to another Writer.
func (w *Writer) WriteTo(other *Writer) (n int64) {
	n, _ = w.buf.WriteTo(other.buf)
	return n
}

// PutUint16 puts uint16 data into the buffer.
func (w *Writer) PutUint16(v uint16) {
	binary.LittleEndian.PutUint16(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:2])
}

// PutUint32 puts uint32 data into the buffer.
func (w *Writer) PutUint32(v uint32) {
	binary.LittleEndian.PutUint32(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:4])
}

// PutUint64 puts uint64 data into the buffer.
func (w *Writer) PutUint64(v uint64) {
	binary.LittleEndian.PutUint64(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:8])
}

// Reset cleans up the buffer.
func (w *Writer) Reset() {
	w.buf.Reset()
}

// Len returns the length of the buffer.
func (w *Writer) Len() int {
	return w.buf.Len()
}

// Bytes outputs the data in the buffer.
func (w *Writer) Bytes() []byte {
	return w.buf.Bytes()
}
