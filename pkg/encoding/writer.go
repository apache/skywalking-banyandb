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

package encoding

import (
	"encoding/binary"
)

// Writer writes bits to an io.BufferWriter.
type Writer struct {
	out       BufferWriter
	cache     byte
	available byte
}

// NewWriter create bit writer.
func NewWriter() *Writer {
	return new(Writer)
}

// Reset writes to a new writer.
func (w *Writer) Reset(buffer BufferWriter) {
	w.out = buffer
	w.cache = 0
	w.available = 8
}

// WriteBool writes a boolean value
// true: 1
// false: 0
func (w *Writer) WriteBool(b bool) {
	if b {
		w.cache |= 1 << (w.available - 1)
	}

	w.available--

	if w.available == 0 {
		// WriteByte never returns error
		_ = w.out.WriteByte(w.cache)
		w.cache = 0
		w.available = 8
	}
}

// WriteBits writes number of bits.
func (w *Writer) WriteBits(u uint64, numBits int) {
	u <<= 64 - uint(numBits)

	for ; numBits >= 8; numBits -= 8 {
		byt := byte(u >> 56)
		_ = w.WriteByte(byt)
		u <<= 8
	}

	remainder := byte(u >> 56)
	for ; numBits > 0; numBits-- {
		w.WriteBool((remainder & 0x80) != 0)
		remainder <<= 1
	}
}

// WriteByte write a byte.
func (w *Writer) WriteByte(b byte) error {
	if err := w.out.WriteByte(w.cache | (b >> (8 - w.available))); err != nil {
		return err
	}
	w.cache = b << w.available
	return nil
}

// Flush flushes the currently in-process byte.
func (w *Writer) Flush() {
	if w.available != 8 {
		_ = w.out.WriteByte(w.cache)
	}
	w.Reset(w.out)
}

// Packer writes data into a buffer.
type Packer struct {
	buf BufferWriter

	scratch [binary.MaxVarintLen64]byte
}

// NewPacker returns a new Writer.
func NewPacker(buf BufferWriter) *Packer {
	return &Packer{
		buf: buf,
	}
}

// Write binaries to the buffer.
func (w *Packer) Write(p []byte) {
	_, _ = w.buf.Write(p)
}

// PutUint16 puts uint16 data into the buffer.
func (w *Packer) PutUint16(v uint16) {
	binary.LittleEndian.PutUint16(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:2])
}

// PutUint32 puts uint32 data into the buffer.
func (w *Packer) PutUint32(v uint32) {
	binary.LittleEndian.PutUint32(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:4])
}

// PutUint64 puts uint64 data into the buffer.
func (w *Packer) PutUint64(v uint64) {
	binary.LittleEndian.PutUint64(w.scratch[:], v)
	_, _ = w.buf.Write(w.scratch[:8])
}

// Bytes outputs the data in the buffer.
func (w *Packer) Bytes() []byte {
	return w.buf.Bytes()
}
