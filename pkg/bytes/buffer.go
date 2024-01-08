// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package bytes provides utilities for operating bytes.
package bytes

import (
	"fmt"
	"io"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

var (
	_ fs.Writer = (*Buffer)(nil)
	_ fs.Reader = (*Buffer)(nil)
)

// Buffer is a in-memory buffer.
type Buffer struct {
	Buf []byte
}

// Close implements fs.Writer.
func (*Buffer) Close() error {
	return nil
}

// Path implements fs.Writer.
func (b *Buffer) Path() string {
	return fmt.Sprintf("mem/%p", b)
}

// Write implements fs.Writer.
func (b *Buffer) Write(bb []byte) (int, error) {
	b.Buf = append(b.Buf, bb...)
	return len(bb), nil
}

// Read implements fs.Reader.
func (b *Buffer) Read(offset int64, buffer []byte) (int, error) {
	var err error
	n := copy(buffer, b.Buf[offset:])
	if n < len(buffer) {
		err = io.EOF
	}
	return n, err
}

func (b *Buffer) StreamRead() io.Reader {
	return &reader{bb: b}
}

// Reset resets the buffer.
func (b *Buffer) Reset() {
	b.Buf = b.Buf[:0]
}

type reader struct {
	bb *Buffer

	readOffset int
}

func (r *reader) Path() string {
	return r.bb.Path()
}

func (r *reader) Read(p []byte) (int, error) {
	var err error
	n := copy(p, r.bb.Buf[r.readOffset:])
	if n < len(p) {
		err = io.EOF
	}
	r.readOffset += n
	return n, err
}

func (r *reader) MustClose() {
	r.bb = nil
	r.readOffset = 0
}

// BufferPool is a pool of Buffer.
type BufferPool struct {
	p sync.Pool
}

// Generate generates a Buffer.
func (bp *BufferPool) Generate() *Buffer {
	bbv := bp.p.Get()
	if bbv == nil {
		return &Buffer{}
	}
	return bbv.(*Buffer)
}

// Release releases a Buffer.
func (bp *BufferPool) Release(b *Buffer) {
	b.Reset()
	bp.p.Put(b)
}
