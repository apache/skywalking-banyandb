// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// mergeSpillThreshold bounds how many bytes one staged merge section keeps in
// memory before it moves to a spill file.
var mergeSpillThreshold = 4 << 20

// spillFactory names the spill files of one merge. An empty prefix disables
// spilling, keeping every staged section in memory.
type spillFactory struct {
	prefix   string
	sequence int
}

func (f *spillFactory) newBuffer() *spillBuffer {
	return &spillBuffer{factory: f}
}

func (f *spillFactory) nextPath() string {
	f.sequence++
	return fmt.Sprintf("%s.spill-%d", f.prefix, f.sequence)
}

// spillBuffer stages a merge section whose length must be written before its
// bytes. It holds up to mergeSpillThreshold bytes in memory and appends the
// rest to a spill file, so a staged section costs bounded memory however
// large it grows. A spill file lives only until the buffer is reset or
// closed.
type spillBuffer struct {
	factory *spillFactory
	file    fs.File
	writer  fs.SeqWriter
	path    string
	memory  []byte
	size    uint64
}

func (b *spillBuffer) Write(data []byte) (int, error) {
	if b.writer == nil && b.factory.prefix != "" && len(b.memory)+len(data) > mergeSpillThreshold {
		if spillErr := b.startSpill(); spillErr != nil {
			return 0, spillErr
		}
	}
	if b.writer != nil {
		written, writeErr := b.writer.Write(data)
		b.size += uint64(written)
		return written, writeErr
	}
	b.memory = append(b.memory, data...)
	b.size += uint64(len(data))
	return len(data), nil
}

func (b *spillBuffer) startSpill() error {
	path := b.factory.nextPath()
	file, createErr := segmentFileSystem.CreateFile(path, 0o600)
	if createErr != nil {
		return fmt.Errorf("create merge spill file: %w", createErr)
	}
	// The spill file is read back moments later and deleted; keep its pages.
	fs.SetCached(file, true)
	b.file, b.path = file, path
	b.writer = file.SequentialWrite()
	if _, writeErr := b.writer.Write(b.memory); writeErr != nil {
		return fmt.Errorf("write merge spill file: %w", writeErr)
	}
	b.memory = b.memory[:0]
	return nil
}

// Len returns the number of staged bytes.
func (b *spillBuffer) Len() uint64 {
	return b.size
}

// copyTo writes every staged byte to destination in order.
func (b *spillBuffer) copyTo(destination io.Writer) error {
	if b.writer == nil {
		_, writeErr := destination.Write(b.memory)
		return writeErr
	}
	writer := b.writer
	b.writer = nil
	if closeErr := writer.Close(); closeErr != nil {
		return fmt.Errorf("flush merge spill file: %w", closeErr)
	}
	reader := b.file.SequentialRead()
	copied, copyErr := io.Copy(destination, reader)
	copyErr = errors.Join(copyErr, reader.Close())
	if copyErr == nil && uint64(copied) != b.size {
		copyErr = fmt.Errorf("merge spill file %q holds %d bytes, want %d: %w", b.path, copied, b.size, io.ErrUnexpectedEOF)
	}
	return copyErr
}

// reset discards the staged bytes, removing any spill file, and keeps the
// in-memory capacity for the next section.
func (b *spillBuffer) reset() error {
	resetErr := b.closeFile()
	b.memory = b.memory[:0]
	b.size = 0
	return resetErr
}

func (b *spillBuffer) closeFile() error {
	var closeErr error
	if b.writer != nil {
		closeErr = b.writer.Close()
		b.writer = nil
	}
	if b.file != nil {
		closeErr = errors.Join(closeErr, b.file.Close(), segmentFileSystem.DeleteFile(b.path))
		b.file, b.path = nil, ""
	}
	return closeErr
}
