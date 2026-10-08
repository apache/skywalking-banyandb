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

// unlinkSpillOnCreate reports whether a spill file's name is removed as soon
// as the file is created; see fs.OpenFileNamesMutable. A variable so tests can
// exercise the close-then-delete path on any platform.
var unlinkSpillOnCreate = fs.OpenFileNamesMutable

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
// large it grows. A spill file lives only until the buffer is reset, and
// where the platform allows it without a name (see startSpill).
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
	// Where an open file can be unlinked, the name is removed at once: the
	// open file stays readable and writable, and a crash can then leave
	// nothing behind in the index directory, where the previous release's
	// engine would never clean it. Windows refuses to unlink an open file,
	// so there the name is removed once the file is closed (closeFile), and
	// a crash can leave it for the next owner start to remove.
	if unlinkSpillOnCreate {
		if deleteErr := segmentFileSystem.DeleteFile(path); deleteErr != nil {
			return errors.Join(fmt.Errorf("unlink merge spill file: %w", deleteErr), file.Close())
		}
	}
	// The spill file is read back moments later; keep its pages.
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
	// The spill is never read again; its pages need not wait for the
	// file's removal to leave the page cache.
	_ = fs.AdvisePageCache(b.file, fs.PageCacheDontNeed)
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
		closeErr = errors.Join(closeErr, b.file.Close())
		if !unlinkSpillOnCreate {
			closeErr = errors.Join(closeErr, segmentFileSystem.DeleteFile(b.path))
		}
		b.file, b.path = nil, ""
	}
	return closeErr
}
