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

package bnc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var errClosed = errors.New("bnc writer is closed")

// Writer writes one container. Its first error sticks: every later call returns it, and a
// closed Writer fails every call.
type Writer struct {
	w          io.Writer
	err        error
	paths      *pathSet
	entries    []Entry
	buf        []byte
	off        int64
	footerSize int // entry count and entries; headerSize(seq) comes on top
	fileCRC    uint32
	seq        uint32 // volume number written into the footer
}

// NewWriter returns a Writer that writes a container to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, paths: newPathSet(), footerSize: uvarintLen(0), seq: 1}
}

// Add copies exactly size bytes of r as member path, checking ctx before each block. It reads r
// until io.EOF, so r must end: a shorter or longer r is an error, because snapshot files are
// frozen and a mismatch means the source changed.
// Errors of r keep their identity, and a canceled ctx returns ctx.Err() unwrapped.
func (w *Writer) Add(ctx context.Context, path string, size int64, r io.Reader) error {
	return w.addPiece(ctx, path, 0, size, size, r)
}

// addPiece copies exactly size bytes of r as the piece of file path that starts at fileOffset
// in a file of fileSize bytes. A piece that continues a file must be the first member, and
// nothing follows a piece that leaves its file unfinished.
func (w *Writer) addPiece(ctx context.Context, path string, fileOffset, size, fileSize int64, r io.Reader) error {
	if w.err != nil {
		return w.err
	}
	if size < 0 || fileOffset < 0 {
		return w.fail(fmt.Errorf("member %q has a negative size %d or offset %d", path, size, fileOffset))
	}
	// The rules Open applies to a volume; whether this member ends the volume is not known yet.
	if err := checkPiece(path, uint64(fileOffset), uint64(size), uint64(fileSize), len(w.entries) == 0, true); err != nil {
		return w.fail(err)
	}
	if n := len(w.entries); n > 0 && !w.entries[n-1].ends() {
		return w.fail(fmt.Errorf("member %q follows %q, which is unfinished", path, w.entries[n-1].Path))
	}
	if len(w.entries) >= maxEntries {
		return w.fail(fmt.Errorf("a container holds at most %d members", maxEntries))
	}
	footerSize := w.footerSize + encodedEntrySize(path, fileOffset, fileSize) + uvarintLen(uint64(len(w.entries)+1)) - uvarintLen(uint64(len(w.entries)))
	if footerSize+headerSize(w.seq) > maxFooterSize {
		return w.fail(fmt.Errorf("member %q would grow the footer beyond %d bytes", path, maxFooterSize))
	}
	if err := w.paths.add(path); err != nil {
		return w.fail(err)
	}
	if w.buf == nil {
		w.buf = make([]byte, WriteSize)
	}
	var crc uint32
	var copied int64
	for {
		if err := ctx.Err(); err != nil {
			return w.fail(err)
		}
		n, err := r.Read(w.buf)
		if n > 0 {
			if int64(n) > size-copied {
				return w.fail(fmt.Errorf("member %q is longer than its size %d", path, size))
			}
			crc = crc32.Update(crc, crc32.IEEETable, w.buf[:n])
			if werr := w.write(w.buf[:n]); werr != nil {
				return werr
			}
			copied += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return w.fail(fmt.Errorf("read member %q: %w", path, err))
		}
	}
	if copied != size {
		return w.fail(fmt.Errorf("member %q has %d bytes, not its size %d", path, copied, size))
	}
	w.entries = append(w.entries, Entry{Path: path, Offset: w.off - size, Size: size, FileOffset: fileOffset, FileSize: fileSize, CRC32: crc})
	w.footerSize = footerSize
	return nil
}

// Close appends the footer, its CRC-32, its length and the magic, and returns the totals of
// the container. A container needs at least one member. A container a Writer writes alone is
// volume 1, and the last one.
func (w *Writer) Close() (Summary, error) {
	return w.close(true)
}

// close finishes the container as volume w.seq, last telling whether it ends the container.
func (w *Writer) close(last bool) (Summary, error) {
	if w.err != nil {
		return Summary{}, w.err
	}
	if len(w.entries) == 0 {
		return Summary{}, w.fail(errors.New("a container needs at least one member"))
	}
	footer := encodeFooter(w.seq, last, w.entries, headerSize(w.seq)+w.footerSize)
	trailer := make([]byte, 0, trailerSize)
	trailer = binary.LittleEndian.AppendUint32(trailer, crc32.ChecksumIEEE(footer))
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(footer)))
	trailer = append(trailer, magic...)
	if err := w.write(footer); err != nil {
		return Summary{}, err
	}
	if err := w.write(trailer); err != nil {
		return Summary{}, err
	}
	w.err = errClosed
	return Summary{Bytes: uint64(w.off), CRC32: w.fileCRC, Entries: uint32(len(w.entries))}, nil
}

// write checksums p before handing it over: a Sink may reuse p once Write returns.
func (w *Writer) write(p []byte) error {
	crc := crc32.Update(w.fileCRC, crc32.IEEETable, p)
	n, err := w.w.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return w.fail(err)
	}
	w.fileCRC = crc
	w.off += int64(n)
	return nil
}

func (w *Writer) fail(err error) error {
	w.err = err
	return err
}
