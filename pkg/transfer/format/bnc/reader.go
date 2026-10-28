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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"strings"
)

// verifyBlock is the read size of Verify.
const verifyBlock = 1 << 20

// Container is one opened volume whose structure has been checked. Its methods may be called
// concurrently. Files are read through a VolumeSet, which joins the pieces of a file split
// across volumes.
type Container struct {
	r       io.ReaderAt
	closer  io.Closer
	entries []Entry
	tail    []byte // footer and trailer as Open read them
	size    int64
}

// Open checks the structure of the container of size bytes in r with two ReadAt calls, without
// reading the members: the trailer, the footer and its CRC-32, every footer bound, and every
// member path. A truncated container is detected only when its tail no longer parses; the
// size and CRC-32 a manifest records are the final word.
func Open(r io.ReaderAt, size int64) (*Container, error) {
	if size < trailerSize {
		return nil, corruptf("%d bytes are too few for a container", size)
	}
	trailer := make([]byte, trailerSize)
	if err := readFullAt(r, trailer, size-trailerSize); err != nil {
		return nil, err
	}
	if m := string(trailer[8:]); m != magic {
		if strings.HasPrefix(m, magic[:3]) && m[3] >= 'B' && m[3] <= 'Z' {
			return nil, fmt.Errorf("%w: format version %q", ErrUnsupportedVersion, m[3:])
		}
		return nil, corruptf("bad magic %q", m)
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[4:8]))
	if footerLen > size-trailerSize || footerLen > maxFooterSize {
		return nil, corruptf("footer length %d does not fit a container of %d bytes", footerLen, size)
	}
	membersEnd := size - trailerSize - footerLen
	tail := make([]byte, footerLen+trailerSize)
	if err := readFullAt(r, tail[:footerLen], membersEnd); err != nil {
		return nil, err
	}
	copy(tail[footerLen:], trailer)
	footer := tail[:footerLen]
	if crc32.ChecksumIEEE(footer) != binary.LittleEndian.Uint32(trailer) {
		return nil, corruptf("footer checksum mismatch")
	}
	entries, err := decodeFooter(footer, membersEnd)
	if err != nil {
		return nil, err
	}
	return &Container{r: r, size: size, entries: entries, tail: tail}, nil
}

// OpenFile opens and checks the container in the regular file at path, sizing it on the open
// descriptor. Close releases the file.
func OpenFile(path string) (*Container, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		err = fmt.Errorf("%s is not a regular file", path)
	}
	var c *Container
	if err == nil {
		c, err = Open(f, st.Size())
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open container %s: %w", path, err)
	}
	c.closer = f
	return c, nil
}

// Close closes the file of a Container from OpenFile; it does nothing for one from Open.
func (c *Container) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

// Entries returns a copy of the members in footer order.
func (c *Container) Entries() []Entry {
	return slices.Clone(c.entries)
}

// member returns a reader of the bytes of e, an entry of c. Reading it to the end returns
// ErrChecksum instead of io.EOF when the bytes do not match, and ErrCorrupt when the container
// ends early. Each Read is one ReadAt, so read with large buffers.
func (c *Container) member(e Entry) io.Reader {
	return &memberReader{r: io.NewSectionReader(c.r, e.Offset, e.Size), path: e.Path, want: e.CRC32, left: e.Size}
}

type memberReader struct {
	r    io.Reader
	path string
	left int64
	crc  uint32
	want uint32
}

func (m *memberReader) Read(p []byte) (int, error) {
	if m.left == 0 {
		if m.crc != m.want {
			return 0, fmt.Errorf("%w: %q", ErrChecksum, m.path)
		}
		return 0, io.EOF
	}
	n, err := m.r.Read(p)
	m.crc = crc32.Update(m.crc, crc32.IEEETable, p[:n])
	m.left -= int64(n)
	if errors.Is(err, io.EOF) {
		if m.left > 0 {
			return n, corruptf("member %q ends %d bytes early", m.path, m.left)
		}
		err = nil
	}
	if err != nil {
		return n, fmt.Errorf("read member %q: %w", m.path, err)
	}
	return n, nil
}

// Verify reads the whole container once: it checks the CRC-32 of every member, checks that the
// footer and trailer still hold what Open read, and returns the size and CRC-32 of the file. A
// canceled ctx returns ctx.Err() unwrapped.
func (c *Container) Verify(ctx context.Context) (Summary, error) {
	buf := make([]byte, min(verifyBlock, c.size))
	var fileCRC, memberCRC uint32
	membersEnd := c.size - int64(len(c.tail))
	next, left := c.nextMember(-1)
	for pos := int64(0); pos < c.size; {
		if err := ctx.Err(); err != nil {
			return Summary{}, err
		}
		chunk := buf[:min(int64(len(buf)), c.size-pos)]
		if err := readFullAt(c.r, chunk, pos); err != nil {
			return Summary{}, err
		}
		fileCRC = crc32.Update(fileCRC, crc32.IEEETable, chunk)
		for p := chunk; len(p) > 0; {
			if pos >= membersEnd {
				if !bytes.Equal(p, c.tail[pos-membersEnd:pos-membersEnd+int64(len(p))]) {
					return Summary{}, corruptf("footer changed since the container was opened")
				}
				pos += int64(len(p))
				break
			}
			n := min(int64(len(p)), left)
			memberCRC = crc32.Update(memberCRC, crc32.IEEETable, p[:n])
			p, pos, left = p[n:], pos+n, left-n
			if left == 0 {
				if memberCRC != c.entries[next].CRC32 {
					return Summary{}, fmt.Errorf("%w: %q", ErrChecksum, c.entries[next].Path)
				}
				memberCRC = 0
				next, left = c.nextMember(next)
			}
		}
	}
	return Summary{Bytes: uint64(c.size), CRC32: fileCRC, Entries: uint32(len(c.entries))}, nil
}

// nextMember returns the member with bytes after i and its size, or -1 and 0 when there is none.
// Zero-length members need no reading: Open already required their CRC-32 to be 0.
func (c *Container) nextMember(i int) (int, int64) {
	for i++; i < len(c.entries); i++ {
		if c.entries[i].Size > 0 {
			return i, c.entries[i].Size
		}
	}
	return -1, 0
}

// readFullAt fills buf from r at off; a container that ends early is ErrCorrupt.
func readFullAt(r io.ReaderAt, buf []byte, off int64) error {
	n, err := r.ReadAt(buf, off)
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return corruptf("container ends at %d, before %d", off+int64(n), off+int64(len(buf)))
	}
	return fmt.Errorf("read container at %d: %w", off, err)
}
