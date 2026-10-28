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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"unicode/utf8"
)

const (
	// maxFooterSize bounds the encoded footer of one container.
	maxFooterSize = 16 << 20
	// maxEntries bounds the members of one container.
	maxEntries = 1 << 20
	// WriteSize is the largest slice a Sink.Write call receives.
	WriteSize = 256 << 10

	// magic ends with the format version letter.
	magic = "BNCA"
	// trailerSize covers the footer CRC-32, the footer length and the magic.
	trailerSize = 12
	// entryFixedSize is the member size and CRC-32 of a footer entry.
	entryFixedSize = 12
	// minEntrySize is the smallest encoded entry: a one-byte path length, a one-byte path, the
	// fixed fields and one-byte file offset and file size.
	minEntrySize    = 2 + entryFixedSize + 2
	maxPathLen      = 4096
	maxComponentLen = 255
)

var (
	// ErrCorrupt reports a container that does not follow the format.
	ErrCorrupt = errors.New("corrupt bnc container")
	// ErrUnsupportedVersion reports a container of a format version this code cannot read.
	ErrUnsupportedVersion = errors.New("unsupported bnc container version")
	// ErrChecksum reports member bytes that do not match their CRC-32; it is an ErrCorrupt.
	ErrChecksum = fmt.Errorf("%w: member checksum mismatch", ErrCorrupt)
)

// Entry is one member of a container as its footer describes it: a whole file, or one piece
// of a file split across consecutive volumes. Offset is where its bytes start in the
// container, derived from the sizes of the members before it; Size and CRC32 cover those
// bytes. FileOffset is where they start in the file and FileSize is the size of the whole
// file, so a whole file has FileOffset 0 and FileSize equal to Size.
type Entry struct {
	Path       string
	Offset     int64
	Size       int64
	FileOffset int64
	FileSize   int64
	CRC32      uint32
}

// ends tells whether e holds the last bytes of its file.
func (e Entry) ends() bool { return e.FileOffset+e.Size == e.FileSize }

// Summary describes a whole container file in the types of manifest.File and FileFinished.
type Summary struct {
	Bytes   uint64
	CRC32   uint32
	Entries uint32
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}

// encodedEntrySize is the footer bytes an entry of path takes when it starts at fileOffset in a
// file of fileSize bytes.
func encodedEntrySize(p string, fileOffset, fileSize int64) int {
	return uvarintLen(uint64(len(p))) + len(p) + entryFixedSize + uvarintLen(uint64(fileOffset)) + uvarintLen(uint64(fileSize))
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

// encodeFooter encodes the footer of entries, whose sizes the caller has checked.
func encodeFooter(entries []Entry, size int) []byte {
	b := make([]byte, 0, size)
	b = binary.AppendUvarint(b, uint64(len(entries)))
	for _, e := range entries {
		b = binary.AppendUvarint(b, uint64(len(e.Path)))
		b = append(b, e.Path...)
		b = binary.LittleEndian.AppendUint64(b, uint64(e.Size))
		b = binary.LittleEndian.AppendUint32(b, e.CRC32)
		b = binary.AppendUvarint(b, uint64(e.FileOffset))
		b = binary.AppendUvarint(b, uint64(e.FileSize))
	}
	return b
}

// decodeFooter decodes footer b of a container whose members take membersLen bytes. It checks
// every bound before it allocates, so a small footer cannot claim a large allocation.
func decodeFooter(b []byte, membersLen int64) ([]Entry, error) {
	count, n := readUvarint(b)
	if n <= 0 {
		return nil, corruptf("footer entry count is truncated or not minimally encoded")
	}
	b = b[n:]
	if count == 0 {
		return nil, corruptf("a container needs at least one member")
	}
	if count > maxEntries {
		return nil, corruptf("footer claims %d entries, more than %d", count, maxEntries)
	}
	if count > uint64(len(b))/minEntrySize {
		return nil, corruptf("footer claims %d entries in %d bytes", count, len(b))
	}
	entries := make([]Entry, 0, count)
	paths := newPathSet()
	var off int64
	for i := uint64(0); i < count; i++ {
		l, n := readUvarint(b)
		if n <= 0 {
			return nil, corruptf("footer entry %d: path length is truncated or not minimally encoded", i)
		}
		b = b[n:]
		if l > maxPathLen || l > uint64(len(b)) {
			return nil, corruptf("footer entry %d: path length %d exceeds the footer", i, l)
		}
		p := string(b[:l])
		b = b[l:]
		if len(b) < entryFixedSize {
			return nil, corruptf("footer entry %d (%q) is truncated", i, p)
		}
		size := binary.LittleEndian.Uint64(b)
		crc := binary.LittleEndian.Uint32(b[8:])
		b = b[entryFixedSize:]
		fileOffset, n := readUvarint(b)
		if n <= 0 {
			return nil, corruptf("footer entry %d (%q): file offset is truncated or not minimally encoded", i, p)
		}
		b = b[n:]
		fileSize, n := readUvarint(b)
		if n <= 0 {
			return nil, corruptf("footer entry %d (%q): file size is truncated or not minimally encoded", i, p)
		}
		b = b[n:]
		if size > uint64(membersLen-off) {
			return nil, corruptf("member %q of %d bytes exceeds the member area", p, size)
		}
		if size == 0 && crc != 0 {
			return nil, corruptf("empty member %q has a non-zero checksum", p)
		}
		if err := checkPiece(p, fileOffset, size, fileSize, i == 0, i == count-1); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		if err := paths.add(p); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		entries = append(entries, Entry{
			Path: p, Offset: off, Size: int64(size), FileOffset: int64(fileOffset), FileSize: int64(fileSize), CRC32: crc,
		})
		off += int64(size)
	}
	if len(b) != 0 {
		return nil, corruptf("footer has %d trailing bytes", len(b))
	}
	if off != membersLen {
		return nil, corruptf("members take %d bytes but the member area is %d bytes", off, membersLen)
	}
	return entries, nil
}

// checkPiece checks where a member of size bytes sits in its file: within the file, a piece of
// a split file not empty, a piece that does not start the file only first in its volume, and
// a piece that does not end the file only last in it.
func checkPiece(p string, fileOffset, size, fileSize uint64, first, last bool) error {
	if fileSize > math.MaxInt64 || size > fileSize || fileOffset > fileSize-size {
		return fmt.Errorf("member %q of %d bytes at %d does not fit its file of %d bytes", p, size, fileOffset, fileSize)
	}
	if size == 0 && fileSize != 0 {
		return fmt.Errorf("member %q is an empty piece of a file of %d bytes", p, fileSize)
	}
	if fileOffset > 0 && !first {
		return fmt.Errorf("member %q continues its file but is not the first of its volume", p)
	}
	if fileOffset+size < fileSize && !last {
		return fmt.Errorf("member %q leaves its file unfinished but is not the last of its volume", p)
	}
	return nil
}

// readUvarint decodes a uvarint and rejects any encoding but the shortest, so that one footer
// has exactly one encoding; n <= 0 reports a bad one.
func readUvarint(b []byte) (uint64, int) {
	v, n := binary.Uvarint(b)
	if n > 0 && n != uvarintLen(v) {
		return 0, -1
	}
	return v, n
}

// validPath checks that p is a clean, relative, slash-separated member path.
func validPath(p string) error {
	if p == "" {
		return errors.New("empty member path")
	}
	if len(p) > maxPathLen {
		return fmt.Errorf("member path of %d bytes is longer than %d", len(p), maxPathLen)
	}
	if !utf8.ValidString(p) {
		return fmt.Errorf("member path %q is not valid UTF-8", p)
	}
	for i := 0; i < len(p); i++ {
		if c := p[i]; c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("member path %q has a control character or a backslash", p)
		}
	}
	if strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return fmt.Errorf("member path %q is not a clean relative path", p)
	}
	for _, c := range strings.Split(p, "/") {
		if c == "." || c == ".." {
			return fmt.Errorf("member path %q has a %q component", p, c)
		}
		if len(c) > maxComponentLen {
			return fmt.Errorf("member path %q has a component longer than %d bytes", p, maxComponentLen)
		}
	}
	return nil
}

// pathSet tracks member paths so that none repeats and none is also a directory of another.
type pathSet struct {
	files map[string]struct{}
	dirs  map[string]struct{}
}

func newPathSet() *pathSet {
	return &pathSet{files: map[string]struct{}{}, dirs: map[string]struct{}{}}
}

func (s *pathSet) add(p string) error {
	if err := validPath(p); err != nil {
		return err
	}
	if _, ok := s.files[p]; ok {
		return fmt.Errorf("member path %q repeats", p)
	}
	if _, ok := s.dirs[p]; ok {
		return fmt.Errorf("member path %q is also a directory of another member", p)
	}
	// Walk the parent directories from the deepest: once one is known, its own parents were
	// checked when it was recorded.
	for i := len(p) - 1; i > 0; i-- {
		if p[i] != '/' {
			continue
		}
		dir := p[:i]
		if _, ok := s.dirs[dir]; ok {
			break
		}
		if _, ok := s.files[dir]; ok {
			return fmt.Errorf("member path %q lies under member %q", p, dir)
		}
		s.dirs[dir] = struct{}{}
	}
	s.files[p] = struct{}{}
	return nil
}
