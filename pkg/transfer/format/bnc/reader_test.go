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
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func smallContainer(t *testing.T) []byte {
	t.Helper()
	rng := rand.New(rand.NewPCG(5, 6)) //nolint:gosec // a fixed seed keeps failures reproducible
	random := func(n int) []byte { return randomBytes(rng, n) }
	b, _ := writeContainer(t,
		testMember{"0000000000000001/metadata.json", random(160)},
		testMember{"0000000000000001/primary.bin", random(900)},
		testMember{"0000000000000001/searchable.tff", nil},
		testMember{"idx/000000000000.seg", random(700)},
		testMember{"idx/000000000001.snp", random(48)},
	)
	require.Less(t, len(b), 4096)
	return b
}

func TestOpen_EveryTruncation(t *testing.T) {
	b := smallContainer(t)
	for n := 0; n < len(b); n++ {
		_, err := openBytes(b[:n])
		require.ErrorIs(t, err, ErrCorrupt, "prefix of %d bytes", n)
	}
}

// A container whose first member is itself a whole container still opens when it is cut right
// after that member: only the size and CRC-32 in the manifest can tell. This pins the limit of
// what Open can promise.
func TestOpen_TruncationAtNestedContainer(t *testing.T) {
	inner, _ := writeContainer(t, testMember{"a", []byte("payload")})
	outer, _ := writeContainer(t, testMember{"inner.bnc", inner}, testMember{"b", []byte("more")})
	c, err := openBytes(outer[:len(inner)])
	require.NoError(t, err)
	_, err = c.Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "a", c.Entries()[0].Path)
}

func TestOpen_FooterLengthBounds(t *testing.T) {
	members := []byte("data")
	open := func(b []byte) (*countingReaderAt, error) {
		r := &countingReaderAt{r: bytes.NewReader(b)}
		_, err := Open(r, int64(len(b)))
		return r, err
	}
	for name, footerLen := range map[string]uint32{"beyond the file": uint32(len(members)) + 1, "max uint32": 0xFFFFFFFF} {
		t.Run(name, func(t *testing.T) {
			b := rawContainer(members, nil, magic)
			binary.LittleEndian.PutUint32(b[len(b)-8:], footerLen)
			r, err := open(b)
			require.ErrorIs(t, err, ErrCorrupt)
			assert.Equal(t, []readCall{{off: int64(len(b) - trailerSize), n: trailerSize}}, r.calls, "nothing beyond the trailer is read")
		})
	}
	t.Run("above maxFooterSize", func(t *testing.T) {
		b := rawContainer(make([]byte, maxFooterSize+1), nil, magic)
		binary.LittleEndian.PutUint32(b[len(b)-8:], maxFooterSize+1)
		r, err := open(b)
		require.ErrorIs(t, err, ErrCorrupt)
		assert.Len(t, r.calls, 1)
	})
	t.Run("exactly maxFooterSize is read", func(t *testing.T) {
		footer := make([]byte, maxFooterSize)
		r, err := open(rawContainer(nil, footer, magic))
		require.ErrorIs(t, err, ErrCorrupt, "the zero bytes do not decode as entries")
		assert.Len(t, r.calls, 2)
	})
}

func TestOpen_FooterEncoding(t *testing.T) {
	member := []byte("abc")
	good := rawEntry{path: "a", size: 3, crc: crcOf(member)}
	long := rawEntry{path: strings.Repeat("p", 26), size: 3, crc: crcOf(member)}
	cases := map[string][]byte{
		"truncated count":    {0x80},
		"overlong count":     append([]byte{0x81, 0x00}, rawFooter(good)[1:]...),
		"count beyond bytes": binary.AppendUvarint(nil, 3),
		"count above max":    binary.AppendUvarint(nil, maxEntries+1),
		// Each of these declares two entries over enough bytes to pass the count bound, so the
		// decoder reaches the second entry and fails there.
		"truncated path length": twoEntries(long, 0x80),
		"path beyond footer":    twoEntries(long, 200, 'a'),
		"truncated fixed part":  twoEntries(long, 1, 'b', 1, 2, 3, 4, 5),
		"trailing bytes":        append(rawFooter(good), 0),
		"sizes short of area":   rawFooter(rawEntry{path: "a", size: 2, crc: crcOf(member[:2])}),
		"sizes beyond area":     rawFooter(rawEntry{path: "a", size: 4}),
		"empty with checksum":   rawFooter(good, rawEntry{path: "b", size: 0, crc: 1}),
		"no entries, members":   rawFooter(),
		// The position of a member in its file.
		"truncated file offset": append(rawFooter(good)[:len(rawFooter(good))-2], 0x80),
		"overlong file offset":  append(rawFooter(good)[:len(rawFooter(good))-2], 0x80, 0x00, 3),
		"truncated file size":   append(rawFooter(good)[:len(rawFooter(good))-1], 0x80),
		"file smaller than it":  rawFooter(rawEntry{path: "a", size: 3, fileSize: 2, crc: crcOf(member)}),
		"beyond its file":       rawFooter(rawEntry{path: "a", size: 3, fileOffset: 8, fileSize: 10, crc: crcOf(member)}),
		"file size above int64": rawFooter(rawEntry{path: "a", size: 3, fileSize: 1 << 63, crc: crcOf(member)}),
		"empty piece":           rawFooter(good, rawEntry{path: "b", size: 0, fileSize: 5}),
		"continues, not first": rawFooter(
			rawEntry{path: "a", size: 1, crc: crcOf(member[:1])},
			rawEntry{path: "b", size: 2, fileOffset: 1, fileSize: 3, crc: crcOf(member[1:])},
		),
		"unfinished, not last": rawFooter(rawEntry{path: "a", size: 1, fileSize: 2, crc: crcOf(member[:1])}, rawEntry{path: "b", size: 2, crc: crcOf(member[1:])}),
	}
	// A volume may start with the rest of a file and end with the start of another.
	c, err := openBytes(rawContainer(member, rawFooter(
		rawEntry{path: "a", size: 1, fileOffset: 4, fileSize: 5, crc: crcOf(member[:1])},
		rawEntry{path: "b", size: 2, fileSize: 9, crc: crcOf(member[1:])},
	), magic))
	require.NoError(t, err)
	assert.Equal(t, []Entry{
		{Path: "a", Size: 1, FileOffset: 4, FileSize: 5, CRC32: crcOf(member[:1])},
		{Path: "b", Offset: 1, Size: 2, FileSize: 9, CRC32: crcOf(member[1:])},
	}, c.Entries())
	empty := rawContainer(nil, rawFooter(), magic)
	_, err = openBytes(empty)
	require.ErrorIs(t, err, ErrCorrupt, "a container needs at least one member")
	for name, footer := range cases {
		t.Run(name, func(t *testing.T) {
			_, openErr := openBytes(rawContainer(member, footer, magic))
			require.ErrorIs(t, openErr, ErrCorrupt)
		})
	}
	c, err = openBytes(rawContainer(member, rawFooter(good), magic))
	require.NoError(t, err, "the hand-made footer itself is valid")
	_, err = openBytes(rawContainer(member, rawFooter(long), magic))
	require.NoError(t, err, "the first entry of the two-entry cases is valid")
	assert.Equal(t, member, readMember(t, c, c.Entries()[0]))
}

// twoEntries encodes a footer declaring two entries: first, then the raw bytes of the second.
func twoEntries(first rawEntry, second ...byte) []byte {
	b := append(binary.AppendUvarint(nil, 2), rawFooter(first)[1:]...)
	return append(b, second...)
}

func TestOpen_HugeEntryCountDoesNotAllocate(t *testing.T) {
	b := rawContainer(nil, binary.AppendUvarint(nil, maxEntries), magic)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := openBytes(b)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}

func TestOpen_Magic(t *testing.T) {
	member := []byte("abc")
	footer := rawFooter(rawEntry{path: "a", size: 3, crc: crcOf(member)})
	for _, m := range []string{"XXXX", "BNC\x00", "BNCa", "bnca"} {
		_, err := openBytes(rawContainer(member, footer, m))
		require.ErrorIs(t, err, ErrCorrupt, m)
		require.NotErrorIs(t, err, ErrUnsupportedVersion, m)
	}
	for _, m := range []string{"BNCB", "BNCZ"} {
		_, err := openBytes(rawContainer(member, footer, m))
		require.ErrorIs(t, err, ErrUnsupportedVersion, m)
		require.NotErrorIs(t, err, ErrCorrupt, m)
	}
}

func TestOpen_FooterChecksum(t *testing.T) {
	b := smallContainer(t)
	c := mustOpen(t, b)
	footerStart := len(b) - len(c.tail)
	for i := footerStart; i < len(b)-trailerSize; i++ {
		mutated := bytes.Clone(b)
		mutated[i] ^= 0x01
		_, err := openBytes(mutated)
		require.ErrorIs(t, err, ErrCorrupt, "footer byte %d", i)
	}
}

func TestOpen_InvalidPathsInFooter(t *testing.T) {
	for name, p := range invalidPaths {
		t.Run(name, func(t *testing.T) {
			_, err := openBytes(rawContainer(nil, rawFooter(rawEntry{path: p}), magic))
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
	for name, paths := range map[string][]string{"repeat": {"a", "a"}, "ancestor": {"a", "a-x", "a/b"}} {
		t.Run(name, func(t *testing.T) {
			var entries []rawEntry
			for _, p := range paths {
				entries = append(entries, rawEntry{path: p})
			}
			_, err := openBytes(rawContainer(nil, rawFooter(entries...), magic))
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestOpen_ReadsOnlyTheTail(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", bytes.Repeat([]byte("x"), 1<<20)}, testMember{"b", []byte("y")})
	r := &countingReaderAt{r: bytes.NewReader(b)}
	_, err := Open(r, int64(len(b)))
	require.NoError(t, err)
	assert.Len(t, r.calls, 2)
}

func TestMember_ChecksumMismatch(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", []byte("first")}, testMember{"b", []byte("second")})
	b[1] ^= 0xff
	c := mustOpen(t, b)
	entries := c.Entries()
	r := c.member(entries[0])
	buf := make([]byte, 5)
	n, err := io.ReadFull(r, buf)
	require.NoError(t, err, "the mismatch shows only after the last byte")
	assert.Equal(t, 5, n)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, ErrChecksum)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), `"a"`)
	assert.Equal(t, []byte("second"), readMember(t, c, entries[1]))
	_, err = c.Verify(context.Background())
	require.ErrorIs(t, err, ErrChecksum)
}

func TestMember_ShortRead(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", bytes.Repeat([]byte("x"), 100)})
	c := mustOpen(t, b)
	e := c.Entries()[0]
	c.r = bytes.NewReader(b[:50])
	_, err := io.ReadAll(c.member(e))
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrChecksum)
	_, err = c.Verify(context.Background())
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrChecksum)
	s, err := NewVolumeSet([]*Container{c})
	require.NoError(t, err)
	_, err = s.ReadFile(s.Files()[0], e.Size)
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrChecksum)
}

// An I/O failure of the source is reported as such, not as a corrupt container.
func TestReadErrorsKeepTheirIdentity(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", bytes.Repeat([]byte("x"), 100)})
	_, err := Open(failingReaderAt{}, int64(len(b)))
	require.ErrorIs(t, err, errSink)
	require.NotErrorIs(t, err, ErrCorrupt)
	c := mustOpen(t, b)
	e := c.Entries()[0]
	c.r = failingReaderAt{}
	_, err = c.Verify(context.Background())
	require.ErrorIs(t, err, errSink)
	require.NotErrorIs(t, err, ErrCorrupt)
	s, err := NewVolumeSet([]*Container{c})
	require.NoError(t, err)
	_, err = s.ReadFile(s.Files()[0], e.Size)
	require.ErrorIs(t, err, errSink)
	require.NotErrorIs(t, err, ErrCorrupt)
}

type failingReaderAt struct{}

func (failingReaderAt) ReadAt([]byte, int64) (int, error) { return 0, errSink }

func TestVolumeSetOpen_RejectsForeignFile(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", []byte("abc")})
	s := setOf(t, b)
	for _, forged := range []File{{Path: "b", Size: 3}, {Path: "a", Size: 1 << 30}} {
		_, err := s.Open(forged)
		require.Error(t, err)
	}
}

func TestMember_ConcurrentAndRepeated(t *testing.T) {
	members := []testMember{{"a", bytes.Repeat([]byte("a"), 3000)}, {"b", bytes.Repeat([]byte("b"), 5000)}, {"c", nil}}
	b, _ := writeContainer(t, members...)
	c := mustOpen(t, b)
	entries := c.Entries()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test readers joined by the WaitGroup; a panic must fail the test
		go func(i int) {
			defer wg.Done()
			m := members[i%len(members)]
			r := c.member(entries[i%len(entries)])
			data, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Equal(t, len(m.data), len(data))
			assert.True(t, bytes.Equal(m.data, data))
		}(i)
	}
	wg.Wait()
}

func TestMember_Empty(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", []byte("x")}, testMember{"empty", nil})
	c := mustOpen(t, b)
	r := c.member(c.Entries()[1])
	n, err := r.Read(make([]byte, 8))
	assert.Zero(t, n)
	assert.ErrorIs(t, err, io.EOF)
}

func TestEntries_ReturnsCopy(t *testing.T) {
	b, _ := writeContainer(t, testMember{"0000000000000001/metadata.json", []byte("{}")})
	c := mustOpen(t, b)
	entries := c.Entries()
	entries[0].Path = "x"
	entries[0].Size = 99
	assert.Equal(t, []byte("{}"), readMember(t, c, c.Entries()[0]))
	s := setOf(t, b)
	files := s.Files()
	files[0].Path = "x"
	assert.Equal(t, "0000000000000001/metadata.json", s.Shard().Parts[0].Subs[0].Meta.Path)
}

func TestReadFile(t *testing.T) {
	b, _ := writeContainer(t, testMember{"m", []byte(`{"totalCount":3}`)})
	s := setOf(t, b)
	f := s.Files()[0]
	data, err := s.ReadFile(f, 1<<20)
	require.NoError(t, err)
	assert.Equal(t, `{"totalCount":3}`, string(data))
	_, err = s.ReadFile(f, 4)
	require.Error(t, err)
	b[0] ^= 1
	_, err = setOf(t, b).ReadFile(f, 1<<20)
	require.ErrorIs(t, err, ErrChecksum)
}

func TestVerify_Canceled(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", bytes.Repeat([]byte("x"), 3*verifyBlock)})
	c := mustOpen(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Verify(ctx)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	c.r = &cancelAfterReaderAt{r: bytes.NewReader(b), cancel: cancel}
	_, err = c.Verify(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// cancelAfterReaderAt cancels its context after the first read.
type cancelAfterReaderAt struct {
	r      io.ReaderAt
	cancel context.CancelFunc
}

func (c *cancelAfterReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.cancel()
	return c.r.ReadAt(p, off)
}

func TestVerify_TailReplaced(t *testing.T) {
	b, _ := writeContainer(t, testMember{"a", []byte("abc")})
	c := mustOpen(t, b)
	replaced := bytes.Clone(b)
	replaced[len(replaced)-trailerSize-1] ^= 0xff
	c.r = bytes.NewReader(replaced)
	_, err := c.Verify(context.Background())
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestVerify_ZeroLengthMembers(t *testing.T) {
	b, sum := writeContainer(t, testMember{"a", nil}, testMember{"b", []byte("x")}, testMember{"c", nil}, testMember{"d", nil})
	got, err := mustOpen(t, b).Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sum, got)
	b, sum = writeContainer(t, testMember{"only", nil})
	got, err = mustOpen(t, b).Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sum, got)
}

func TestOpenFile(t *testing.T) {
	dir := t.TempDir()
	b, sum := writeContainer(t, testMember{"a", []byte("abc")})
	p := filepath.Join(dir, "shard-0-00001.bnc")
	require.NoError(t, os.WriteFile(p, b, 0o600))
	c, err := OpenFile(p)
	require.NoError(t, err)
	got, err := c.Verify(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sum, got)
	require.NoError(t, c.Close())

	_, err = OpenFile(dir)
	require.Error(t, err, "a directory is not a container")
	_, err = OpenFile(filepath.Join(dir, "missing.bnc"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
