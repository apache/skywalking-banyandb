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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_RoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed seed keeps failures reproducible
	for round := 0; round < 20; round++ {
		n := 1 + rng.IntN(50)
		members := make([]testMember, n)
		for i := range members {
			var size int
			switch rng.IntN(4) {
			case 0:
				size = 0
			case 1:
				size = (1 << 20) + rng.IntN(1<<16)
			default:
				size = rng.IntN(4096)
			}
			members[i] = testMember{path: fmt.Sprintf("%016x/Dir.%d/file-%d.tf", round, i%3, i), data: randomBytes(rng, size)}
		}
		b, sum := writeContainer(t, members...)
		c := mustOpen(t, b)
		entries := c.Entries()
		require.Len(t, entries, n)
		var off int64
		for i, e := range entries {
			assert.Equal(t, members[i].path, e.Path)
			assert.Equal(t, off, e.Offset)
			assert.Equal(t, int64(len(members[i].data)), e.Size)
			assert.Equal(t, crcOf(members[i].data), e.CRC32)
			assert.Equal(t, members[i].data, readMember(t, c, e))
			off += e.Size
		}
		want := Summary{Bytes: uint64(len(b)), CRC32: crcOf(b), Entries: uint32(n)}
		assert.Equal(t, want, sum)
		verified, err := c.Verify(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want, verified)
	}
}

func TestWriter_Deterministic(t *testing.T) {
	members := []testMember{{"0000000000000001/primary.bin", []byte("abc")}, {"idx/000000000000.seg", nil}, {"x", []byte("y")}}
	first, _ := writeContainer(t, members...)
	second, _ := writeContainer(t, members...)
	assert.Equal(t, first, second)
}

func TestWriter_NoMembers(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, err := w.Close()
	require.Error(t, err)
	assert.Zero(t, buf.Len(), "no footer is written")
}

func TestWriter_SizeMismatch(t *testing.T) {
	for name, data := range map[string]string{"shorter": "ab", "longer": "abcd"} {
		t.Run(name, func(t *testing.T) {
			w := NewWriter(io.Discard)
			require.Error(t, w.Add(context.Background(), "f", 3, strings.NewReader(data)))
		})
	}
}

// invalidPaths are refused by the writer and, in a hand-made footer, by the reader.
var invalidPaths = map[string]string{
	"empty":            "",
	"absolute":         "/a",
	"trailing slash":   "a/",
	"double slash":     "a//b",
	"dot component":    "./a",
	"dot-dot inside":   "a/../b",
	"dot-dot":          "..",
	"backslash":        `a\b`,
	"nul":              "a\x00b",
	"newline":          "a\nb",
	"del":              "a\x7fb",
	"invalid utf-8":    "a\xffb",
	"long component":   strings.Repeat("c", 256),
	"long path":        strings.Repeat("abcdefghi/", 410) + "z",
	"single dot":       ".",
	"dot at end":       "a/.",
	"dot-dot at start": "../a",
}

func TestWriter_InvalidPaths(t *testing.T) {
	for name, p := range invalidPaths {
		t.Run(name, func(t *testing.T) {
			w := NewWriter(io.Discard)
			require.Error(t, w.Add(context.Background(), p, 0, bytes.NewReader(nil)))
		})
	}
}

func TestWriter_PathConflicts(t *testing.T) {
	cases := map[string][]string{
		"repeat":                  {"a/b", "a/b"},
		"file then child":         {"a", "a/b"},
		"child then file":         {"a/b", "a"},
		"interleaved sort order":  {"a", "a-x", "a/b"},
		"deep ancestor conflicts": {"a/b/c/d", "a/b"},
	}
	for name, paths := range cases {
		t.Run(name, func(t *testing.T) {
			w := NewWriter(io.Discard)
			var err error
			for _, p := range paths {
				if err = w.Add(context.Background(), p, 0, bytes.NewReader(nil)); err != nil {
					break
				}
			}
			require.Error(t, err)
		})
	}
	w := NewWriter(io.Discard)
	for _, p := range []string{"a-x", "a.b", "a/b", "A/b"} {
		require.NoError(t, w.Add(context.Background(), p, 0, bytes.NewReader(nil)), "distinct paths with shared prefixes are fine")
	}
}

func TestWriter_ErrorsStick(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.Add(context.Background(), "a", 5, strings.NewReader("abc"))
	require.Error(t, err)
	written := buf.Len()
	assert.Equal(t, err, w.Add(context.Background(), "b", 1, strings.NewReader("b")))
	_, closeErr := w.Close()
	assert.Equal(t, err, closeErr)
	assert.Equal(t, written, buf.Len(), "nothing is written after the first error")
}

func TestWriter_Lifecycle(t *testing.T) {
	w := NewWriter(io.Discard)
	require.NoError(t, w.Add(context.Background(), "a", 1, strings.NewReader("a")))
	_, err := w.Close()
	require.NoError(t, err)
	_, err = w.Close()
	require.Error(t, err)
	require.Error(t, w.Add(context.Background(), "b", 1, strings.NewReader("b")))
}

func TestWriter_FooterLimit(t *testing.T) {
	w := NewWriter(io.Discard)
	component := strings.Repeat("p", 250)
	longPath := strings.TrimSuffix(strings.Repeat(component+"/", 15), "/")
	var err error
	for i := 0; err == nil; i++ {
		err = w.Add(context.Background(), fmt.Sprintf("%s/%07d", longPath, i), 0, bytes.NewReader(nil))
		require.Less(t, i, maxFooterSize/len(longPath)+2, "the footer limit must stop the writer")
	}
	assert.Contains(t, err.Error(), "footer")
}

func TestWriter_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &cancelingReader{cancel: cancel, left: 4 * WriteSize}
	w := NewWriter(io.Discard)
	err := w.Add(ctx, "big", int64(r.left), r)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrCorrupt)
}

// cancelingReader cancels its context after the first read.
type cancelingReader struct {
	cancel context.CancelFunc
	left   int
}

func (r *cancelingReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.left)
	r.left -= n
	r.cancel()
	return n, nil
}

func TestWriter_SourceErrorIdentity(t *testing.T) {
	w := NewWriter(io.Discard)
	err := w.Add(context.Background(), "gone", 3, iotest.ErrReader(fs.ErrNotExist))
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func TestWriter_WriteFailure(t *testing.T) {
	w := NewWriter(&failAfter{})
	err := w.Add(context.Background(), "a", 1, strings.NewReader("a"))
	assert.True(t, errors.Is(err, errSink))
}

func TestWriter_NegativeSize(t *testing.T) {
	w := NewWriter(io.Discard)
	require.Error(t, w.Add(context.Background(), "a", -1, bytes.NewReader(nil)))
}

func TestWriter_FooterWriteFailures(t *testing.T) {
	for name, fail := range map[string]io.Writer{
		"error":       &failAfter{n: 1},
		"short write": &shortAfter{n: 1},
	} {
		t.Run(name, func(t *testing.T) {
			w := NewWriter(fail)
			require.NoError(t, w.Add(context.Background(), "a", 1, strings.NewReader("a")))
			_, err := w.Close()
			require.Error(t, err)
			_, again := w.Close()
			assert.Equal(t, err, again, "the error sticks")
		})
	}
}

// failAfter fails every Write after the first n.
type failAfter struct{ n int }

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n == 0 {
		return 0, errSink
	}
	f.n--
	return len(p), nil
}

// shortAfter writes one byte less than asked after the first n Writes, without an error.
type shortAfter struct{ n int }

func (s *shortAfter) Write(p []byte) (int, error) {
	if s.n == 0 {
		return len(p) - 1, nil
	}
	s.n--
	return len(p), nil
}

func TestWriter_PieceRules(t *testing.T) {
	add := func(w *Writer, p string, fileOffset, size, fileSize int64) error {
		return w.addPiece(context.Background(), p, fileOffset, size, fileSize, bytes.NewReader(make([]byte, size)))
	}
	cases := map[string]func(w *Writer) error{
		"beyond the file": func(w *Writer) error { return add(w, "a", 3, 3, 5) },
		"file too small":  func(w *Writer) error { return add(w, "a", 0, 3, 2) },
		"negative offset": func(w *Writer) error { return add(w, "a", -1, 1, 2) },
		"empty piece":     func(w *Writer) error { return add(w, "a", 0, 0, 2) },
		"continues, not first": func(w *Writer) error {
			if err := add(w, "a", 0, 1, 1); err != nil {
				return err
			}
			return add(w, "b", 1, 1, 2)
		},
		"after an unfinished piece": func(w *Writer) error {
			if err := add(w, "a", 0, 1, 2); err != nil {
				return err
			}
			return add(w, "b", 0, 1, 1)
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, fn(NewWriter(io.Discard)))
		})
	}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, add(w, "a", 4, 1, 5))
	require.NoError(t, add(w, "b", 0, 2, 9))
	_, err := w.Close()
	require.NoError(t, err)
	assert.Equal(t, []Entry{
		{Path: "a", Size: 1, FileOffset: 4, FileSize: 5, CRC32: crcOf([]byte{0})},
		{Path: "b", Offset: 1, Size: 2, FileSize: 9, CRC32: crcOf([]byte{0, 0})},
	}, mustOpen(t, buf.Bytes()).Entries())
}
