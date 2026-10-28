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
	"fmt"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// zeroReader yields n zero bytes.
type zeroReader struct{ n int64 }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.n == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), z.n)
	clear(p[:n])
	z.n -= n
	return int(n), nil
}

// zeroReaderAt serves a container whose members are all zero bytes and whose tail is tail.
type zeroReaderAt struct {
	tail       []byte
	membersEnd int64
}

func (z zeroReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	if off < z.membersEnd {
		n = int(min(int64(len(p)), z.membersEnd-off))
		clear(p[:n])
	}
	if tailOff := off + int64(n) - z.membersEnd; n < len(p) && tailOff < int64(len(z.tail)) {
		n += copy(p[n:], z.tail[tailOff:])
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// tailWriter drops the first skip bytes written to it and keeps the rest.
type tailWriter struct {
	tail []byte
	skip int64
}

func (t *tailWriter) Write(p []byte) (int, error) {
	n := len(p)
	k := min(int64(n), t.skip)
	t.skip -= k
	t.tail = append(t.tail, p[k:]...)
	return n, nil
}

var benchShapes = []struct {
	name    string
	members int
	size    int64
}{
	{name: "1x256MiB", members: 1, size: 256 << 20},
	{name: "10000x4KiB", members: 10000, size: 4 << 10},
}

func BenchmarkWriterAdd(b *testing.B) {
	for _, shape := range benchShapes {
		b.Run(shape.name, func(b *testing.B) {
			b.SetBytes(int64(shape.members) * shape.size)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				w := NewWriter(io.Discard)
				for m := 0; m < shape.members; m++ {
					if err := w.Add(context.Background(), fmt.Sprintf("m%05d", m), shape.size, &zeroReader{n: shape.size}); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// zeroContainer returns a reader of a container of zero-byte members of the given shape. Only
// its footer and trailer are kept in memory.
func zeroContainer(b testing.TB, members int, size int64) (io.ReaderAt, int64) {
	membersEnd := int64(members) * size
	tail := &tailWriter{skip: membersEnd}
	w := NewWriter(tail)
	for m := 0; m < members; m++ {
		require.NoError(b, w.Add(context.Background(), fmt.Sprintf("m%05d", m), size, &zeroReader{n: size}))
	}
	sum, err := w.Close()
	require.NoError(b, err)
	return zeroReaderAt{membersEnd: membersEnd, tail: tail.tail}, int64(sum.Bytes)
}

func BenchmarkVerify(b *testing.B) {
	for _, shape := range benchShapes {
		b.Run(shape.name, func(b *testing.B) {
			r, size := zeroContainer(b, shape.members, shape.size)
			c, err := Open(r, size)
			require.NoError(b, err)
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = c.Verify(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOpen(b *testing.B) {
	for _, n := range []int{5000, 50000} {
		b.Run(fmt.Sprintf("%d-entries", n), func(b *testing.B) {
			r, size := zeroContainer(b, n, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Open(r, size); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFile(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 64<<20)
	blob, _ := writeContainer(b, testMember{"m", data})
	s := setOf(b, blob)
	f := s.Files()[0]
	for _, bufSize := range []int{4 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("read-%dKiB", bufSize>>10), func(b *testing.B) {
			buf := make([]byte, bufSize)
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				r, err := s.Open(f)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = io.CopyBuffer(io.Discard, struct{ io.Reader }{r}, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The memory of writing and verifying does not grow with the container: a 64 MiB member
// allocates about as much as a 64 KiB one.
func TestMemoryDoesNotGrowWithSize(t *testing.T) {
	sizes := []int64{64 << 10, 64 << 20}
	measure := func(fn func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		fn()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	for _, size := range sizes {
		wrote := measure(func() {
			w := NewWriter(io.Discard)
			require.NoError(t, w.Add(context.Background(), "m", size, &zeroReader{n: size}))
			_, err := w.Close()
			require.NoError(t, err)
		})
		assert.Less(t, wrote, uint64(1<<20), "writing %d bytes", size)
		r, total := zeroContainer(t, 1, size)
		c, err := Open(r, total)
		require.NoError(t, err)
		verified := measure(func() {
			_, err = c.Verify(context.Background())
			require.NoError(t, err)
		})
		assert.Less(t, verified, uint64(2<<20), "verifying %d bytes", size)
	}
}
