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
	"hash/crc32"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testMember struct {
	path string
	data []byte
}

// writeContainer writes members into a container and returns its bytes and summary.
func writeContainer(t testing.TB, members ...testMember) ([]byte, Summary) {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, m := range members {
		require.NoError(t, w.Add(context.Background(), m.path, int64(len(m.data)), bytes.NewReader(m.data)))
	}
	sum, err := w.Close()
	require.NoError(t, err)
	return buf.Bytes(), sum
}

func openBytes(b []byte) (*Container, error) {
	return Open(bytes.NewReader(b), int64(len(b)))
}

func mustOpen(t testing.TB, b []byte) *Container {
	t.Helper()
	c, err := openBytes(b)
	require.NoError(t, err)
	return c
}

func readMember(t testing.TB, c *Container, e Entry) []byte {
	t.Helper()
	data, err := io.ReadAll(c.member(e))
	require.NoError(t, err)
	return data
}

// newSet opens the given volumes and joins them into a VolumeSet.
func newSet(volumes ...[]byte) (*VolumeSet, error) {
	cs := make([]*Container, 0, len(volumes))
	for _, v := range volumes {
		c, err := openBytes(v)
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return NewVolumeSet(cs)
}

func setOf(t testing.TB, volumes ...[]byte) *VolumeSet {
	t.Helper()
	s, err := newSet(volumes...)
	require.NoError(t, err)
	return s
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// rawEntry is a footer entry; a zero fileSize stands for a whole file of size bytes.
type rawEntry struct {
	path       string
	size       uint64
	fileOffset uint64
	fileSize   uint64
	crc        uint32
}

// rawFooter encodes the footer of a container of one volume by hand, so tests can build footers
// the Writer refuses to write.
func rawFooter(entries ...rawEntry) []byte { return rawVolumeFooter(1, true, entries...) }

// rawVolumeFooter encodes the footer of volume seq by hand.
func rawVolumeFooter(seq uint64, last bool, entries ...rawEntry) []byte {
	b := binary.AppendUvarint(nil, seq)
	if last {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(entries)))
	for _, e := range entries {
		b = binary.AppendUvarint(b, uint64(len(e.path)))
		b = append(b, e.path...)
		b = binary.LittleEndian.AppendUint64(b, e.size)
		b = binary.LittleEndian.AppendUint32(b, e.crc)
		fileSize := e.fileSize
		if fileSize == 0 {
			fileSize = e.size
		}
		b = binary.AppendUvarint(b, e.fileOffset)
		b = binary.AppendUvarint(b, fileSize)
	}
	return b
}

// hdr returns the footer header of a container of one volume followed by rest.
func hdr(rest ...byte) []byte { return append([]byte{1, 1}, rest...) }

// rawContainer assembles members, footer and a trailer with a correct footer CRC-32.
func rawContainer(members, footer []byte, mag string) []byte {
	out := append(append([]byte{}, members...), footer...)
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(footer))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, mag...)
}

func crcOf(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

type readCall struct {
	off int64
	n   int
}

// countingReaderAt records every ReadAt call.
type countingReaderAt struct {
	r     io.ReaderAt
	calls []readCall
	mu    sync.Mutex
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	c.calls = append(c.calls, readCall{off: off, n: len(p)})
	c.mu.Unlock()
	return c.r.ReadAt(p, off)
}

type sinkFile struct {
	path     string
	data     []byte
	sum      Summary
	seq      uint32
	finished bool
}

// memSink keeps the volumes of a VolumeWriter in memory. It scribbles over every slice it
// receives after copying it, to prove the writer does not rely on the slice afterwards.
type memSink struct {
	failOn   string
	files    []*sinkFile
	maxWrite int
}

var errSink = errors.New("sink failure")

func (s *memSink) StartFile(path string, seq uint32) error {
	if s.failOn == "start" {
		return errSink
	}
	if n := len(s.files); n > 0 && !s.files[n-1].finished {
		return errors.New("StartFile before FinishFile")
	}
	s.files = append(s.files, &sinkFile{path: path, seq: seq})
	return nil
}

func (s *memSink) Write(p []byte) error {
	if s.failOn == "write" {
		return errSink
	}
	s.maxWrite = max(s.maxWrite, len(p))
	f := s.files[len(s.files)-1]
	f.data = append(f.data, p...)
	for i := range p {
		p[i] ^= 0xff
	}
	return nil
}

func (s *memSink) FinishFile(sum Summary) error {
	if s.failOn == "finish" {
		return errSink
	}
	f := s.files[len(s.files)-1]
	if f.finished {
		return errors.New("FinishFile twice")
	}
	f.sum, f.finished = sum, true
	return nil
}

// memFiles serves member bytes by path for a VolumeWriter.
type memFiles map[string][]byte

func (m memFiles) open(path string) (io.ReadCloser, error) {
	data, ok := m[path]
	if !ok {
		return nil, errors.New("no such member")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
