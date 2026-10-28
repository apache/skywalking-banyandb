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
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// splitVolumes writes a small file, a file spanning three volumes and another small file, and
// returns the volume bytes with the files.
func splitVolumes(t *testing.T) ([][]byte, memFiles, []Member) {
	t.Helper()
	rng := rand.New(rand.NewPCG(21, 22)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 1000, 150<<10, 2000)
	sink := writeVolumes(t, MinFileSize, files, members)
	require.Len(t, sink.files, 3)
	out := make([][]byte, len(sink.files))
	for i, f := range sink.files {
		out[i] = f.data
	}
	return out, files, members
}

func TestVolumeSet_RejectsVolumesThatDoNotFit(t *testing.T) {
	v, _, _ := splitVolumes(t)
	cases := map[string][][]byte{
		"middle volume missing":   {v[0], v[2]},
		"first volume missing":    {v[1], v[2]},
		"last volume missing":     {v[0], v[1]},
		"volumes out of order":    {v[0], v[2], v[1]},
		"volume repeated":         {v[0], v[1], v[1], v[2]},
		"set repeated":            {v[0], v[1], v[2], v[0], v[1], v[2]},
		"only the middle volume":  {v[1]},
		"only a continued volume": {v[2]},
	}
	for name, volumes := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newSet(volumes...)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
	_, err := NewVolumeSet(nil)
	require.Error(t, err)
}

func TestVolumeSet_RejectsPiecesThatDoNotFit(t *testing.T) {
	data := []byte("abcdef")
	piece := func(p string, from, to, fileOffset, fileSize uint64) []byte {
		return rawContainer(data[from:to], rawFooter(rawEntry{path: p, size: to - from, fileOffset: fileOffset, fileSize: fileSize, crc: crcOf(data[from:to])}), magic)
	}
	first := piece("a", 0, 3, 0, 6)
	cases := map[string][]byte{
		"another file":       piece("b", 3, 6, 3, 6),
		"another file size":  piece("a", 3, 6, 3, 7),
		"a gap":              piece("a", 4, 6, 4, 6),
		"an overlap":         piece("a", 2, 6, 2, 6),
		"a new file instead": piece("b", 0, 3, 0, 3),
	}
	for name, second := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newSet(first, second)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
	s, err := newSet(first, piece("a", 3, 6, 3, 6))
	require.NoError(t, err)
	got, err := s.ReadFile(s.Files()[0], 6)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestVolumeSet_RejectsPathsRepeatedAcrossVolumes(t *testing.T) {
	a, _ := writeContainer(t, testMember{"a", []byte("x")})
	ab, _ := writeContainer(t, testMember{"a/b", []byte("y")})
	for name, volumes := range map[string][][]byte{"repeat": {a, a}, "file under a file": {a, ab}, "file over a file": {ab, a}} {
		_, err := newSet(volumes...)
		require.ErrorIs(t, err, ErrCorrupt, name)
	}
}

func TestVolumeSet_ChecksumOfAPieceInALaterVolume(t *testing.T) {
	v, files, members := splitVolumes(t)
	v[1][10] ^= 0xff
	s, err := newSet(v...)
	require.NoError(t, err, "opening reads only the footers")
	f := s.Files()[1]
	r, err := s.Open(f)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.ErrorIs(t, err, ErrChecksum)
	assert.Less(t, len(data), int(members[1].Size)+1)
	_, err = s.ReadFile(f, f.Size)
	require.ErrorIs(t, err, ErrChecksum)
	_, err = s.Verify(context.Background())
	require.ErrorIs(t, err, ErrChecksum)
	assert.Contains(t, err.Error(), "volume 2")
	got, err := s.ReadFile(s.Files()[0], 1<<20)
	require.NoError(t, err)
	assert.Equal(t, files[members[0].Path], got, "the other files still read")
}

func TestVolumeSet_Verify(t *testing.T) {
	v, _, _ := splitVolumes(t)
	s, err := newSet(v...)
	require.NoError(t, err)
	sums, err := s.Verify(context.Background())
	require.NoError(t, err)
	require.Len(t, sums, 3)
	for i, sum := range sums {
		assert.Equal(t, uint64(len(v[i])), sum.Bytes)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Verify(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrCorrupt)
}

func TestOpenVolumes(t *testing.T) {
	v, files, members := splitVolumes(t)
	dir := t.TempDir()
	var paths []string
	for i, data := range v {
		p := filepath.Join(dir, "shard-0-0000"+string(rune('1'+i))+".bnc")
		require.NoError(t, os.WriteFile(p, data, 0o600))
		paths = append(paths, p)
	}
	s, err := OpenVolumes(paths)
	require.NoError(t, err)
	for i, f := range s.Files() {
		got, readErr := s.ReadFile(f, f.Size)
		require.NoError(t, readErr)
		assert.True(t, bytes.Equal(files[members[i].Path], got))
	}
	require.NoError(t, s.Close())

	_, err = OpenVolumes([]string{paths[0], filepath.Join(dir, "missing.bnc")})
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = OpenVolumes([]string{paths[0], paths[2]})
	require.ErrorIs(t, err, ErrCorrupt)
}

// A part whose files cross volumes groups like one inside a single volume.
func TestVolumeSet_ShardAcrossVolumes(t *testing.T) {
	files := memFiles{}
	var members []Member
	for i, p := range []string{id1 + "/metadata.json", id1 + "/primary.bin", id2 + "/metadata.json", id2 + "/primary.bin", "18e29603bfcd2125.snp"} {
		size := 40 << 10
		if p == id1+"/primary.bin" {
			size = 100 << 10
		}
		files[p] = bytes.Repeat([]byte{byte(i)}, size)
		members = append(members, Member{Path: p, Size: int64(size)})
	}
	sink := writeVolumes(t, MinFileSize, files, OrderShard(members))
	require.Greater(t, len(sink.files), 3)
	set := checkVolumes(t, sink, files, OrderShard(members), MinFileSize)
	layout := set.Shard()
	require.Len(t, layout.Parts, 2)
	assert.Equal(t, id1+"/metadata.json", layout.Parts[0].Subs[0].Meta.Path)
	assert.Equal(t, []string{id1 + "/primary.bin"}, entryPaths(layout.Parts[0].Subs[0].Files))
	assert.Greater(t, len(layout.Parts[0].Subs[0].Files[0].pieces), 1, "the primary file crosses volumes")
	assert.Equal(t, []string{"18e29603bfcd2125.snp"}, entryPaths(layout.Snapshots))
}
