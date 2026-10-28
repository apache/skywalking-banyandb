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
	"math/rand/v2"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomFiles registers files of the given sizes with random bytes, named "fNNN".
func randomFiles(rng *rand.Rand, files memFiles, sizes ...int) []Member {
	ms := make([]Member, 0, len(sizes))
	for i, size := range sizes {
		p := fmt.Sprintf("f%03d", i)
		files[p] = randomBytes(rng, size)
		ms = append(ms, Member{Path: p, Size: int64(size)})
	}
	return ms
}

// writeVolumes writes members through a VolumeWriter of the given limit.
func writeVolumes(t *testing.T, limit uint64, files memFiles, members []Member) *memSink {
	t.Helper()
	sink := &memSink{}
	v, err := NewVolumeWriter(sink, "stream/g/seg-20261028/shard-0", limit, files.open)
	require.NoError(t, err)
	for _, m := range members {
		require.NoError(t, v.Add(context.Background(), m))
	}
	require.NoError(t, v.Close())
	return sink
}

// checkVolumes checks the volumes of sink against members and the limit: names and sequence
// numbers, every volume within the limit and filled, every summary right, and every file read
// back whole through a VolumeSet. It returns the set.
func checkVolumes(t *testing.T, sink *memSink, files memFiles, members []Member, limit uint64) *VolumeSet {
	t.Helper()
	count, err := CountVolumes(members, limit)
	require.NoError(t, err)
	require.Len(t, sink.files, int(count), "CountVolumes announces the volumes")
	volumes := make([]*Container, 0, len(sink.files))
	for i, f := range sink.files {
		require.True(t, f.finished)
		assert.Equal(t, uint32(i+1), f.seq)
		assert.Equal(t, fmt.Sprintf("stream/g/seg-20261028/shard-0-%05d.bnc", i+1), f.path)
		c := mustOpen(t, f.data)
		sum, verifyErr := c.Verify(context.Background())
		require.NoError(t, verifyErr)
		assert.Equal(t, f.sum, sum, "FinishFile reports what the volume holds")
		assert.Equal(t, uint32(len(c.Entries())), sum.Entries)
		if limit > 0 {
			require.LessOrEqual(t, len(f.data), int(limit), "volume %d", i+1)
		}
		volumes = append(volumes, c)
	}
	// A volume is sealed only when it cannot take a single further byte: one that leaves a
	// file unfinished is exactly full, and the first piece of the next one does not fit in it.
	for i := 0; limit > 0 && i+1 < len(volumes); i++ {
		entries, next := volumes[i].Entries(), volumes[i+1].Entries()[0]
		if !entries[len(entries)-1].ends() {
			assert.Len(t, sink.files[i].data, int(limit), "volume %d ends inside a file", i+1)
			continue
		}
		grown := len(sink.files[i].data) - uvarintLen(uint64(len(entries))) + uvarintLen(uint64(len(entries)+1)) +
			encodedEntrySize(next.Path, next.FileOffset, next.FileSize)
		if next.FileSize > 0 {
			grown++
		}
		assert.Greater(t, grown, int(limit), "volume %d was sealed although %q fits", i+1, next.Path)
	}
	set, err := NewVolumeSet(volumes)
	require.NoError(t, err)
	got := set.Files()
	require.Len(t, got, len(members))
	for i, m := range members {
		assert.Equal(t, m.Path, got[i].Path)
		assert.Equal(t, m.Size, got[i].Size)
		data, readErr := set.ReadFile(got[i], m.Size)
		require.NoError(t, readErr)
		assert.True(t, bytes.Equal(files[m.Path], data), "file %s reads back whole", m.Path)
	}
	return set
}

func TestVolumeWriter_SplitsAFileAcrossVolumes(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 200<<10)
	sink := writeVolumes(t, MinFileSize, files, members)
	require.Len(t, sink.files, 4)
	set := checkVolumes(t, sink, files, members, MinFileSize)
	pieces := set.Files()[0].pieces
	require.Len(t, pieces, 4)
	var off int64
	for i, p := range pieces {
		assert.Equal(t, i, p.volume)
		assert.Equal(t, off, p.entry.FileOffset)
		assert.Equal(t, int64(200<<10), p.entry.FileSize)
		off += p.entry.Size
	}
	for _, f := range sink.files[:3] {
		assert.Len(t, f.data, MinFileSize, "a volume ending inside a file is full")
	}
}

func TestVolumeWriter_FillsEveryVolume(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	// Small files share a volume with the start of the next large one.
	members := randomFiles(rng, files, 10<<10, 0, 100<<10, 30<<10, 0, 0, 150<<10, 5)
	sink := writeVolumes(t, MinFileSize, files, members)
	set := checkVolumes(t, sink, files, members, MinFileSize)
	first := mustOpen(t, sink.files[0].data).Entries()
	require.Len(t, first, 3)
	assert.Equal(t, []string{"f000", "f001", "f002"}, []string{first[0].Path, first[1].Path, first[2].Path})
	assert.False(t, first[2].ends(), "the large file starts in the volume the small ones left room in")
	assert.Greater(t, len(set.Files()[6].pieces), 2, "a file can span three volumes")
}

func TestVolumeWriter_NoLimit(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 300<<10, 0, 70<<10)
	sink := writeVolumes(t, 0, files, members)
	require.Len(t, sink.files, 1)
	checkVolumes(t, sink, files, members, 0)
}

// An empty file needs room only for its entry; with none left it opens the next volume.
func TestVolumeWriter_EmptyFileAtTheEndOfAVolume(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 100<<10, 0)
	sink := writeVolumes(t, MinFileSize, files, members)
	set := checkVolumes(t, sink, files, members, MinFileSize)
	last := set.Files()[1]
	assert.Equal(t, int64(0), last.Size)
	assert.Equal(t, 1, last.pieces[0].volume, "the empty file follows the end of the large one")
}

func TestVolumeWriter_RandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10)) //nolint:gosec // a fixed seed keeps failures reproducible
	spanning := 0
	for round := 0; round < 40; round++ {
		limits := []uint64{0, MinFileSize, MinFileSize + uint64(rng.IntN(1<<16)), 256 << 10}
		limit := limits[rng.IntN(len(limits))]
		sizes := make([]int, 1+rng.IntN(25))
		for i := range sizes {
			switch rng.IntN(4) {
			case 0:
				sizes[i] = 0
			case 1:
				sizes[i] = rng.IntN(300 << 10)
			default:
				sizes[i] = rng.IntN(20 << 10)
			}
		}
		files := memFiles{}
		members := randomFiles(rng, files, sizes...)
		set := checkVolumes(t, writeVolumes(t, limit, files, members), files, members, limit)
		for _, f := range set.Files() {
			if len(f.pieces) > 2 {
				spanning++
			}
		}
	}
	assert.NotZero(t, spanning, "some file spans three volumes or more")
}

func TestVolumeWriter_MinFileSize(t *testing.T) {
	for _, limit := range []uint64{1, MinFileSize - 1} {
		_, err := NewVolumeWriter(&memSink{}, "p", limit, memFiles{}.open)
		require.Error(t, err, limit)
		_, err = CountVolumes([]Member{{Path: "a", Size: 1}}, limit)
		require.Error(t, err, limit)
	}
	for _, limit := range []uint64{0, MinFileSize} {
		_, err := NewVolumeWriter(&memSink{}, "p", limit, memFiles{}.open)
		require.NoError(t, err, limit)
	}
}

func TestVolumeWriter_NoFileNoVolume(t *testing.T) {
	sink := &memSink{}
	v, err := NewVolumeWriter(sink, "p", 0, memFiles{}.open)
	require.NoError(t, err)
	require.NoError(t, v.Close())
	assert.Empty(t, sink.files)
}

func TestVolumeWriter_RejectsBadMembers(t *testing.T) {
	files := memFiles{"a": []byte("x"), "a/b": []byte("y"), "b": []byte("z")}
	cases := map[string][]Member{
		"negative size":       {{Path: "b", Size: -1}},
		"invalid path":        {{Path: "../b", Size: 1}},
		"repeated path":       {{Path: "a", Size: 1}, {Path: "a", Size: 1}},
		"file under a file":   {{Path: "a", Size: 1}, {Path: "a/b", Size: 1}},
		"source shorter":      {{Path: "b", Size: 2}},
		"source longer":       {{Path: "b", Size: 0}},
		"missing source file": {{Path: "c", Size: 1}},
	}
	for name, members := range cases {
		t.Run(name, func(t *testing.T) {
			v, err := NewVolumeWriter(&memSink{}, "p", 0, files.open)
			require.NoError(t, err)
			for _, m := range members {
				err = v.Add(context.Background(), m)
			}
			require.Error(t, err)
			assert.Equal(t, err, v.Close(), "the error sticks")
		})
	}
}

// The duplicate path is caught even when the first copy sits in an earlier volume.
func TestVolumeWriter_RejectsARepeatAcrossVolumes(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 100<<10)
	v, err := NewVolumeWriter(&memSink{}, "p", MinFileSize, files.open)
	require.NoError(t, err)
	require.NoError(t, v.Add(context.Background(), members[0]))
	require.Error(t, v.Add(context.Background(), members[0]))
}

func TestVolumeWriter_VolumeLimit(t *testing.T) {
	_, err := CountVolumes([]Member{{Path: "m", Size: maxVolumes * MinFileSize}}, MinFileSize)
	require.Error(t, err)
	n, err := CountVolumes([]Member{{Path: "m", Size: (maxVolumes - 1) * (MinFileSize - 64)}}, MinFileSize)
	require.NoError(t, err)
	assert.Less(t, n, uint32(maxVolumes))

	sink := &countingSink{}
	v, err := NewVolumeWriter(sink, "p", MinFileSize, func(string) (io.ReadCloser, error) {
		return io.NopCloser(&zeroReader{n: 2 * MinFileSize}), nil
	})
	require.NoError(t, err)
	v.plan.n = maxVolumes - 1
	err = v.Add(context.Background(), Member{Path: "m", Size: 2 * MinFileSize})
	require.Error(t, err)
	assert.Equal(t, 1, sink.started, "the last volume number is used, the next one is refused")
}

// countingSink discards volumes and counts them.
type countingSink struct{ started int }

func (s *countingSink) StartFile(string, uint32) error { s.started++; return nil }
func (s *countingSink) Write([]byte) error             { return nil }
func (s *countingSink) FinishFile(Summary) error       { return nil }

func TestVolumeWriter_SinkContract(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 14)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 3*WriteSize+17, 5)
	sink := writeVolumes(t, 0, files, members)
	assert.LessOrEqual(t, sink.maxWrite, WriteSize)
	// memSink scribbles over every slice after copying it; the volumes still verify.
	checkVolumes(t, sink, files, members, 0)
}

func TestVolumeWriter_Failures(t *testing.T) {
	t.Run("open fails", func(t *testing.T) {
		v, err := NewVolumeWriter(&memSink{}, "p", 0, func(string) (io.ReadCloser, error) { return nil, errSink })
		require.NoError(t, err)
		err = v.Add(context.Background(), Member{Path: "a", Size: 1})
		require.ErrorIs(t, err, errSink)
		require.ErrorIs(t, v.Close(), errSink)
	})
	t.Run("source fails", func(t *testing.T) {
		v, err := NewVolumeWriter(&memSink{}, "p", 0, func(string) (io.ReadCloser, error) {
			return io.NopCloser(iotest.ErrReader(errSink)), nil
		})
		require.NoError(t, err)
		require.ErrorIs(t, v.Add(context.Background(), Member{Path: "a", Size: 1}), errSink)
	})
	for _, stage := range []string{"start", "write", "finish"} {
		t.Run(stage+" fails", func(t *testing.T) {
			sink := &memSink{failOn: stage}
			files := memFiles{"g": []byte("abc"), "h": []byte("d")}
			v, err := NewVolumeWriter(sink, "p", 0, files.open)
			require.NoError(t, err)
			err = v.Add(context.Background(), Member{Path: "g", Size: 3})
			if err == nil {
				err = v.Close()
			}
			assert.True(t, errors.Is(err, errSink))
			assert.Equal(t, err, v.Add(context.Background(), Member{Path: "h", Size: 1}))
		})
	}
	t.Run("close twice", func(t *testing.T) {
		v, err := NewVolumeWriter(&memSink{}, "p", 0, memFiles{}.open)
		require.NoError(t, err)
		require.NoError(t, v.Close())
		require.Error(t, v.Close())
		require.Error(t, v.Add(context.Background(), Member{Path: "a"}))
	})
}

func TestCountVolumes_Errors(t *testing.T) {
	_, err := CountVolumes([]Member{{Path: "a", Size: -1}}, 0)
	require.Error(t, err)
	n, err := CountVolumes(nil, 0)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestVolumeWriter_CanceledContext(t *testing.T) {
	rng := rand.New(rand.NewPCG(15, 16)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 10, 10)
	sink := &memSink{}
	v, err := NewVolumeWriter(sink, "p", MinFileSize, files.open)
	require.NoError(t, err)
	require.NoError(t, v.Add(context.Background(), members[0]))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, v.Add(ctx, members[1]), context.Canceled)
	require.Len(t, sink.files, 1, "no volume is started")
	assert.False(t, sink.files[0].finished, "the open volume is not sealed either")

	// A cancel while a large file is copied stops it there.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	v, err = NewVolumeWriter(&memSink{}, "p", 0, func(string) (io.ReadCloser, error) {
		return io.NopCloser(&cancelingReader{cancel: cancel, left: 4 * WriteSize}), nil
	})
	require.NoError(t, err)
	require.ErrorIs(t, v.Add(ctx, Member{Path: "big", Size: 4 * WriteSize}), context.Canceled)
}

func TestVolumeWriter_CloseErrorOfMember(t *testing.T) {
	v, err := NewVolumeWriter(&memSink{}, "p", 0, func(string) (io.ReadCloser, error) {
		return closeFailing{strings.NewReader("abc")}, nil
	})
	require.NoError(t, err)
	require.ErrorIs(t, v.Add(context.Background(), Member{Path: "a", Size: 3}), errSink)
}

type closeFailing struct{ io.Reader }

func (closeFailing) Close() error { return errSink }

// A volume that cannot be sealed stops the writer before the next volume starts.
func TestVolumeWriter_SealError(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 18)) //nolint:gosec // a fixed seed keeps failures reproducible
	files := memFiles{}
	members := randomFiles(rng, files, 100<<10)
	sink := &memSink{failOn: "finish"}
	v, err := NewVolumeWriter(sink, "p", MinFileSize, files.open)
	require.NoError(t, err)
	err = v.Add(context.Background(), members[0])
	require.ErrorIs(t, err, errSink)
	require.Len(t, sink.files, 1, "the next volume is not started after the seal failed")
	assert.Equal(t, err, v.Close())
}
