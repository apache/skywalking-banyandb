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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The benchmarks in this file run on the local disk with files far larger than a page of
// metadata, the way an export and an import use the package.

// diskSink writes volumes as files under dir.
type diskSink struct {
	f     *os.File
	dir   string
	paths []string
}

func (s *diskSink) StartFile(rel string, _ uint32) error {
	f, err := os.Create(filepath.Join(s.dir, rel))
	if err != nil {
		return err
	}
	s.f = f
	s.paths = append(s.paths, f.Name())
	return nil
}

func (s *diskSink) Write(p []byte) error {
	_, err := s.f.Write(p)
	return err
}

func (s *diskSink) FinishFile(Summary) error { return s.f.Close() }

// zeroMembers returns count members of size bytes each and an opener that serves zero bytes.
func zeroMembers(count int, size int64) ([]Member, func(string) (io.ReadCloser, error)) {
	members := make([]Member, count)
	for i := range members {
		members[i] = Member{Path: fmt.Sprintf("%016x/file-%06d.bin", i/16, i), Size: size}
	}
	return members, func(string) (io.ReadCloser, error) { return io.NopCloser(&zeroReader{n: size}), nil }
}

// writeVolumesToDisk writes members as volumes of at most limit bytes under dir.
func writeVolumesToDisk(tb testing.TB, dir string, members []Member, open func(string) (io.ReadCloser, error), limit uint64) []string {
	sink := &diskSink{dir: dir}
	v, err := NewVolumeWriter(sink, "shard-0", limit, open)
	if err != nil {
		tb.Fatal(err)
	}
	for _, m := range members {
		if err = v.Add(context.Background(), m); err != nil {
			tb.Fatal(err)
		}
	}
	if err = v.Close(); err != nil {
		tb.Fatal(err)
	}
	return sink.paths
}

var diskShapes = []struct {
	name    string
	members int
	size    int64
	limit   uint64
}{
	{name: "1x1GiB/64MiB-volumes", members: 1, size: 1 << 30, limit: 64 << 20},
	{name: "1x1GiB/512MiB-volumes", members: 1, size: 1 << 30, limit: 512 << 20},
	{name: "64x16MiB/64MiB-volumes", members: 64, size: 16 << 20, limit: 64 << 20},
	{name: "100000x4KiB/64MiB-volumes", members: 100000, size: 4 << 10, limit: 64 << 20},
}

func BenchmarkVolumeWriterDisk(b *testing.B) {
	for _, shape := range diskShapes {
		b.Run(shape.name, func(b *testing.B) {
			members, open := zeroMembers(shape.members, shape.size)
			b.SetBytes(int64(shape.members) * shape.size)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dir := b.TempDir()
				writeVolumesToDisk(b, dir, members, open, shape.limit)
				b.StopTimer()
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkVolumeSetDisk opens the volumes of a container from disk and reads every file once,
// the way an importer does, or verifies every volume.
func BenchmarkVolumeSetDisk(b *testing.B) {
	for _, shape := range diskShapes {
		members, open := zeroMembers(shape.members, shape.size)
		paths := writeVolumesToDisk(b, b.TempDir(), members, open, shape.limit)
		total := int64(shape.members) * shape.size
		b.Run(shape.name+"/read-all", func(b *testing.B) {
			buf := make([]byte, 1<<20)
			b.SetBytes(total)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s, err := OpenVolumes(paths)
				if err != nil {
					b.Fatal(err)
				}
				for _, f := range s.Files() {
					r, openErr := s.Open(f)
					if openErr != nil {
						b.Fatal(openErr)
					}
					if _, err = io.CopyBuffer(struct{ io.Writer }{io.Discard}, r, buf); err != nil {
						b.Fatal(err)
					}
					if err = r.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run(shape.name+"/verify", func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s, err := OpenVolumes(paths)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = s.Verify(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// liveHeap returns the heap in use after a collection.
func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// BenchmarkMemoryPerMember reports the heap a writer and a volume set hold per member while
// they are in use, which is what grows with the number of files of a container.
func BenchmarkMemoryPerMember(b *testing.B) {
	for _, count := range []int{10000, 100000} {
		members, open := zeroMembers(count, 1)
		b.Run(fmt.Sprintf("%d-members/writer", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				before := liveHeap()
				v, err := NewVolumeWriter(&countingSink{}, "shard-0", 0, open)
				if err != nil {
					b.Fatal(err)
				}
				for _, m := range members {
					if err = v.Add(context.Background(), m); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(liveHeap()-before)/float64(count), "B/member")
				runtime.KeepAlive(v)
			}
		})
		paths := writeVolumesToDisk(b, b.TempDir(), members, open, 0)
		b.Run(fmt.Sprintf("%d-members/volume-set", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				before := liveHeap()
				s, err := OpenVolumes(paths)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(liveHeap()-before)/float64(count), "B/member")
				runtime.KeepAlive(s)
			}
		})
	}
}

// BenchmarkReadEveryFile opens the volumes of a container of count one-byte files from disk
// and reads every file, which costs a volume open per file when nothing is reused.
func BenchmarkReadEveryFile(b *testing.B) {
	for _, count := range []int{100, 1000, 10000} {
		members, open := zeroMembers(count, 1)
		paths := writeVolumesToDisk(b, b.TempDir(), members, open, 0)
		b.Run(fmt.Sprintf("%d-files", count), func(b *testing.B) {
			buf := make([]byte, 1<<20)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s, err := OpenVolumes(paths)
				if err != nil {
					b.Fatal(err)
				}
				for _, f := range s.Files() {
					r, openErr := s.Open(f)
					if openErr != nil {
						b.Fatal(openErr)
					}
					if _, err = io.CopyBuffer(struct{ io.Writer }{io.Discard}, r, buf); err != nil {
						b.Fatal(err)
					}
					if err = r.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if err = s.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
