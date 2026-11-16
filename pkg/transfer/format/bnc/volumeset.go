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
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// File is one file of a VolumeSet, whole: its path and size, and where its pieces are.
type File struct {
	Path   string
	Size   int64
	index  int // position in VolumeSet.files
	first  int // the entry of its first piece in VolumeSet.entries; the others follow it
	pieces int
	volume int // the volume of its first piece; each further piece opens the next volume
}

// volume is one volume of a set: either opened by the caller, or a file opened on demand, so
// that a set of many volumes holds few descriptors.
type volume struct {
	c         *Container // the caller's open volume; nil for one opened on demand
	path      string
	first     int // its entries are VolumeSet.entries[first:end]
	end       int
	size      int64
	footerLen int64
	trailer   [trailerSize]byte // as first read; it carries the footer CRC-32
	seq       uint32
	last      bool
}

// VolumeSet is the volumes of one container, joined in order: a file split across volumes
// reads as one. Its methods may be called concurrently.
type VolumeSet struct {
	// idle is the last volume opened on demand, kept open while no reader uses it so that
	// reading the files of one volume one after another opens it once.
	idle    *Container
	entries []Entry // the entries of every volume, in volume order
	volumes []volume
	files   []File
	mu      sync.Mutex
	idleVol int
	idleUse int // readers of idle
}

// OpenVolumes reads the footers of the volume files at paths, given in volume order, and joins
// them into a VolumeSet. Volumes are opened again on demand to read files or verify; while no
// file is read, the set keeps at most one of them open, which Close releases.
func OpenVolumes(paths []string) (*VolumeSet, error) {
	containers := make([]*Container, 0, len(paths))
	for _, p := range paths {
		c, err := OpenFile(p)
		if err != nil {
			return nil, err
		}
		if err = c.Close(); err != nil {
			return nil, err
		}
		containers = append(containers, c)
	}
	return joinVolumes(containers, func(i int) volume { return volume{path: paths[i]} })
}

// NewVolumeSet joins volumes the caller opened, given in volume order; Close closes them.
func NewVolumeSet(containers []*Container) (*VolumeSet, error) {
	return joinVolumes(containers, func(i int) volume { return volume{c: containers[i]} })
}

// joinVolumes checks that the volumes make one container: they are numbered 1, 2, … and only
// the last one says it is last, so a missing, repeated, misplaced or trailing volume is
// ErrCorrupt; a file left unfinished at the end of a volume continues, at the next byte, as
// the first member of the next one, and no other member continues a file; and no path
// repeats or lies under another file across the volumes. Open already checked every path and
// the piece rules within each volume.
func joinVolumes(containers []*Container, newVolume func(i int) volume) (*VolumeSet, error) {
	if len(containers) == 0 {
		return nil, errors.New("a volume set needs at least one volume")
	}
	// One array holds the entries of every volume, sized once.
	total := 0
	for _, c := range containers {
		total += len(c.entries)
	}
	entries := make([]Entry, 0, total)
	volumes := make([]volume, len(containers))
	for i, c := range containers {
		v := newVolume(i)
		v.first, v.end = len(entries), len(entries)+len(c.entries)
		v.size, v.footerLen, v.trailer, v.seq, v.last = c.size, c.footerLen, c.trailer, c.seq, c.last
		volumes[i] = v
		entries = append(entries, c.entries...)
	}
	s := &VolumeSet{volumes: volumes, entries: entries, files: make([]File, 0, total), idleVol: -1}
	// Open checked the paths of each volume; only a set of several needs them checked across.
	var paths *pathSet
	if len(volumes) > 1 {
		paths = newPathSet()
	}
	var open *File // a file left unfinished by the previous volume
	for vi := range volumes {
		v := &volumes[vi]
		if v.seq != uint32(vi+1) {
			return nil, corruptf("volume %d of the set is volume %d of its container", vi+1, v.seq)
		}
		if last := vi == len(volumes)-1; v.last != last {
			if last {
				return nil, corruptf("the set ends at volume %d, which is not the last of its container", vi+1)
			}
			return nil, corruptf("volume %d is the last of its container, but more follow", vi+1)
		}
		for i := v.first; i < v.end; i++ {
			e := entries[i]
			if e.FileOffset > 0 {
				// Open checked that only the first member of a volume continues a file, so the
				// piece before it, when a file is open, is the last of the previous volume.
				if open == nil || open.Path != e.Path || open.Size != e.FileSize ||
					entries[i-1].FileOffset+entries[i-1].Size != e.FileOffset {
					return nil, corruptf("volume %d starts with %q at %d, which does not continue the previous volume", vi+1, e.Path, e.FileOffset)
				}
				open.pieces++
			} else {
				if open != nil {
					return nil, corruptf("file %q is left unfinished at the end of volume %d", open.Path, vi)
				}
				if paths != nil {
					if err := paths.insert(e.Path); err != nil {
						return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
					}
				}
				s.files = append(s.files, File{Path: e.Path, Size: e.FileSize, index: len(s.files), first: i, pieces: 1, volume: vi})
				open = &s.files[len(s.files)-1]
			}
			if e.ends() {
				open = nil
			}
		}
	}
	if open != nil {
		return nil, corruptf("file %q is left unfinished at the end of the last volume", open.Path)
	}
	return s, nil
}

// acquire returns volume i ready to read and a function that releases it. A volume opened on
// demand reuses the entries the set decoded: reopening only checks that the file still has the
// size and the trailer, which carries the footer CRC-32, it had then.
func (s *VolumeSet) acquire(i int) (*Container, func(), error) {
	v := &s.volumes[i]
	if v.c != nil {
		return v.c, func() {}, nil
	}
	s.mu.Lock()
	if s.idle != nil && s.idleVol == i {
		s.idleUse++
		c := s.idle
		s.mu.Unlock()
		return c, s.releaseIdle, nil
	}
	s.mu.Unlock()
	c, err := s.reopen(i)
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idleUse == 0 {
		// Nobody reads the idle volume: this one takes its place.
		if s.idle != nil {
			_ = s.idle.Close()
		}
		s.idle, s.idleVol, s.idleUse = c, i, 1
		return c, s.releaseIdle, nil
	}
	return c, func() { _ = c.Close() }, nil
}

func (s *VolumeSet) releaseIdle() {
	s.mu.Lock()
	s.idleUse--
	s.mu.Unlock()
}

func (s *VolumeSet) reopen(i int) (*Container, error) {
	v := &s.volumes[i]
	f, err := os.Open(v.path)
	if err != nil {
		return nil, err
	}
	c, err := s.checkReopened(f, v)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return c, nil
}

func (s *VolumeSet) checkReopened(f *os.File, v *volume) (*Container, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	var trailer [trailerSize]byte
	if st.Size() == v.size {
		if err = readFullAt(f, trailer[:], v.size-trailerSize); err != nil {
			return nil, err
		}
	}
	if st.Size() != v.size || trailer != v.trailer {
		return nil, corruptf("volume %s changed since the set was opened", v.path)
	}
	return &Container{
		r: f, closer: f, entries: s.entries[v.first:v.end],
		size: v.size, footerLen: v.footerLen, trailer: v.trailer, seq: v.seq, last: v.last,
	}, nil
}

// Close closes the volumes the caller opened for NewVolumeSet and the volume kept open for
// reading, and returns the first error.
func (s *VolumeSet) Close() error {
	var errs []error
	for _, v := range s.volumes {
		if v.c != nil {
			errs = append(errs, v.c.Close())
		}
	}
	s.mu.Lock()
	if s.idle != nil {
		errs = append(errs, s.idle.Close())
		s.idle, s.idleVol = nil, -1
	}
	s.mu.Unlock()
	return errors.Join(errs...)
}

// Files returns the files of the set in write order.
func (s *VolumeSet) Files() []File {
	return slices.Clone(s.files)
}

// VolumeEntries returns the number of entries in the footer of volume i of the set.
func (s *VolumeSet) VolumeEntries(i int) int {
	v := &s.volumes[i]
	return v.end - v.first
}

// Open returns a reader of the bytes of f, a file of s, across the volumes that hold it; each
// volume is open only while its piece is read. The Read that returns the last byte of a piece
// whose bytes do not match its CRC-32 returns ErrChecksum. Close releases the volume being read
// and returns the first error any Read returned, so a caller whose copy helper drops a final
// error, as io.CopyN and io.ReadFull do once they have all the bytes, still sees it: check the
// error of Close before trusting the bytes.
func (s *VolumeSet) Open(f File) (io.ReadCloser, error) {
	if f.index < 0 || f.index >= len(s.files) || s.files[f.index] != f {
		return nil, fmt.Errorf("%q is not a file of this volume set", f.Path)
	}
	return &fileReader{set: s, next: f.first, end: f.first + f.pieces, volume: f.volume}, nil
}

// fileReader reads the pieces of a file one after another, opening the volume of each in turn.
type fileReader struct {
	set     *VolumeSet
	cur     io.Reader
	err     error // the first error a Read returned, which Close returns
	release func()
	next    int // the entry of the next piece
	end     int
	volume  int // the volume of the next piece
}

func (r *fileReader) Read(p []byte) (int, error) {
	for {
		if r.cur == nil {
			if r.next == r.end {
				return 0, io.EOF
			}
			c, release, err := r.set.acquire(r.volume)
			if err != nil {
				return 0, r.fail(err)
			}
			r.cur, r.release = c.member(r.set.entries[r.next]), release
			r.next++
			r.volume++
		}
		n, err := r.cur.Read(p)
		if errors.Is(err, io.EOF) {
			r.releaseVolume()
			if n == 0 {
				continue
			}
			err = nil
		}
		if err != nil {
			return n, r.fail(err)
		}
		return n, nil
	}
}

func (r *fileReader) fail(err error) error {
	if r.err == nil {
		r.err = err
	}
	return err
}

func (r *fileReader) releaseVolume() {
	if r.release != nil {
		r.release()
	}
	r.cur, r.release = nil, nil
}

// Close releases the volume being read and returns the first error a Read returned.
func (r *fileReader) Close() error {
	r.releaseVolume()
	return r.err
}

// ReadFile reads a small file whole, such as the metadata.json of a part, refusing a file
// larger than limit bytes and checking its CRC-32.
func (s *VolumeSet) ReadFile(f File, limit int64) ([]byte, error) {
	if f.Size > limit {
		return nil, fmt.Errorf("file %q of %d bytes is larger than %d", f.Path, f.Size, limit)
	}
	r, err := s.Open(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data := make([]byte, f.Size)
	if _, err = io.ReadFull(r, data); err != nil {
		return nil, err
	}
	// The read past the last byte checks the CRC-32 of the last piece.
	if _, err = r.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = corruptf("file %q is longer than %d bytes", f.Path, f.Size)
		}
		return nil, err
	}
	return data, nil
}

// Verify verifies every volume in order, one at a time through one buffer, and returns their
// summaries.
func (s *VolumeSet) Verify(ctx context.Context) ([]Summary, error) {
	sums := make([]Summary, 0, len(s.volumes))
	var buf []byte
	for i := range s.volumes {
		c, release, err := s.acquire(i)
		if err != nil {
			return nil, fmt.Errorf("volume %d: %w", i+1, err)
		}
		n := int(min(verifyBlock, c.size))
		if len(buf) < n {
			buf = make([]byte, n)
		}
		sum, err := c.verify(ctx, buf[:n])
		release()
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil, err
			}
			return nil, fmt.Errorf("volume %d: %w", i+1, err)
		}
		sums = append(sums, sum)
	}
	return sums, nil
}

// PartGroup is one part of a shard container: its core files and those of its trace secondary
// index parts.
type PartGroup struct {
	// Subs holds the core part first, then the secondary index parts by index name.
	Subs   []SubPart
	PartID uint64
}

// SubPart is the core part or one secondary index part of a PartGroup.
type SubPart struct {
	// PartType is "" for the core part and the index name for a secondary index part.
	PartType string
	// Files are the other files of the sub part, by path.
	Files []File
	// Meta is the metadata.json of a core part or the manifest.json of a secondary index part.
	Meta File
}

// ShardLayout is a shard container split by kind.
type ShardLayout struct {
	Parts        []PartGroup
	Snapshots    []File
	ElementIndex []File
	Others       []File
	// Orphans are files of incomplete parts: a part without core files (its secondary index
	// outlived it), a core part without its metadata.json, or a secondary index part without
	// its manifest.json. A closed segment can hold such leftovers; a receiver cannot load
	// them, so an importer skips them.
	Orphans []File
}

// Shard groups the files of a shard container whatever their order in the volumes.
func (s *VolumeSet) Shard() *ShardLayout {
	b := bucketShard(s.files, filePath)
	layout := &ShardLayout{Snapshots: b.snapshots, ElementIndex: b.elementIndex, Others: b.others}
	for _, p := range b.parts {
		// The core part sorts first; without a usable one, none of the part can be loaded.
		var core SubPart
		ok := false
		if p.types[0].partType == "" {
			core, ok = subPart(p.id, "", p.types[0].items)
		}
		if !ok {
			for _, t := range p.types {
				layout.Orphans = append(layout.Orphans, t.items...)
			}
			continue
		}
		g := PartGroup{PartID: p.id, Subs: []SubPart{core}}
		for _, t := range p.types[1:] {
			if sub, subOK := subPart(p.id, t.partType, t.items); subOK {
				g.Subs = append(g.Subs, sub)
			} else {
				layout.Orphans = append(layout.Orphans, t.items...)
			}
		}
		layout.Parts = append(layout.Parts, g)
	}
	for _, list := range [][]File{layout.Snapshots, layout.Others, layout.Orphans} {
		sortByPath(list, filePath)
	}
	sortNative(layout.ElementIndex, filePath)
	return layout
}

// subPart splits the files of one sub part into its metadata file and the rest; ok is false
// when the metadata file is missing.
func subPart(id uint64, partType string, files []File) (SubPart, bool) {
	dir := fmt.Sprintf("%016x/", id)
	metaName := nativefmt.CoreMetaFile
	if partType != "" {
		dir = nativefmt.SecondaryIndexDir + "/" + partType + "/" + dir
		metaName = nativefmt.SecondaryMetaFile
	}
	sub := SubPart{PartType: partType}
	for _, f := range files {
		if f.Path == dir+metaName {
			sub.Meta = f
			continue
		}
		sub.Files = append(sub.Files, f)
	}
	sortByPath(sub.Files, filePath)
	return sub, sub.Meta.Path != ""
}

func filePath(f File) string { return f.Path }
