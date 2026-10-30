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
	"slices"
)

// File is one file of a VolumeSet, whole: its path, its size and the volume members that hold
// its bytes in order.
type File struct {
	Path   string
	pieces []filePiece
	Size   int64
}

type filePiece struct {
	entry  Entry
	volume int
}

// volume is one volume of a set: either opened by the caller, or a file opened only while it is
// read, so that a set of many volumes holds at most one descriptor per reader.
type volume struct {
	c    *Container // the caller's open volume; nil for one opened on demand
	path string
	tail []byte // footer and trailer as first read; a reopened file must match them
}

func (v *volume) open() (*Container, func(), error) {
	if v.c != nil {
		return v.c, func() {}, nil
	}
	c, err := OpenFile(v.path)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(c.tail, v.tail) {
		_ = c.Close()
		return nil, nil, corruptf("volume %s changed since the set was opened", v.path)
	}
	return c, func() { _ = c.Close() }, nil
}

// VolumeSet is the volumes of one container, joined in order: a file split across volumes
// reads as one. Its methods may be called concurrently.
type VolumeSet struct {
	index   map[string]int
	volumes []volume
	files   []File
}

// OpenVolumes reads the footers of the volume files at paths, given in volume order, and joins
// them into a VolumeSet. It keeps no file open: a volume is opened again only while a file in it
// is read or it is verified.
func OpenVolumes(paths []string) (*VolumeSet, error) {
	volumes := make([]volume, 0, len(paths))
	containers := make([]*Container, 0, len(paths))
	for _, p := range paths {
		c, err := OpenFile(p)
		if err != nil {
			return nil, err
		}
		if err = c.Close(); err != nil {
			return nil, err
		}
		volumes = append(volumes, volume{path: p, tail: c.tail})
		containers = append(containers, c)
	}
	return joinVolumes(volumes, containers)
}

// NewVolumeSet joins volumes the caller opened, given in volume order; Close closes them.
func NewVolumeSet(containers []*Container) (*VolumeSet, error) {
	volumes := make([]volume, 0, len(containers))
	for _, c := range containers {
		volumes = append(volumes, volume{c: c})
	}
	return joinVolumes(volumes, containers)
}

// joinVolumes checks that the volumes, whose footers containers hold, make one container:
// they are numbered 1, 2, … and only the last one says it is last, so a missing, repeated,
// misplaced or trailing volume is ErrCorrupt; a file left unfinished at the end of a volume
// continues, at the next byte, as the first member of the next one, and no other member
// continues a file; and no path repeats or lies under another file across the volumes.
func joinVolumes(volumes []volume, containers []*Container) (*VolumeSet, error) {
	if len(containers) == 0 {
		return nil, errors.New("a volume set needs at least one volume")
	}
	s := &VolumeSet{volumes: volumes, index: map[string]int{}}
	paths := newPathSet()
	var open *File // a file left unfinished by the previous volume
	for vi, c := range containers {
		if c.seq != uint32(vi+1) {
			return nil, corruptf("volume %d of the set is volume %d of its container", vi+1, c.seq)
		}
		if last := vi == len(containers)-1; c.last != last {
			if last {
				return nil, corruptf("the set ends at volume %d, which is not the last of its container", vi+1)
			}
			return nil, corruptf("volume %d is the last of its container, but more follow", vi+1)
		}
		for _, e := range c.entries {
			piece := filePiece{entry: e, volume: vi}
			if e.FileOffset > 0 {
				// Open checked that only the first member of a volume continues a file.
				if open == nil || open.Path != e.Path || open.Size != e.FileSize || filePiecesEnd(open) != e.FileOffset {
					return nil, corruptf("volume %d starts with %q at %d, which does not continue the previous volume", vi+1, e.Path, e.FileOffset)
				}
				open.pieces = append(open.pieces, piece)
			} else {
				if open != nil {
					return nil, corruptf("file %q is left unfinished at the end of volume %d", open.Path, vi)
				}
				if err := paths.add(e.Path); err != nil {
					return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
				}
				open = &File{Path: e.Path, Size: e.FileSize, pieces: []filePiece{piece}}
			}
			if e.ends() {
				s.index[open.Path] = len(s.files)
				s.files = append(s.files, *open)
				open = nil
			}
		}
	}
	// The last volume says it is last, and Open requires a last member to end its file only
	// when it is not last; check the end explicitly all the same.
	if open != nil {
		return nil, corruptf("file %q is left unfinished at the end of the last volume", open.Path)
	}
	return s, nil
}

func filePiecesEnd(f *File) int64 {
	last := f.pieces[len(f.pieces)-1].entry
	return last.FileOffset + last.Size
}

// Close closes the volumes the caller opened for NewVolumeSet and returns the first error. A set
// from OpenVolumes holds no open file.
func (s *VolumeSet) Close() error {
	var errs []error
	for _, v := range s.volumes {
		if v.c != nil {
			errs = append(errs, v.c.Close())
		}
	}
	return errors.Join(errs...)
}

// Files returns the files of the set in write order.
func (s *VolumeSet) Files() []File {
	return slices.Clone(s.files)
}

// Open returns a reader of the bytes of f, a file of s, across the volumes that hold it; each
// volume is open only while its piece is read, and Close releases the current one. Reading to
// the end returns ErrChecksum instead of io.EOF when a piece does not match its CRC-32.
func (s *VolumeSet) Open(f File) (io.ReadCloser, error) {
	i, ok := s.index[f.Path]
	if !ok || s.files[i].Size != f.Size {
		return nil, fmt.Errorf("%q is not a file of this volume set", f.Path)
	}
	return &fileReader{set: s, pieces: s.files[i].pieces}, nil
}

// fileReader reads the pieces of a file one after another, opening the volume of each in turn.
type fileReader struct {
	set     *VolumeSet
	cur     io.Reader
	release func()
	pieces  []filePiece
}

func (r *fileReader) Read(p []byte) (int, error) {
	for {
		if r.cur == nil {
			if len(r.pieces) == 0 {
				return 0, io.EOF
			}
			piece := r.pieces[0]
			c, release, err := r.set.volumes[piece.volume].open()
			if err != nil {
				return 0, err
			}
			r.cur, r.release, r.pieces = c.member(piece.entry), release, r.pieces[1:]
		}
		n, err := r.cur.Read(p)
		if errors.Is(err, io.EOF) {
			r.Close()
			if n == 0 {
				continue
			}
			err = nil
		}
		return n, err
	}
}

// Close releases the volume being read.
func (r *fileReader) Close() error {
	if r.release != nil {
		r.release()
	}
	r.cur, r.release = nil, nil
	return nil
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

// Verify verifies every volume in order, opening one at a time, and returns their summaries.
func (s *VolumeSet) Verify(ctx context.Context) ([]Summary, error) {
	sums := make([]Summary, 0, len(s.volumes))
	for i := range s.volumes {
		c, release, err := s.volumes[i].open()
		if err != nil {
			return nil, fmt.Errorf("volume %d: %w", i+1, err)
		}
		sum, err := c.Verify(ctx)
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
	metaName := coreMetaFile
	if partType != "" {
		dir = secondaryIndexDir + "/" + partType + "/" + dir
		metaName = secondaryIndexMeta
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
