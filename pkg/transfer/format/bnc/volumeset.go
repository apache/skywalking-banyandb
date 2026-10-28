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

// VolumeSet is the volumes of one container, opened in order and joined: a file split across
// volumes reads as one. Its methods may be called concurrently.
type VolumeSet struct {
	index   map[string]int
	volumes []*Container
	files   []File
}

// OpenVolumes opens the volume files at paths, given in volume order, as one VolumeSet. Close
// releases them.
func OpenVolumes(paths []string) (*VolumeSet, error) {
	volumes := make([]*Container, 0, len(paths))
	for _, p := range paths {
		c, err := OpenFile(p)
		if err != nil {
			closeAll(volumes)
			return nil, err
		}
		volumes = append(volumes, c)
	}
	s, err := NewVolumeSet(volumes)
	if err != nil {
		closeAll(volumes)
		return nil, err
	}
	return s, nil
}

func closeAll(volumes []*Container) {
	for _, c := range volumes {
		_ = c.Close()
	}
}

// NewVolumeSet joins opened volumes, given in volume order. It checks that they fit together:
// a file left unfinished at the end of a volume continues, at the next byte, as the first
// member of the next one; no other member continues a file; the last volume finishes its last
// file; and no path repeats or lies under another file across the volumes. A missing,
// repeated or misplaced volume breaks one of these and is ErrCorrupt. Close closes the volumes.
func NewVolumeSet(volumes []*Container) (*VolumeSet, error) {
	if len(volumes) == 0 {
		return nil, errors.New("a volume set needs at least one volume")
	}
	s := &VolumeSet{volumes: volumes, index: map[string]int{}}
	paths := newPathSet()
	var open *File // a file left unfinished by the previous volume
	for vi, c := range volumes {
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
	if open != nil {
		return nil, corruptf("file %q is left unfinished at the end of the last volume", open.Path)
	}
	return s, nil
}

func filePiecesEnd(f *File) int64 {
	last := f.pieces[len(f.pieces)-1].entry
	return last.FileOffset + last.Size
}

// Close closes every volume and returns the first error.
func (s *VolumeSet) Close() error {
	var errs []error
	for _, c := range s.volumes {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

// Files returns the files of the set in write order.
func (s *VolumeSet) Files() []File {
	return slices.Clone(s.files)
}

// Open returns a reader of the bytes of f, a file of s, across the volumes that hold it. Reading
// it to the end returns ErrChecksum instead of io.EOF when a piece does not match its CRC-32.
func (s *VolumeSet) Open(f File) (io.Reader, error) {
	i, ok := s.index[f.Path]
	if !ok || s.files[i].Size != f.Size {
		return nil, fmt.Errorf("%q is not a file of this volume set", f.Path)
	}
	pieces := s.files[i].pieces
	readers := make([]io.Reader, 0, len(pieces))
	for _, p := range pieces {
		readers = append(readers, s.volumes[p.volume].member(p.entry))
	}
	return io.MultiReader(readers...), nil
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

// Verify verifies every volume in order and returns their summaries.
func (s *VolumeSet) Verify(ctx context.Context) ([]Summary, error) {
	sums := make([]Summary, 0, len(s.volumes))
	for i, c := range s.volumes {
		sum, err := c.Verify(ctx)
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
