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
	"slices"
	"strconv"
	"strings"
)

const (
	// MinFileSize is the smallest volume limit a VolumeWriter accepts, far above the trailer
	// and the footer entry of the longest path, so that every volume holds file bytes.
	MinFileSize = 64 << 10
	// maxVolumes bounds the volume numbers; names pad them to its width (volumeDigits).
	maxVolumes = 99999
	// VolumeExt is the extension of a volume file.
	VolumeExt = ".bnc"
)

// volumeDigits is the width volume numbers are zero-padded to, so that volume names sort in order.
var volumeDigits = len(strconv.Itoa(maxVolumes))

// Sink receives the volumes of a VolumeWriter. Write gets at most WriteSize bytes, and the
// slice is reused once Write returns: a Sink that keeps the bytes must copy them. A file is
// complete only when FinishFile follows its StartFile; after any error FinishFile is not
// called, and the caller must discard the file it started.
type Sink interface {
	StartFile(relativePath string, seq uint32) error
	Write(p []byte) error
	FinishFile(sum Summary) error
}

// Member is one file to pack: its path relative to the container root and its size.
type Member struct {
	Path string
	Size int64
}

// VolumeWriter packs files into volumes "<pathPrefix>-<seq>.bnc", seq printed with five digits
// from 1. Every volume is a complete container of at most maxFileSize bytes; it is filled up
// to that size, and a file that crosses the end of a volume continues as the first member of
// the next one, so the volumes in order hold every file whole. Its first error sticks.
type VolumeWriter struct {
	sink   Sink
	err    error
	open   func(path string) (io.ReadCloser, error)
	cur    *Writer
	paths  *pathSet
	prefix string
	buf    []byte
	plan   volumePlan
}

// NewVolumeWriter returns a VolumeWriter whose volume files are at most maxFileSize bytes on
// disk, footer and trailer included; 0 never splits, and any other value below MinFileSize is
// an error. open returns the bytes of a member by its path.
func NewVolumeWriter(sink Sink, pathPrefix string, maxFileSize uint64, open func(path string) (io.ReadCloser, error)) (*VolumeWriter, error) {
	if err := checkFileSize(maxFileSize); err != nil {
		return nil, err
	}
	return &VolumeWriter{sink: sink, prefix: pathPrefix, plan: volumePlan{limit: maxFileSize}, open: open, paths: newPathSet()}, nil
}

func checkFileSize(maxFileSize uint64) error {
	if maxFileSize != 0 && maxFileSize < MinFileSize {
		return fmt.Errorf("a volume limit of %d bytes is below the minimum of %d", maxFileSize, MinFileSize)
	}
	return nil
}

// Add writes the file m, splitting it across volumes where it crosses the end of one. Paths
// must be unique and none may be a directory of another, across all volumes.
func (v *VolumeWriter) Add(ctx context.Context, m Member) error {
	if v.err != nil {
		return v.err
	}
	// A canceled export must not seal a volume or start one it would abandon at once.
	if err := ctx.Err(); err != nil {
		return v.fail(err)
	}
	if m.Size < 0 {
		return v.fail(fmt.Errorf("member %q has a negative size %d", m.Path, m.Size))
	}
	if err := v.paths.add(m.Path); err != nil {
		return v.fail(err)
	}
	rc, err := v.open(m.Path)
	if err != nil {
		return v.fail(fmt.Errorf("open member %q: %w", m.Path, err))
	}
	err = v.copy(ctx, m, rc)
	if closeErr := rc.Close(); err == nil && closeErr != nil {
		err = v.fail(fmt.Errorf("close member %q: %w", m.Path, closeErr))
	}
	return err
}

func (v *VolumeWriter) copy(ctx context.Context, m Member, r io.Reader) error {
	for off := int64(0); ; {
		// Opening the source or sealing a volume may outlast a cancel: check before each step
		// that creates or finishes a volume.
		if err := ctx.Err(); err != nil {
			return v.fail(err)
		}
		take, seal, err := v.plan.place(m.Path, off, m.Size)
		if seal {
			if sealErr := v.seal(false); sealErr != nil {
				return sealErr
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return v.fail(ctxErr)
			}
		}
		if err != nil {
			return v.fail(fmt.Errorf("%s: %w", v.prefix, err))
		}
		if v.cur == nil {
			if err = v.start(); err != nil {
				return err
			}
		}
		// The last piece reads the source to its end, so the Writer refuses a file that grew
		// since it was listed.
		piece := r
		if off+take < m.Size {
			piece = io.LimitReader(r, take)
		}
		if err = v.cur.addPiece(ctx, m.Path, off, take, m.Size, piece); err != nil {
			return v.fail(err)
		}
		if off += take; off == m.Size {
			return nil
		}
	}
}

func (v *VolumeWriter) start() error {
	if err := v.sink.StartFile(VolumeName(v.prefix, v.plan.n), v.plan.n); err != nil {
		return v.fail(err)
	}
	if v.buf == nil {
		v.buf = make([]byte, WriteSize)
	}
	v.cur = NewWriter(sinkWriter{v.sink})
	v.cur.buf = v.buf
	v.cur.seq = v.plan.n
	// The VolumeWriter checks every path across all volumes; the volume need not keep them.
	v.cur.paths = nil
	return nil
}

// Close seals the last volume. A VolumeWriter that got no file produces no volume.
func (v *VolumeWriter) Close() error {
	if v.err != nil {
		return v.err
	}
	var err error
	if v.cur != nil {
		err = v.seal(true)
	}
	if err == nil {
		v.err = errClosed
	}
	return err
}

func (v *VolumeWriter) seal(last bool) error {
	sum, err := v.cur.close(last)
	if err != nil {
		return v.fail(err)
	}
	v.cur = nil
	if err = v.sink.FinishFile(sum); err != nil {
		return v.fail(err)
	}
	return nil
}

func (v *VolumeWriter) fail(err error) error {
	v.err = err
	return err
}

// volumePlan fills volumes up to the limit; VolumeWriter follows it while writing.
type volumePlan struct {
	limit   uint64
	members uint64 // member bytes of the open volume
	footer  int    // footer entry bytes of the open volume
	entries int    // members of the open volume
	n       uint32 // volumes started
	open    bool
}

// place returns how many bytes of file path, from fileOffset in a file of fileSize bytes, go
// into the open volume, starting volume n when none is open: all that is left when they fit,
// otherwise as many as fit, ending the volume. seal tells that the open volume is sealed first.
func (p *volumePlan) place(path string, fileOffset, fileSize int64) (take int64, seal bool, err error) {
	for {
		if !p.open {
			if p.n == maxVolumes {
				return 0, seal, fmt.Errorf("more than %d volumes", maxVolumes)
			}
			p.n++
			p.open, p.members, p.footer, p.entries = true, 0, 0, 0
		}
		entry := encodedEntrySize(path, fileOffset, fileSize)
		if take, ok := p.room(entry, fileSize-fileOffset); ok {
			p.members += uint64(take)
			p.footer += entry
			p.entries++
			return take, seal, nil
		}
		if p.entries == 0 {
			return 0, seal, fmt.Errorf("a volume of %d bytes cannot hold any byte of %q", p.limit, path)
		}
		seal, p.open = true, false
	}
}

// room returns how many of the remaining bytes of a file fit the open volume with an entry of
// entry bytes: all of them, or as many as fit when that is at least one byte. ok is false when
// the entry does not fit with a byte of a non-empty file, or at all.
func (p *volumePlan) room(entry int, remaining int64) (int64, bool) {
	footer := headerSize(p.n) + p.footer + entry + uvarintLen(uint64(p.entries+1))
	if p.entries >= maxEntries || footer > maxFooterSize {
		return 0, false
	}
	if p.limit == 0 {
		return remaining, true
	}
	used := p.members + uint64(footer+trailerSize)
	if used > p.limit {
		return 0, false
	}
	free := p.limit - used
	if uint64(remaining) <= free {
		return remaining, true
	}
	if free == 0 {
		return 0, false
	}
	return int64(free), true
}

// sinkWriter feeds a Sink in slices of at most WriteSize bytes.
type sinkWriter struct{ sink Sink }

func (s sinkWriter) Write(p []byte) (int, error) {
	for start := 0; start < len(p); start += WriteSize {
		if err := s.sink.Write(p[start:min(start+WriteSize, len(p))]); err != nil {
			return start, err
		}
	}
	return len(p), nil
}

// VolumeName is the path of volume seq (from 1) of the container at prefix: "<prefix>-NNNNN.bnc".
func VolumeName(prefix string, seq uint32) string {
	return fmt.Sprintf("%s-%0*d%s", prefix, volumeDigits, seq, VolumeExt)
}

// ParseVolumeName splits a volume path "<prefix>-NNNNN.bnc" into its container prefix and volume
// number; ok is false for any other path.
func ParseVolumeName(p string) (prefix string, seq uint32, ok bool) {
	name, found := strings.CutSuffix(p, VolumeExt)
	i := strings.LastIndex(name, "-")
	if !found || i < 0 || len(name)-i-1 != volumeDigits {
		return "", 0, false
	}
	n, err := strconv.ParseUint(name[i+1:], 10, 32)
	if err != nil || n == 0 {
		return "", 0, false
	}
	return name[:i], uint32(n), true
}

// GroupVolumes groups the volume paths among paths by container prefix, each group in volume
// order; a path that is not a volume name is left out.
func GroupVolumes(paths []string) map[string][]string {
	out := map[string][]string{}
	for _, p := range paths {
		if prefix, _, ok := ParseVolumeName(p); ok {
			out[prefix] = append(out[prefix], p)
		}
	}
	for _, vols := range out {
		slices.Sort(vols)
	}
	return out
}
