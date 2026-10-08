// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/blevesearch/vellum"
)

// pagedFST reads a term dictionary -- a vellum version 1 FST -- in place in
// its segment file, one state at a time, instead of loading the whole FST
// into the heap. A lookup reads only the states on its key's path and an
// iteration only the states it visits, through small reads the OS page
// cache serves, so a persisted segment keeps no dictionary bytes in memory.
//
// The decoding follows vellum's version 1 encoding (encoder_v1.go and
// decoder_v1.go in github.com/blevesearch/vellum, Apache-2.0): a state is
// addressed by its last byte and its fields are laid out below it, and
// transition destinations are deltas from a state's lowest byte. The
// differential tests check every lookup and iteration against vellum itself.
type pagedFST struct {
	file segmentFile
	path string
	// top holds the dictionary's last pagedFSTTopSize bytes, where the root
	// and the states every lookup passes through live; it is the only part
	// kept in memory.
	top      []byte
	topStart uint64
	base     uint64
	length   uint64
	root     uint64
	count    int
}

const (
	pagedFSTHeaderSize  = 16
	pagedFSTFooterSize  = 16
	pagedFSTVersion     = 1
	pagedFSTEmptyAddr   = 0
	pagedFSTNoneAddr    = 1
	pagedFSTSingleFlag  = 1 << 7
	pagedFSTNextFlag    = 1 << 6
	pagedFSTFinalFlag   = 1 << 6
	pagedFSTMaxCommon   = 1<<6 - 1
	pagedFSTMaxNumTrans = 1<<6 - 1
)

// pagedFSTCommonInputs maps vellum's one-byte "common input" codes back to
// the input byte they stand for (vellum's commonInputsInv).
var pagedFSTCommonInputs = [256]byte{
	0x74, 0x65, 0x2f, 0x6f, 0x61, 0x73, 0x72, 0x69, 0x70, 0x63, 0x6e, 0x77, 0x2e, 0x68, 0x6c, 0x6d,
	0x2d, 0x64, 0x75, 0x30, 0x31, 0x32, 0x67, 0x3d, 0x3a, 0x62, 0x66, 0x33, 0x79, 0x35, 0x26, 0x5f,
	0x34, 0x76, 0x39, 0x36, 0x37, 0x38, 0x6b, 0x25, 0x3f, 0x78, 0x43, 0x44, 0x41, 0x53, 0x46, 0x49,
	0x42, 0x45, 0x6a, 0x50, 0x54, 0x7a, 0x52, 0x4e, 0x4d, 0x2b, 0x4c, 0x4f, 0x71, 0x48, 0x47, 0x57,
	0x55, 0x56, 0x2c, 0x59, 0x4b, 0x4a, 0x5a, 0x58, 0x51, 0x3b, 0x29, 0x28, 0x7e, 0x5b, 0x5d, 0x24,
	0x21, 0x27, 0x2a, 0x40, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
	0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
	0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x22, 0x23, 0x3c, 0x3e, 0x5c, 0x5e, 0x60, 0x7b, 0x7c, 0x7d, 0x7f,
	0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
	0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
	0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf,
	0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf,
	0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
	0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
	0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef,
	0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff,
}

func openPagedFST(file segmentFile, path string, base, length uint64) (*pagedFST, error) {
	if length < pagedFSTHeaderSize+pagedFSTFooterSize {
		return nil, corruptError("segment %q has a truncated term dictionary", path)
	}
	var header [pagedFSTHeaderSize]byte
	if readErr := readSegmentBytes(file, base, header[:], path); readErr != nil {
		return nil, readErr
	}
	if version := binary.LittleEndian.Uint64(header[0:8]); version != pagedFSTVersion {
		return nil, corruptError("segment %q has term dictionary version %d", path, version)
	}
	var footer [pagedFSTFooterSize]byte
	if readErr := readSegmentBytes(file, base+length-pagedFSTFooterSize, footer[:], path); readErr != nil {
		return nil, readErr
	}
	count := binary.LittleEndian.Uint64(footer[0:8])
	root := binary.LittleEndian.Uint64(footer[8:16])
	if root >= length-pagedFSTFooterSize && root != pagedFSTEmptyAddr && root != pagedFSTNoneAddr {
		return nil, corruptError("segment %q has a term dictionary root outside it", path)
	}
	if count > math.MaxInt32 {
		return nil, corruptError("segment %q has an implausible term dictionary length", path)
	}
	fst := &pagedFST{file: file, path: path, base: base, length: length, root: root, count: int(count)}
	fst.topStart = length - min(length, pagedFSTTopSize)
	fst.top = make([]byte, length-fst.topStart)
	if readErr := readSegmentBytes(file, base+fst.topStart, fst.top, path); readErr != nil {
		return nil, readErr
	}
	return fst, nil
}

func isReaderClosedError(err error) bool {
	return errors.Is(err, ErrReaderClosed)
}

// lookupError classifies a failed dictionary lookup: a closed reader stays
// ErrReaderClosed, anything else is corruption.
func lookupError(path string, err error) error {
	if isReaderClosedError(err) {
		return err
	}
	return corruptError("look up term in segment %q", path, err)
}

func readSegmentBytes(file segmentFile, offset uint64, destination []byte, path string) error {
	read, readErr := file.ReadAt(destination, int64(offset))
	if readErr != nil {
		if isReaderClosedError(readErr) {
			return readErr
		}
		return corruptError("read segment %q", path, readErr)
	}
	if read != len(destination) {
		return corruptError("read segment %q: short read", path)
	}
	return nil
}

// fstNode is one decoded state: its transitions in ascending input order,
// with absolute destinations and outputs.
type fstNode struct {
	keys        []byte
	dests       []uint64
	outs        []uint64
	finalOutput uint64
	final       bool
}

// fstWindow reads an FST through a few aligned pages: an operation's
// states cluster in a small set of regions it alternates between -- a
// lookup's path; for an iteration, the subtree it walks and the suffix
// states vellum shares between neighboring keys, which lie anywhere in the
// region compiled shortly before them -- so a handful of pages turns
// repeated reads of one region into one. Page buffers are allocated on first
// use, so a short walk holds only the pages it touched.
type fstWindow struct {
	fst     *pagedFST
	pages   []fstPage
	scratch []byte
	clock   uint64
	last    int
	shift   uint
}

type fstPage struct {
	data  []byte
	index uint64
	used  uint64
	valid bool
}

// pagedFSTTopSize is how many bytes at the end of a dictionary -- the root
// and the states nearest it -- stay in memory.
const pagedFSTTopSize = 4 << 10

// fstWindowShape is a window's page size (as a shift) and page count.
type fstWindowShape struct {
	shift uint
	slots int
}

var (
	// A lookup touches one state per key byte, clustered on a few pages.
	pagedFSTLookupWindow = fstWindowShape{shift: 12, slots: 8}
	// An iteration's working set is the region compiled shortly before the
	// keys it visits; see fstWindow. 64 pages of 4 KiB hold it for
	// high-entropy keys, where 32 pages already read a page per term.
	pagedFSTIteratorWindow = fstWindowShape{shift: 12, slots: 64}
)

// fstWindowBudget bounds the page buffers of the iterators one k-way walk
// -- a merge, or a walk over every segment of a generation -- holds open
// together, so its transient memory does not grow with its fan-in. Narrower
// windows only cost re-reads, served by the page cache. It is a variable
// only so tests can compare budgets.
var fstWindowBudget = 1 << 20

// iteratorWindow returns the window shape for one of fanIn iterators open
// together: the default up to fstWindowBudget in total, smaller and finer
// pages beyond, down to 16 KiB per iterator.
func iteratorWindow(fanIn int) fstWindowShape {
	full := pagedFSTIteratorWindow
	if fanIn <= 1 || fanIn*full.slots<<full.shift <= fstWindowBudget {
		return full
	}
	share := fstWindowBudget / fanIn
	switch {
	case share >= 128<<10:
		return fstWindowShape{shift: 11, slots: 64}
	case share >= 16<<10:
		return fstWindowShape{shift: 9, slots: min(share>>9, 128)}
	default:
		return fstWindowShape{shift: 9, slots: 32}
	}
}

// searchDictionary opens an iterator over the keys of dictionary in
// [start, end) that automaton (nil: every key) accepts, as one of fanIn
// iterators open together.
func searchDictionary(dictionary termIndex, automaton vellum.Automaton, start, end []byte, fanIn int) (termIterator, error) {
	if paged, ok := dictionary.(*pagedFST); ok {
		return paged.search(automaton, start, end, iteratorWindow(fanIn))
	}
	if automaton == nil {
		return dictionary.Iterator(start, end)
	}
	return dictionary.Search(automaton, start, end)
}

// page returns page index, reading it on a miss into the least recently
// used slot.
func (w *fstWindow) page(index uint64) ([]byte, error) {
	if page := &w.pages[w.last]; page.valid && page.index == index {
		return page.data, nil
	}
	w.clock++
	victim := 0
	for slot := range w.pages {
		page := &w.pages[slot]
		if page.valid && page.index == index {
			page.used = w.clock
			w.last = slot
			return page.data, nil
		}
		if !page.valid || page.used < w.pages[victim].used || (w.pages[victim].valid && !page.valid) {
			victim = slot
		}
	}
	page := &w.pages[victim]
	size := uint64(1) << w.shift
	start := index << w.shift
	length := min(size, w.fst.length-start)
	if uint64(cap(page.data)) < size {
		page.data = make([]byte, size)
	}
	page.data = page.data[:length]
	page.valid = false
	if readErr := readSegmentBytes(w.fst.file, w.fst.base+start, page.data, w.fst.path); readErr != nil {
		return nil, readErr
	}
	page.index, page.used, page.valid = index, w.clock, true
	w.last = victim
	return page.data, nil
}

// bytes returns the FST bytes [low, high].
func (w *fstWindow) bytes(low, high uint64) ([]byte, error) {
	if low > high || high >= w.fst.length {
		return nil, corruptError("segment %q has a term dictionary state outside it", w.fst.path)
	}
	top := w.fst.topStart
	if low >= top {
		return w.fst.top[low-top : high-top+1], nil
	}
	if first := low >> w.shift; first == high>>w.shift && high < top {
		data, pageErr := w.page(first)
		if pageErr != nil {
			return nil, pageErr
		}
		offset := low - first<<w.shift
		return data[offset : offset+high-low+1], nil
	}
	w.scratch = w.scratch[:0]
	for position := low; position <= high; {
		if position >= top {
			w.scratch = append(w.scratch, w.fst.top[position-top:high-top+1]...)
			break
		}
		index := position >> w.shift
		data, pageErr := w.page(index)
		if pageErr != nil {
			return nil, pageErr
		}
		pageStart := index << w.shift
		to := min(high, pageStart+uint64(len(data))-1)
		w.scratch = append(w.scratch, data[position-pageStart:to-pageStart+1]...)
		position = to + 1
	}
	return w.scratch, nil
}

func (w *fstWindow) byteAt(position uint64) (byte, error) {
	data, readErr := w.bytes(position, position)
	if readErr != nil {
		return 0, readErr
	}
	return data[0], nil
}

func packedUint(data []byte) uint64 {
	var value uint64
	for index, b := range data {
		value |= uint64(b) << (uint(index) * 8)
	}
	return value
}

// node decodes the state at addr into node, reusing its slices.
//
//nolint:gocyclo // the two state encodings are decoded explicitly.
func (w *fstWindow) node(addr uint64, node *fstNode) error {
	node.keys, node.dests, node.outs = node.keys[:0], node.dests[:0], node.outs[:0]
	node.final, node.finalOutput = false, 0
	switch addr {
	case pagedFSTEmptyAddr:
		node.final = true
		return nil
	case pagedFSTNoneAddr:
		return nil
	}
	if addr < pagedFSTHeaderSize {
		return corruptError("segment %q has a term dictionary state at %d", w.fst.path, addr)
	}
	head, readErr := w.byteAt(addr)
	if readErr != nil {
		return readErr
	}
	position := addr
	below := func(count uint64) error {
		if position < pagedFSTHeaderSize+count {
			return corruptError("segment %q has a term dictionary state below its header", w.fst.path)
		}
		position -= count
		return nil
	}
	if head&pagedFSTSingleFlag != 0 {
		input := head & pagedFSTMaxCommon
		if input == 0 {
			if moveErr := below(1); moveErr != nil {
				return moveErr
			}
			if input, readErr = w.byteAt(position); readErr != nil {
				return readErr
			}
		} else {
			input = pagedFSTCommonInputs[input-1]
		}
		var dest, out uint64
		if head&pagedFSTNextFlag != 0 {
			dest = position - 1
		} else {
			if moveErr := below(1); moveErr != nil {
				return moveErr
			}
			pack, packErr := w.byteAt(position)
			if packErr != nil {
				return packErr
			}
			transSize, outSize := uint64(pack>>4), uint64(pack&0x0f)
			if moveErr := below(transSize); moveErr != nil {
				return moveErr
			}
			if transSize > 0 {
				raw, rawErr := w.bytes(position, position+transSize-1)
				if rawErr != nil {
					return rawErr
				}
				dest = packedUint(raw)
			}
			if outSize > 0 {
				if moveErr := below(outSize); moveErr != nil {
					return moveErr
				}
				outRaw, outErr := w.bytes(position, position+outSize-1)
				if outErr != nil {
					return outErr
				}
				out = packedUint(outRaw)
			}
			if dest != 0 {
				dest = position - dest
			}
		}
		node.keys = append(node.keys, input)
		node.dests = append(node.dests, dest)
		node.outs = append(node.outs, out)
		return nil
	}
	node.final = head&pagedFSTFinalFlag != 0
	numTrans := uint64(head & pagedFSTMaxNumTrans)
	if numTrans == 0 {
		if moveErr := below(1); moveErr != nil {
			return moveErr
		}
		count, countErr := w.byteAt(position)
		if countErr != nil {
			return countErr
		}
		numTrans = uint64(count)
		if numTrans == 1 {
			numTrans = 256
		}
	}
	if moveErr := below(1); moveErr != nil {
		return moveErr
	}
	pack, packErr := w.byteAt(position)
	if packErr != nil {
		return packErr
	}
	transSize, outSize := uint64(pack>>4), uint64(pack&0x0f)
	if moveErr := below(numTrans); moveErr != nil {
		return moveErr
	}
	keysBottom := position
	if moveErr := below(numTrans * transSize); moveErr != nil {
		return moveErr
	}
	destsBottom := position
	outsBottom, finalBottom := uint64(0), uint64(0)
	if outSize > 0 {
		if moveErr := below(numTrans * outSize); moveErr != nil {
			return moveErr
		}
		outsBottom = position
		if node.final {
			if moveErr := below(outSize); moveErr != nil {
				return moveErr
			}
			finalBottom = position
		}
	}
	bottom := position
	state, stateErr := w.bytes(bottom, addr)
	if stateErr != nil {
		return stateErr
	}
	at := func(offset, size uint64) uint64 {
		return packedUint(state[offset-bottom : offset-bottom+size])
	}
	for logical := uint64(0); logical < numTrans; logical++ {
		physical := numTrans - logical - 1
		node.keys = append(node.keys, state[keysBottom-bottom+physical])
		dest := at(destsBottom+physical*transSize, transSize)
		if dest != 0 {
			if dest > bottom {
				return corruptError("segment %q has a term dictionary transition below its start", w.fst.path)
			}
			dest = bottom - dest
		}
		node.dests = append(node.dests, dest)
		var out uint64
		if outSize > 0 {
			out = at(outsBottom+physical*outSize, outSize)
		}
		node.outs = append(node.outs, out)
	}
	if node.final && outSize > 0 {
		node.finalOutput = at(finalBottom, outSize)
	}
	return nil
}

// fstLookup is the reusable working set of exact lookups: one lookup, or
// every lookup of one batch (see termLookups).
//
//nolint:govet // the window and the node it decodes into stay adjacent.
type fstLookup struct {
	window fstWindow
	node   fstNode
}

var fstLookupPool = sync.Pool{New: func() any { return &fstLookup{} }}

func acquireFSTLookup(fst *pagedFST) *fstLookup {
	lookup := fstLookupPool.Get().(*fstLookup)
	lookup.window.reset(fst, pagedFSTLookupWindow)
	return lookup
}

// releaseFSTLookup returns lookup to the pool, dropping its dictionary so a
// pooled entry does not keep a closed segment's reader reachable.
func releaseFSTLookup(lookup *fstLookup) {
	lookup.window.fst = nil
	fstLookupPool.Put(lookup)
}

// reset points the window at fst with the given shape, keeping its page
// buffers for reuse when the shape is unchanged.
func (w *fstWindow) reset(fst *pagedFST, shape fstWindowShape) {
	w.fst = fst
	w.clock, w.last = 0, 0
	if len(w.pages) != shape.slots || w.shift != shape.shift {
		w.pages = make([]fstPage, shape.slots)
		w.shift = shape.shift
	}
	for slot := range w.pages {
		w.pages[slot].valid = false
		w.pages[slot].used = 0
	}
}

func (n *fstNode) transitionFor(input byte) int {
	return bytes.IndexByte(n.keys, input)
}

// Get returns term's value and whether the dictionary holds it.
func (f *pagedFST) Get(term []byte) (uint64, bool, error) {
	lookup := acquireFSTLookup(f)
	defer releaseFSTLookup(lookup)
	return lookup.get(term)
}

// get looks term up through the lookup's window, whose pages stay valid
// across the lookups of one batch.
func (lookup *fstLookup) get(term []byte) (uint64, bool, error) {
	f := lookup.window.fst
	window, node := &lookup.window, &lookup.node
	if nodeErr := window.node(f.root, node); nodeErr != nil {
		return 0, false, nodeErr
	}
	var total uint64
	for _, input := range term {
		index := node.transitionFor(input)
		if index < 0 {
			return 0, false, nil
		}
		total += node.outs[index]
		if nodeErr := window.node(node.dests[index], node); nodeErr != nil {
			return 0, false, nodeErr
		}
	}
	if !node.final {
		return 0, false, nil
	}
	return total + node.finalOutput, true, nil
}

// Len returns the number of terms in the dictionary.
func (f *pagedFST) Len() int { return f.count }

// Iterator iterates every term in [start, end).
func (f *pagedFST) Iterator(start, end []byte) (termIterator, error) {
	return f.Search(nil, start, end)
}

// Search returns an iterator over the keys in [start, end) the automaton
// accepts, in lexicographic order, positioned on the first; it reports
// vellum.ErrIteratorDone when there is none, as vellum's Search does.
func (f *pagedFST) Search(automaton vellum.Automaton, start, end []byte) (termIterator, error) {
	return f.search(automaton, start, end, pagedFSTIteratorWindow)
}

func (f *pagedFST) search(automaton vellum.Automaton, start, end []byte, shape fstWindowShape) (termIterator, error) {
	if automaton == nil {
		automaton = alwaysMatch{}
	}
	iterator := &pagedIterator{automaton: automaton, start: start, end: end, window: fstIteratorWindows.Get().(*fstWindow)}
	iterator.window.reset(f, shape)
	if pointErr := iterator.pointTo(start); pointErr != nil {
		_ = iterator.Close()
		return nil, pointErr
	}
	return iterator, nil
}

type alwaysMatch struct{}

func (alwaysMatch) Start() int               { return 0 }
func (alwaysMatch) IsMatch(int) bool         { return true }
func (alwaysMatch) CanMatch(int) bool        { return true }
func (alwaysMatch) WillAlwaysMatch(int) bool { return true }
func (alwaysMatch) Accept(int, byte) int     { return 0 }

// pagedIterator mirrors vellum's FSTIterator over decoded states read
// through a window.
type pagedIterator struct {
	automaton vellum.Automaton
	window    *fstWindow
	start     []byte
	end       []byte
	nodes     []*fstNode
	spare     []*fstNode
	keys      []byte
	positions []int
	values    []uint64
	autStates []int
	nextStart []byte
}

func (i *pagedIterator) push(addr uint64) error {
	var node *fstNode
	if count := len(i.spare); count > 0 {
		node, i.spare = i.spare[count-1], i.spare[:count-1]
	} else {
		node = &fstNode{}
	}
	if nodeErr := i.window.node(addr, node); nodeErr != nil {
		return nodeErr
	}
	i.nodes = append(i.nodes, node)
	return nil
}

func (i *pagedIterator) pop(count int) {
	for index := len(i.nodes) - count; index < len(i.nodes); index++ {
		i.spare = append(i.spare, i.nodes[index])
	}
	i.nodes = i.nodes[:len(i.nodes)-count]
	i.keys = i.keys[:len(i.keys)-count]
	i.positions = i.positions[:len(i.positions)-count]
	i.values = i.values[:len(i.values)-count]
	i.autStates = i.autStates[:len(i.autStates)-count]
}

func (i *pagedIterator) pointTo(key []byte) error {
	if bytes.Compare(key, i.start) < 0 {
		key = i.start
	}
	if i.end != nil && bytes.Compare(key, i.end) > 0 {
		key = i.end
	}
	if pushErr := i.push(i.window.fst.root); pushErr != nil {
		return pushErr
	}
	i.autStates = append(i.autStates, i.automaton.Start())
	lastBefore := -1
	for _, input := range key {
		current := i.nodes[len(i.nodes)-1]
		index := current.transitionFor(input)
		if index < 0 {
			for candidate := len(current.keys) - 1; candidate >= 0; candidate-- {
				if current.keys[candidate] < input {
					lastBefore = candidate
					break
				}
			}
			break
		}
		autNext := i.automaton.Accept(i.autStates[len(i.autStates)-1], input)
		if pushErr := i.push(current.dests[index]); pushErr != nil {
			return pushErr
		}
		i.keys = append(i.keys, input)
		i.positions = append(i.positions, index)
		i.values = append(i.values, current.outs[index])
		i.autStates = append(i.autStates, autNext)
	}
	if !i.nodes[len(i.nodes)-1].final || !i.automaton.IsMatch(i.autStates[len(i.autStates)-1]) ||
		bytes.Compare(i.keys, key) < 0 {
		return i.next(lastBefore)
	}
	return nil
}

func (i *pagedIterator) Current() ([]byte, uint64) {
	current := i.nodes[len(i.nodes)-1]
	if !current.final {
		return nil, 0
	}
	var total uint64
	for _, value := range i.values {
		total += value
	}
	return i.keys, total + current.finalOutput
}

func (i *pagedIterator) Next() error {
	if i.window == nil {
		return vellum.ErrIteratorDone
	}
	return i.next(-1)
}

func (i *pagedIterator) next(lastOffset int) error {
	i.nextStart = append(i.nextStart[:0], i.keys...)
	nextOffset := lastOffset + 1
	allowCompare := false
	for {
		current := i.nodes[len(i.nodes)-1]
		autCurrent := i.autStates[len(i.autStates)-1]
		if current.final && i.automaton.IsMatch(autCurrent) && allowCompare {
			if i.end != nil && bytes.Compare(i.keys, i.end) >= 0 {
				return vellum.ErrIteratorDone
			}
			if bytes.Compare(i.keys, i.nextStart) > 0 {
				return nil
			}
		}
		descended := false
		for nextOffset < len(current.keys) {
			input := current.keys[nextOffset]
			autNext := i.automaton.Accept(autCurrent, input)
			if !i.automaton.CanMatch(autNext) {
				nextOffset++
				continue
			}
			dest, value := current.dests[nextOffset], current.outs[nextOffset]
			if pushErr := i.push(dest); pushErr != nil {
				return pushErr
			}
			i.keys = append(i.keys, input)
			i.positions = append(i.positions, nextOffset)
			i.values = append(i.values, value)
			i.autStates = append(i.autStates, autNext)
			nextOffset = 0
			allowCompare = true
			descended = true
			break
		}
		if descended {
			continue
		}
		if len(i.nodes) <= 1 {
			return vellum.ErrIteratorDone
		}
		popCount := 1
		for index := len(i.nodes) - 1; index > 0; index-- {
			if index == 1 || len(i.nodes[index].keys) != 1 {
				popCount = max(len(i.nodes)-1-index, 1)
				break
			}
		}
		nextOffset = i.positions[len(i.positions)-popCount] + 1
		allowCompare = false
		i.pop(popCount)
	}
}

// Close returns the iterator's page buffers for reuse.
func (i *pagedIterator) Close() error {
	if i.window != nil {
		i.window.fst = nil
		fstIteratorWindows.Put(i.window)
		i.window = nil
	}
	return nil
}

var fstIteratorWindows = sync.Pool{New: func() any { return &fstWindow{} }}

// termIterator walks a term dictionary in order; both vellum's FSTIterator
// and pagedIterator implement it.
type termIterator interface {
	Current() ([]byte, uint64)
	Next() error
	Close() error
}

// termIndex is a field's term dictionary, read in place from a persisted
// segment (pagedFST) or from an admitted segment's payload (vellumIndex).
type termIndex interface {
	Get(term []byte) (uint64, bool, error)
	Len() int
	Iterator(start, end []byte) (termIterator, error)
	Search(automaton vellum.Automaton, start, end []byte) (termIterator, error)
}

// termLookups looks up the terms of one batch in one dictionary. For a
// dictionary read in place it shares one page window across the batch, so
// the states near the root every lookup passes through are read once per
// batch rather than once per term; the window lives only for the batch.
type termLookups struct {
	dictionary termIndex
	paged      *fstLookup
}

func newTermLookups(dictionary termIndex) termLookups {
	lookups := termLookups{dictionary: dictionary}
	if paged, ok := dictionary.(*pagedFST); ok {
		lookups.paged = acquireFSTLookup(paged)
	}
	return lookups
}

// get looks term up. Like lookupTermPosting it turns a panic decoding a
// damaged dictionary into an error, and then drops the shared window's pages
// so no later lookup of the batch reads state the panic left behind.
func (l *termLookups) get(term []byte) (postingOffset uint64, exists bool, err error) {
	if l.paged == nil {
		return lookupTermPosting(l.dictionary, term)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			postingOffset, exists = 0, false
			err = fmt.Errorf("term dictionary lookup panicked: %v", recovered)
			l.paged.window.reset(l.paged.window.fst, pagedFSTLookupWindow)
		}
	}()
	return l.paged.get(term)
}

func (l *termLookups) close() {
	if l.paged != nil {
		releaseFSTLookup(l.paged)
		l.paged = nil
	}
}

// vellumIndex serves a dictionary loaded whole, with a pool of
// single-threaded readers: Reader.Get reuses its decoder state, unlike
// FST.Get, while the pool keeps concurrent lookups independent.
type vellumIndex struct {
	fst  *vellum.FST
	pool *sync.Pool
}

func newVellumIndex(fst *vellum.FST) *vellumIndex {
	return &vellumIndex{fst: fst, pool: &sync.Pool{New: func() any {
		reader, _ := fst.Reader()
		return reader
	}}}
}

func (v *vellumIndex) Get(term []byte) (postingOffset uint64, exists bool, err error) {
	reader, ok := v.pool.Get().(*vellum.Reader)
	if !ok || reader == nil {
		return 0, false, fmt.Errorf("term dictionary reader unavailable")
	}
	defer v.pool.Put(reader)
	defer func() {
		if recovered := recover(); recovered != nil {
			postingOffset, exists = 0, false
			err = fmt.Errorf("term dictionary lookup panicked: %v", recovered)
		}
	}()
	return reader.Get(term)
}

func (v *vellumIndex) Len() int { return v.fst.Len() }

func (v *vellumIndex) Iterator(start, end []byte) (termIterator, error) {
	iterator, iteratorErr := v.fst.Iterator(start, end)
	if iteratorErr != nil {
		return nil, iteratorErr
	}
	return iterator, nil
}

func (v *vellumIndex) Search(automaton vellum.Automaton, start, end []byte) (termIterator, error) {
	iterator, iteratorErr := v.fst.Search(automaton, start, end)
	if iteratorErr != nil {
		return nil, iteratorErr
	}
	return iterator, nil
}
