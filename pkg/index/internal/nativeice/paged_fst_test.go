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
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/blevesearch/vellum"
)

// prefixAutomaton accepts keys that start with prefix.
type prefixAutomaton struct{ prefix []byte }

func (a prefixAutomaton) Start() int { return 0 }
func (a prefixAutomaton) IsMatch(state int) bool {
	return state >= len(a.prefix)
}
func (a prefixAutomaton) CanMatch(state int) bool        { return state >= 0 }
func (a prefixAutomaton) WillAlwaysMatch(state int) bool { return state >= len(a.prefix) }
func (a prefixAutomaton) Accept(state int, input byte) int {
	switch {
	case state < 0:
		return -1
	case state >= len(a.prefix):
		return state
	case a.prefix[state] == input:
		return state + 1
	default:
		return -1
	}
}

func buildFSTFixture(t *testing.T, keys [][]byte) ([]byte, map[string]uint64) {
	t.Helper()
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	var buffer bytes.Buffer
	builder, builderErr := vellum.New(&buffer, nil)
	if builderErr != nil {
		t.Fatal(builderErr)
	}
	values := make(map[string]uint64, len(keys))
	var previous []byte
	for index, key := range keys {
		if index > 0 && bytes.Equal(key, previous) {
			continue
		}
		value := uint64(index*7919) ^ uint64(len(key))<<40
		if insertErr := builder.Insert(key, value); insertErr != nil {
			t.Fatal(insertErr)
		}
		values[string(key)] = value
		previous = key
	}
	if closeErr := builder.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	return buffer.Bytes(), values
}

// pagedFSTFixtureKeys sizes the random and series dictionaries: tens of
// pages each, far beyond the retained top region and the narrowest window,
// while keeping the differential walk fast under the race detector.
const pagedFSTFixtureKeys = 4000

// pagedFSTFixtureBounds is how many random key ranges each shape iterates.
const pagedFSTFixtureBounds = 10

// requireSameIteration walks want and got in lockstep and fails at the first
// differing term, without materializing either result.
func requireSameIteration(t *testing.T, want termIterator, wantErr error, got termIterator, gotErr error, bound [2][]byte, shape fstWindowShape) {
	t.Helper()
	wantDone, gotDone := iterationDone(t, wantErr), iterationDone(t, gotErr)
	for position := 0; ; position++ {
		if wantDone || gotDone {
			if wantDone != gotDone {
				t.Fatalf("Search(%x, %x) with window %v: paged done=%v at term %d, vellum done=%v", bound[0], bound[1], shape, gotDone, position, wantDone)
			}
			return
		}
		wantKey, wantValue := want.Current()
		gotKey, gotValue := got.Current()
		if !bytes.Equal(gotKey, wantKey) || gotValue != wantValue {
			t.Fatalf("Search(%x, %x) with window %v: term %d is %x=%d, want %x=%d", bound[0], bound[1], shape, position, gotKey, gotValue, wantKey, wantValue)
		}
		wantDone, gotDone = iterationDone(t, want.Next()), iterationDone(t, got.Next())
	}
}

func iterationDone(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		return true
	}
	t.Fatal(err)
	return true
}

// TestPagedFSTMatchesVellum differentially checks lookups and bounded,
// automaton-filtered iteration of pagedFST against vellum over dictionaries
// with every byte value, wide fan-out states (beyond 63 and up to 256
// transitions), long shared suffixes, the empty key, and a window much
// smaller than the dictionary.
func TestPagedFSTMatchesVellum(t *testing.T) {
	rng := rand.New(rand.NewSource(17)) //nolint:gosec // deterministic fixture.
	shapes := map[string][][]byte{"empty-key": {{}, {0}, {0xff}, []byte("a")}}
	var every [][]byte
	for b := 0; b < 256; b++ {
		every = append(every, []byte{byte(b)}, []byte{byte(b), byte(255 - b)}, []byte(fmt.Sprintf("x%c-suffix", byte(b))))
	}
	shapes["every-byte"] = every
	var random [][]byte
	for index := 0; index < pagedFSTFixtureKeys; index++ {
		key := make([]byte, 1+rng.Intn(40))
		for position := range key {
			key[position] = byte(rng.Intn(256))
		}
		random = append(random, key)
	}
	shapes["random"] = random
	var series [][]byte
	for index := 0; index < pagedFSTFixtureKeys; index++ {
		series = append(series, []byte(fmt.Sprintf("service_%d/instance-%06d/endpoint-%x", index%17, index, rng.Int63())))
	}
	shapes["series"] = series
	for name, keys := range shapes {
		t.Run(name, func(t *testing.T) {
			data, values := buildFSTFixture(t, keys)
			reference, loadErr := vellum.Load(data)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			const offset = 1234
			file := &byteSegmentFile{data: append(make([]byte, offset), data...)}
			paged, openErr := openPagedFST(file, "test", offset, uint64(len(data)))
			if openErr != nil {
				t.Fatal(openErr)
			}
			if paged.Len() != reference.Len() {
				t.Fatalf("Len %d, want %d", paged.Len(), reference.Len())
			}
			if wantTop := min(len(data), pagedFSTTopSize); len(paged.top) != wantTop || paged.topStart != uint64(len(data)-wantTop) {
				t.Fatalf("kept the %d bytes from %d in memory, want the last %d", len(paged.top), paged.topStart, wantTop)
			}
			probes := make([][]byte, 0, len(values)+200)
			for key := range values {
				probes = append(probes, []byte(key))
			}
			for index := 0; index < 200; index++ {
				probes = append(probes, append(append([]byte(nil), keys[rng.Intn(len(keys))]...), byte(rng.Intn(256))))
			}
			batch := newTermLookups(paged)
			for _, probe := range probes {
				value, found, getErr := paged.Get(probe)
				wantValue, wantFound, _ := reference.Get(probe)
				if getErr != nil || found != wantFound || value != wantValue {
					t.Fatalf("Get(%x) = %d/%v/%v, want %d/%v", probe, value, found, getErr, wantValue, wantFound)
				}
				if value, found, getErr = batch.get(probe); getErr != nil || found != wantFound || value != wantValue {
					t.Fatalf("batched Get(%x) = %d/%v/%v, want %d/%v", probe, value, found, getErr, wantValue, wantFound)
				}
			}
			batch.close()
			bounds := [][2][]byte{{nil, nil}}
			for index := 0; index < pagedFSTFixtureBounds; index++ {
				low, high := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
				if bytes.Compare(low, high) > 0 {
					low, high = high, low
				}
				bounds = append(bounds, [2][]byte{low, high}, [2][]byte{low, nil}, [2][]byte{nil, high})
			}
			for _, bound := range bounds {
				var automata []vellum.Automaton
				automata = append(automata, nil)
				if key := keys[rng.Intn(len(keys))]; len(key) > 0 {
					automata = append(automata, prefixAutomaton{prefix: key[:1+rng.Intn(len(key))]})
				}
				for _, automaton := range automata {
					for _, shape := range []fstWindowShape{pagedFSTIteratorWindow, iteratorWindow(50), {shift: 9, slots: 2}} {
						wantIterator, wantErr := wrapSearch(reference, automaton, bound[0], bound[1])
						gotIterator, gotErr := paged.search(automaton, bound[0], bound[1], shape)
						requireSameIteration(t, wantIterator, wantErr, gotIterator, gotErr, bound, shape)
					}
				}
			}
		})
	}
}

func wrapSearch(fst *vellum.FST, automaton vellum.Automaton, start, end []byte) (termIterator, error) {
	iterator, iteratorErr := fst.Search(automaton, start, end)
	if iteratorErr != nil {
		return nil, iteratorErr
	}
	return iterator, nil
}

// TestIteratorWindowsFitTheBudget checks the iterators of one k-way walk
// share fstWindowBudget once their default windows would exceed it.
func TestIteratorWindowsFitTheBudget(t *testing.T) {
	for _, fanIn := range []int{1, 2, 4, 5, 8, 10, 50, 64} {
		shape := iteratorWindow(fanIn)
		perIterator := shape.slots << shape.shift
		if fanIn*perIterator > max(fstWindowBudget, pagedFSTIteratorWindow.slots<<pagedFSTIteratorWindow.shift) {
			t.Errorf("fan-in %d: %d iterators of %d bytes exceed the %d-byte budget", fanIn, fanIn, perIterator, fstWindowBudget)
		}
	}
	if shape := iteratorWindow(1); shape != pagedFSTIteratorWindow {
		t.Errorf("a lone iterator gets %v, want the default %v", shape, pagedFSTIteratorWindow)
	}
}

type panickingSegmentFile struct {
	segmentFile
	after atomic.Int64
}

func (f *panickingSegmentFile) ReadAt(destination []byte, offset int64) (int, error) {
	if f.after.Add(-1) < 0 {
		panic("injected decode failure")
	}
	return f.segmentFile.ReadAt(destination, offset)
}

// TestBatchLookupsReportDamagedDictionariesAsErrors runs batched lookups
// over a dictionary whose decoding panics, and over randomly damaged ones:
// each lookup either answers or reports an error, which a caller classifies
// as corruption, and none crashes.
func TestBatchLookupsReportDamagedDictionariesAsErrors(t *testing.T) {
	rng := rand.New(rand.NewSource(29)) //nolint:gosec // deterministic fixture.
	var keys [][]byte
	for index := 0; index < 5000; index++ {
		key := make([]byte, 24)
		for position := range key {
			key[position] = byte('a' + rng.Intn(26))
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return bytes.Compare(keys[left], keys[right]) < 0 })
	data, _ := buildFSTFixture(t, keys)
	file := &panickingSegmentFile{segmentFile: &byteSegmentFile{data: data}}
	file.after.Store(1 << 30)
	paged, openErr := openPagedFST(file, "test", 0, uint64(len(data)))
	if openErr != nil {
		t.Fatal(openErr)
	}
	file.after.Store(0)
	lookups := newTermLookups(paged)
	_, _, getErr := lookups.get(keys[0])
	lookups.close()
	if len(data) <= pagedFSTTopSize || getErr == nil || !errors.Is(lookupError("test", getErr), ErrCorrupt) {
		t.Fatalf("a lookup whose decoding panicked (dictionary %d bytes) = %v, want an error classified as corruption", len(data), getErr)
	}
	for mutation := 0; mutation < 300; mutation++ {
		damaged := append([]byte(nil), data...)
		for flips := 0; flips < 1+rng.Intn(8); flips++ {
			damaged[pagedFSTHeaderSize+rng.Intn(len(damaged)-pagedFSTHeaderSize-pagedFSTFooterSize)] ^= byte(1 + rng.Intn(255))
		}
		damagedFST, damagedErr := openPagedFST(&byteSegmentFile{data: damaged}, "test", 0, uint64(len(damaged)))
		if damagedErr != nil {
			continue
		}
		lookups := newTermLookups(damagedFST)
		for probe := 0; probe < 50; probe++ {
			if _, _, err := lookups.get(keys[rng.Intn(len(keys))]); err != nil && !errors.Is(lookupError("test", err), ErrCorrupt) {
				t.Fatalf("a damaged dictionary lookup = %v, want corruption", err)
			}
		}
		lookups.close()
	}
}
