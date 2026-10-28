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
	"testing"
)

// refreshFooterCRC rewrites the footer CRC-32 of data, when its footer length fits, so that a
// mutation of the footer reaches the decoder instead of stopping at the checksum.
func refreshFooterCRC(data []byte) {
	if len(data) < trailerSize {
		return
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	if footerLen > len(data)-trailerSize {
		return
	}
	end := len(data) - trailerSize
	binary.LittleEndian.PutUint32(data[end:], crc32.ChecksumIEEE(data[end-footerLen:end]))
}

func FuzzOpen(f *testing.F) {
	good, _ := writeContainer(f,
		testMember{"0000000000000001/metadata.json", []byte(`{"totalCount":3}`)},
		testMember{"0000000000000001/primary.bin", []byte("primary")},
		testMember{"sidx/latency/0000000000000001/manifest.json", []byte("{}")},
		testMember{"sidx/latency/0000000000000002/data.bin", []byte("orphan")},
		testMember{"idx/000000000001.snp", nil},
		testMember{"idx/000000000000.seg", []byte("seg")},
		testMember{"18e29603bfcd2125.snp", []byte(`["0000000000000001"]`)},
	)
	for _, refresh := range []bool{false, true} {
		f.Add(good, refresh)
		f.Add(good[:len(good)/2], refresh)
		f.Add(rawContainer([]byte("abc"), rawFooter(rawEntry{path: "a", size: 3, crc: crcOf([]byte("abc"))}), "BNCB"), refresh)
		f.Add(rawContainer(nil, rawFooter(rawEntry{path: "a"}, rawEntry{path: "a/b"}), magic), refresh)
		f.Add([]byte("BNCA"), refresh)
		f.Add(rawContainer([]byte("abc"), rawFooter(
			rawEntry{path: "a", size: 1, fileOffset: 4, fileSize: 5, crc: crcOf([]byte("a"))},
			rawEntry{path: "b", size: 2, fileSize: 9, crc: crcOf([]byte("bc"))},
		), magic), refresh)
	}
	f.Fuzz(func(t *testing.T, data []byte, refresh bool) {
		if refresh {
			refreshFooterCRC(data)
		}
		r := &countingReaderAt{r: bytes.NewReader(data)}
		c, err := Open(r, int64(len(data)))
		if err != nil {
			if len(r.calls) > 2 {
				t.Fatalf("a failed Open made %d reads", len(r.calls))
			}
			if corrupt, version := errors.Is(err, ErrCorrupt), errors.Is(err, ErrUnsupportedVersion); corrupt == version {
				t.Fatalf("error is neither or both kinds: %v", err)
			}
			return
		}
		if len(r.calls) != 2 {
			t.Fatalf("Open made %d reads, want 2", len(r.calls))
		}
		entries := c.Entries()
		checkFuzzEntries(t, c, entries, int64(len(data)))
		checkFuzzSet(t, c, entries)
		s, verr := c.Verify(context.Background())
		if verr != nil {
			if !errors.Is(verr, ErrCorrupt) {
				t.Fatalf("Verify: %v", verr)
			}
			return
		}
		// A verified container has exactly one encoding: rewriting its members reproduces it.
		var buf bytes.Buffer
		w := NewWriter(&buf)
		for _, e := range entries {
			if err = w.addPiece(context.Background(), e.Path, e.FileOffset, e.Size, e.FileSize, bytes.NewReader(data[e.Offset:e.Offset+e.Size])); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}
		written, err := w.Close()
		if err != nil {
			t.Fatalf("rewrite: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), data) || written != s {
			t.Fatalf("rewritten container differs")
		}
	})
}

// checkFuzzEntries checks what Open accepted: entries that cover the file back to back, valid
// unique paths, and pieces only where a volume may hold them.
func checkFuzzEntries(t *testing.T, c *Container, entries []Entry, size int64) {
	if len(entries) == 0 || len(entries) > maxEntries {
		t.Fatalf("Open accepted %d entries", len(entries))
	}
	var sum int64
	paths := newPathSet()
	for i, e := range entries {
		if e.Offset != sum {
			t.Fatalf("entry %q at %d, want %d", e.Path, e.Offset, sum)
		}
		if e.Size > e.FileSize || e.FileOffset > e.FileSize-e.Size || (e.FileOffset > 0 && i > 0) || (!e.ends() && i < len(entries)-1) {
			t.Fatalf("Open accepted the misplaced piece %+v at %d", e, i)
		}
		if err := paths.add(e.Path); err != nil {
			t.Fatalf("Open accepted %v", err)
		}
		sum += e.Size
	}
	if sum+int64(len(c.tail)) != size {
		t.Fatalf("entries do not cover the file")
	}
}

// checkFuzzSet checks that a volume alone is a set exactly when it holds whole files only, and
// that Shard groups every file of such a set.
func checkFuzzSet(t *testing.T, c *Container, entries []Entry) {
	set, err := NewVolumeSet([]*Container{c})
	whole := entries[0].FileOffset == 0 && entries[len(entries)-1].ends()
	if (err == nil) != whole {
		t.Fatalf("NewVolumeSet of a volume holding whole files %v: %v", whole, err)
	}
	if err != nil {
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("NewVolumeSet: %v", err)
		}
		return
	}
	// Shard depends on the paths only, so it runs on every set.
	layout := set.Shard()
	grouped := len(layout.Snapshots) + len(layout.ElementIndex) + len(layout.Others) + len(layout.Orphans)
	for _, p := range layout.Parts {
		if len(p.Subs) == 0 || p.Subs[0].PartType != "" {
			t.Fatalf("part %016x does not start with its core part", p.PartID)
		}
		for _, s := range p.Subs {
			grouped += 1 + len(s.Files)
		}
	}
	if grouped != len(entries) {
		t.Fatalf("Shard grouped %d of %d entries", grouped, len(entries))
	}
}
