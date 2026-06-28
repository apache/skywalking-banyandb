// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Test_mustSeqReadSpansFrom_zeroSizeBlock reproduces the trace-merger panic
// "offset N must be equal to bytesRead M". A block whose spans metadata is
// {offset:0, size:0} (an empty-spans block written after other blocks) is read
// through the sequential merge path. The reader has already advanced past byte
// zero, so the unconditional alignment assertion fires even though the block
// has no span bytes to read. mustReadRaw already guards this with
// `bm.spans.size > 0`; mustSeqReadSpansFrom must do the same.
func Test_mustSeqReadSpansFrom_zeroSizeBlock(t *testing.T) {
	req := require.New(t)
	tmpPath, defFn := test.Space(req)
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	spansPath := filepath.Join(tmpPath, spansFilename)

	// Write a non-empty spans file so the sequential reader can advance past
	// offset zero, mimicking a real part where earlier blocks carry span bytes.
	payload := make([]byte, 887)
	for i := range payload {
		payload[i] = byte(i)
	}
	fs.MustFlush(fileSystem, payload, spansPath, 0o644)

	reader := mustOpenReader(spansPath, fileSystem)
	defer fs.MustClose(reader)

	sr := &seqReader{}
	sr.init(reader)
	defer sr.reset()

	// Advance the reader past offset zero, as a prior block read would.
	scratch := make([]byte, len(payload))
	sr.mustReadFull(scratch)
	require.EqualValues(t, len(payload), sr.bytesRead)

	// A zero-size spans block (empty spans, non-first position): offset stays 0
	// from reset because mustWriteSpansTo early-returns for empty spans.
	sm := &dataBlock{offset: 0, size: 0}
	decoder := &encoding.BytesBlockDecoder{}

	// Before the fix this panics with "offset 0 must be equal to bytesRead 887".
	require.NotPanics(t, func() {
		spans, spanIDs := mustSeqReadSpansFrom(decoder, nil, nil, sm, 0, sr)
		require.Empty(t, spans)
		require.Empty(t, spanIDs)
	}, "reading a zero-size spans block must not panic on the alignment assertion")
}

// Test_mergeBlocks_sameTraceID_twoGenerations exercises the fast/slow path
// mixing in mergeBlocks across two merge generations with the real fast path
// enabled (forceSlowMerge=false). A traceID present in two source parts forces
// the slow merge path (block accumulation) interleaved with fast-path raw
// copies of unique traceIDs. The merged output is then merged again, and every
// block must read back with correct span data. This guards the br.peek()
// deep-copy fix (use-after-pool-release of the spans *dataBlock) and overall
// offset bookkeeping.
//
// Coverage note: this exercises the same-traceID-within-a-granule case. The
// peekBlockMetadata boundary branch (same traceID straddling a 128KB primary
// granule edge) cannot be reproduced at unit scale — it needs one traceID to
// own >2600 blocks (>5GB of spans) — so that path is exercised by the long
// soak (scripts/soak-vectorized.sh SOAK_ENGINE=trace), where it originally
// surfaced. Test_mustSeqReadSpansFrom_zeroSizeBlock above is the deterministic
// guard for the resulting panic.
func Test_mergeBlocks_sameTraceID_twoGenerations(t *testing.T) {
	req := require.New(t)
	tmpPath, defFn := test.Space(req)
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	prevForceSlow := forceSlowMerge
	forceSlowMerge = false
	defer func() { forceSlowMerge = prevForceSlow }()

	buildTrace := func(tid, spanID string, ts int64, val string) (string, string, int64, []*tagValue, []byte) {
		tv := &tagValue{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte(val)}
		return tid, spanID, ts, []*tagValue{tv}, []byte("span-" + spanID)
	}

	appendTrace := func(ts *traces, tid, spanID string, t int64, val string) {
		atid, asid, at, atags, aspan := buildTrace(tid, spanID, t, val)
		ts.traceIDs = append(ts.traceIDs, atid)
		ts.spanIDs = append(ts.spanIDs, asid)
		ts.timestamps = append(ts.timestamps, at)
		ts.tags = append(ts.tags, atags)
		ts.spans = append(ts.spans, aspan)
	}

	// Part 0: traceA (unique, fast path), traceM (shared, slow path).
	p0 := &traces{}
	appendTrace(p0, "traceA", "a1", 1, "va1")
	appendTrace(p0, "traceM", "m1", 2, "vm1")

	// Part 1: traceM (shared, slow path), traceZ (unique, fast path).
	p1 := &traces{}
	appendTrace(p1, "traceM", "m2", 3, "vm2")
	appendTrace(p1, "traceZ", "z1", 4, "vz1")

	gen1 := mergeTracesParts(t, fileSystem, tmpPath, 100, []*traces{p0, p1})

	// Second generation: merge the gen-1 output with another fresh part that
	// also shares traceM, re-driving the slow path on a previously merged part.
	p2 := &traces{}
	appendTrace(p2, "traceM", "m3", 5, "vm3")
	appendTrace(p2, "traceQ", "q1", 6, "vq1")
	mp2 := generateMemPart()
	mp2.mustInitFromTraces(p2)
	mp2.mustFlush(fileSystem, partPath(tmpPath, 150))
	releaseMemPart(mp2)

	gen2Parts := []*part{
		gen1,
		mustOpenFilePart(150, tmpPath, fileSystem),
	}
	merged2 := mergePartsDirect(t, fileSystem, tmpPath, 200, gen2Parts)

	// Verify every block in the twice-merged part reads back cleanly.
	expectedCounts := map[string]int{
		"traceA": 1,
		"traceM": 3,
		"traceQ": 1,
		"traceZ": 1,
	}
	got := readBackBlocks(t, merged2)
	for tid, want := range expectedCounts {
		require.Equal(t, want, got[tid], "trace %s span count after two merges", tid)
	}
}

// mergeTracesParts flushes the given traces into separate file parts, merges
// them with mergeBlocks, and returns the opened merged part.
func mergeTracesParts(t *testing.T, fileSystem fs.FileSystem, tmpPath string, dstID uint64, parts []*traces) *part {
	t.Helper()
	src := make([]*part, 0, len(parts))
	for i, ts := range parts {
		mp := generateMemPart()
		mp.mustInitFromTraces(ts)
		mp.mustFlush(fileSystem, partPath(tmpPath, dstID+uint64(i)+1))
		releaseMemPart(mp)
		src = append(src, mustOpenFilePart(dstID+uint64(i)+1, tmpPath, fileSystem))
	}
	return mergePartsDirect(t, fileSystem, tmpPath, dstID, src)
}

// mergePartsDirect runs mergeBlocks over the given source parts and returns the
// opened merged part.
func mergePartsDirect(t *testing.T, fileSystem fs.FileSystem, tmpPath string, dstID uint64, src []*part) *part {
	t.Helper()
	pii := make([]*partMergeIter, 0, len(src))
	var traceSize uint64
	for _, p := range src {
		iter := generatePartMergeIter()
		iter.mustInitFromPart(p)
		pii = append(pii, iter)
		traceSize += p.partMetadata.TotalCount
	}

	br := generateBlockReader()
	br.init(pii)
	bw := generateBlockWriter()
	dstPath := partPath(tmpPath, dstID)
	bw.mustInitForFilePart(fileSystem, dstPath, false, int(traceSize))

	closeCh := make(chan struct{})
	defer close(closeCh)

	pm, tf, tt, err := mergeBlocks(closeCh, bw, br, nil)
	require.NoError(t, err)
	require.NotNil(t, pm)

	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for _, iter := range pii {
		releasePartMergeIter(iter)
	}

	pm.mustWriteMetadata(fileSystem, dstPath)
	tf.mustWriteTraceIDFilter(fileSystem, dstPath)
	tt.mustWriteTagType(fileSystem, dstPath)
	fileSystem.SyncPath(dstPath)
	return mustOpenFilePart(dstID, tmpPath, fileSystem)
}

// readBackBlocks loads every block of the part and returns span counts per
// traceID, asserting span data is decodable.
func readBackBlocks(t *testing.T, p *part) map[string]int {
	t.Helper()
	pmi := generatePartMergeIter()
	pmi.mustInitFromPart(p)
	defer releasePartMergeIter(pmi)

	reader := generateBlockReader()
	reader.init([]*partMergeIter{pmi})
	defer releaseBlockReader(reader)

	decoder := &encoding.BytesBlockDecoder{}
	counts := make(map[string]int)
	for reader.nextBlockMetadata() {
		bm := reader.block.bm
		reader.loadBlockData(decoder)
		require.Equal(t, int(bm.count), len(reader.block.spans),
			"trace %s: span count mismatch between metadata and data", bm.traceID)
		counts[bm.traceID] += int(bm.count)
	}
	require.NoError(t, reader.error())
	return counts
}
