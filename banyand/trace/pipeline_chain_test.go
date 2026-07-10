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

package trace

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// fakeSampler drops every trace whose TraceID is in dropIDs. It optionally
// panics, errors, or returns a mismatched verdict to exercise fail-open.
type fakeSampler struct {
	dropIDs   map[string]struct{}
	proj      sdk.Projection
	panicNow  bool
	errNow    bool
	wrongSize bool
}

func (f *fakeSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (f *fakeSampler) Project() sdk.Projection { return f.proj }
func (f *fakeSampler) Close() error            { return nil }

func (f *fakeSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	if f.panicNow {
		panic("boom")
	}
	if f.errNow {
		return sdk.Verdict{}, fmt.Errorf("sampler error")
	}
	if f.wrongSize {
		return sdk.Verdict{Keep: []bool{true}}, nil
	}
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		_, drop := f.dropIDs[batch.Traces[i].TraceID]
		keep[i] = !drop
	}
	return sdk.Verdict{Keep: keep}, nil
}

func newTestChain(dropIDs map[string]struct{}) *mergeChain {
	return newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{dropIDs: dropIDs}}, 0)
}

// mergeWithFilter merges single-trace parts (one trace id per part, fast raw
// path) through mergeBlocks with the supplied filter and returns the persisted
// block trace ids in order plus the dropped set.
func mergeWithFilter(t *testing.T, parts []*traces, filter *mergeFilter) ([]string, map[string]struct{}) {
	t.Helper()
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()

	var pmi []*partMergeIter
	var traceSize uint64
	for i, tr := range parts {
		mp := generateMemPart()
		mp.mustInitFromTraces(tr)
		mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
		p := mustOpenFilePart(uint64(i), tmpPath, fileSystem)
		iter := generatePartMergeIter()
		iter.mustInitFromPart(p)
		pmi = append(pmi, iter)
		traceSize += p.partMetadata.TotalCount
		releaseMemPart(mp)
	}

	br := generateBlockReader()
	br.init(pmi)
	bw := generateBlockWriter()
	dstPath := partPath(tmpPath, 9999)
	bw.mustInitForFilePart(fileSystem, dstPath, false, int(traceSize))

	closeCh := make(chan struct{})
	defer close(closeCh)

	pm, tf, tagTypes, dropped, err := mergeBlocks(closeCh, bw, br, nil, filter)
	require.NoError(t, err)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for _, iter := range pmi {
		releasePartMergeIter(iter)
	}

	pm.mustWriteMetadata(fileSystem, dstPath)
	tf.mustWriteTraceIDFilter(fileSystem, dstPath)
	tagTypes.mustWriteTagType(fileSystem, dstPath)
	fileSystem.SyncPath(dstPath)

	mergedPart := mustOpenFilePart(9999, tmpPath, fileSystem)
	mergedIter := generatePartMergeIter()
	mergedIter.mustInitFromPart(mergedPart)
	reader := generateBlockReader()
	reader.init([]*partMergeIter{mergedIter})
	var got []string
	for reader.nextBlockMetadata() {
		got = append(got, reader.block.bm.traceID)
	}
	require.NoError(t, reader.error())
	releaseBlockReader(reader)
	releasePartMergeIter(mergedIter)
	return got, dropped
}

func singleTraceParts(ids []string) []*traces {
	parts := make([]*traces, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, &traces{
			traceIDs:   []string{id},
			timestamps: []int64{1},
			tags: [][]*tagValue{
				{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes("v")}},
			},
			spans:   [][]byte{[]byte("span-" + id)},
			spanIDs: []string{"sp-" + id},
		})
	}
	return parts
}

func TestMergeFilter_DropMatureTrace(t *testing.T) {
	filter := &mergeFilter{
		chain:   newTestChain(map[string]struct{}{"traceB": {}}),
		timeout: time.Second,
	}
	got, dropped := mergeWithFilter(t, singleTraceParts([]string{"traceA", "traceB", "traceC"}), filter)
	require.Equal(t, []string{"traceA", "traceC"}, got, "traceB must be dropped, order preserved")
	require.Contains(t, dropped, "traceB")
	require.Len(t, dropped, 1)
}

func TestMergeFilter_HotMergeGate(t *testing.T) {
	now := time.Now().UnixNano()
	grace := int64(time.Minute)
	makePW := func(maxTS int64) *partWrapper {
		return &partWrapper{p: &part{partMetadata: partMetadata{MaxTimestamp: maxTS}}}
	}
	// All parts cold: maxTS well below now-grace.
	cold := []*partWrapper{
		makePW(now - 2*int64(time.Minute)),
		makePW(now - 3*int64(time.Minute)),
	}
	require.False(t, isMergeHot(cold, grace, now), "all-cold parts must not be hot")

	// One part within grace window makes the whole merge hot.
	mixed := []*partWrapper{
		makePW(now - 2*int64(time.Minute)),
		makePW(now - 30*int64(time.Second)),
	}
	require.True(t, isMergeHot(mixed, grace, now), "one warm part makes the merge hot")

	// All parts within grace.
	hot := []*partWrapper{
		makePW(now),
		makePW(now - 10*int64(time.Second)),
	}
	require.True(t, isMergeHot(hot, grace, now), "all-hot parts is hot")

	// Empty slice is not hot.
	require.False(t, isMergeHot(nil, grace, now), "empty part list is not hot")
}

func TestMergeFilter_NilFilterIdenticalToLegacy(t *testing.T) {
	parts := singleTraceParts([]string{"traceA", "traceB", "traceC"})
	got, dropped := mergeWithFilter(t, parts, nil)
	require.Equal(t, []string{"traceA", "traceB", "traceC"}, got)
	require.Nil(t, dropped)
}

func TestMergeFilter_FailOpenOnPanic(t *testing.T) {
	chain := newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{panicNow: true}}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second}
	got, dropped := mergeWithFilter(t, singleTraceParts([]string{"traceA", "traceB"}), filter)
	require.Equal(t, []string{"traceA", "traceB"}, got, "panicking link is bypassed ⇒ retain all")
	require.Empty(t, dropped)
}

// TestMergeChain_Timeout_FailsOpen builds its batch via sdktest (rather than a
// hand-built literal &sdk.TraceBatch{...}) as proof-of-use of the offline dev
// toolkit's fixture builder from inside the engine's own test suite.
func TestMergeChain_Timeout_FailsOpen(t *testing.T) {
	chain := newMergeChain("g", "s", []sdk.Sampler{&sleepSampler{d: 200 * time.Millisecond}}, 0)
	traceX, buildErr := sdktest.NewTrace("x").Build()
	require.NoError(t, buildErr)
	traceY, buildErr := sdktest.NewTrace("y").Build()
	require.NoError(t, buildErr)
	batch := sdktest.Batch(traceX, traceY)
	verdict, err := chain.Execute(batch, 10*time.Millisecond)
	require.Error(t, err)
	require.Equal(t, "timeout", err.Error())
	require.Equal(t, []bool{true, true}, verdict.Keep)
}

func TestMergeChain_CircuitBreakerOpens(t *testing.T) {
	chain := newMergeChain("g", "s", []sdk.Sampler{&sleepSampler{d: 200 * time.Millisecond}}, 2)
	traceX, buildErr := sdktest.NewTrace("x").Build()
	require.NoError(t, buildErr)
	batch := sdktest.Batch(traceX)
	_, err1 := chain.Execute(batch, 10*time.Millisecond)
	require.Equal(t, "timeout", err1.Error())
	_, err2 := chain.Execute(batch, 10*time.Millisecond)
	require.Equal(t, "circuit_open", err2.Error())
	// Once open, no goroutine is spawned and it returns retain-all with no error.
	verdict, err3 := chain.Execute(batch, 10*time.Millisecond)
	require.NoError(t, err3)
	require.Equal(t, []bool{true}, verdict.Keep)
}

func TestMergeChain_ProjectionUnion(t *testing.T) {
	s1 := &fakeSampler{proj: sdk.Projection{Tags: []string{"a", "b"}, SpanIDs: true}}
	s2 := &fakeSampler{proj: sdk.Projection{Tags: []string{"b", "c"}, Spans: true}}
	chain := newMergeChain("g", "s", []sdk.Sampler{s1, s2}, 0)
	require.ElementsMatch(t, []string{"a", "b", "c"}, chain.projection.Tags)
	require.True(t, chain.projection.SpanIDs)
	require.True(t, chain.projection.Spans)
}

type sleepSampler struct {
	d time.Duration
}

func (s *sleepSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (s *sleepSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *sleepSampler) Close() error            { return nil }
func (s *sleepSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	time.Sleep(s.d)
	keep := make([]bool, len(batch.Traces))
	for i := range keep {
		keep[i] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestMergeFilter_CoupledSidxPrune(t *testing.T) {
	// Verify the keepFn constructed from dropped set (as merger.go does) correctly
	// filters sidx-encoded data: retained traces pass, dropped trace is filtered,
	// corrupt data is fail-open.
	filter := &mergeFilter{
		chain:   newTestChain(map[string]struct{}{"traceB": {}}),
		timeout: time.Second,
	}
	_, dropped := mergeWithFilter(t, singleTraceParts([]string{"traceA", "traceB", "traceC"}), filter)
	require.Contains(t, dropped, "traceB")

	var keepFn func([]byte) bool
	if len(dropped) > 0 {
		keepFn = func(data []byte) bool {
			id, decErr := decodeTraceID(data)
			if decErr != nil {
				return true
			}
			_, isDropped := dropped[id]
			return !isDropped
		}
	}
	require.NotNil(t, keepFn)

	encodeID := func(id string) []byte { return append([]byte{byte(idFormatV1)}, []byte(id)...) }

	require.True(t, keepFn(encodeID("traceA")), "retained trace must pass keep")
	require.False(t, keepFn(encodeID("traceB")), "dropped trace must be filtered")
	require.True(t, keepFn(encodeID("traceC")), "retained trace must pass keep")
	require.True(t, keepFn([]byte("corrupt")), "corrupt data must be fail-open (keep=true)")
}

func TestMergeFilter_IdempotentReMerge(t *testing.T) {
	// Re-merge an already-filtered part (traceB absent): the same filter must drop nothing new.
	filter := &mergeFilter{
		chain:   newTestChain(map[string]struct{}{"traceB": {}}),
		timeout: time.Second,
	}
	got, dropped := mergeWithFilter(t, singleTraceParts([]string{"traceA", "traceC"}), filter)
	require.Equal(t, []string{"traceA", "traceC"}, got, "survivors unchanged on re-merge")
	require.Empty(t, dropped, "re-merging already-filtered part drops nothing new")
}
