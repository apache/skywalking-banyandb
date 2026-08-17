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
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var stagedByteArenaTestPoolID atomic.Uint64

// fakeSampler drops every trace whose TraceID is in dropIDs. It optionally
// panics, errors, or returns a mismatched verdict to exercise fail-open.
type fakeSampler struct {
	dropIDs   map[string]struct{}
	proj      sdk.Projection
	panicNow  bool
	errNow    bool
	wrongSize bool
}

type wholeTraceErrorSampler struct {
	calls      atomic.Int64
	traceCount atomic.Int64
	rowCount   atomic.Int64
}

type durationEnvelopeSampler struct {
	threshold int64
	calls     atomic.Int64
	minTS     atomic.Int64
	maxTS     atomic.Int64
}

type blockingProjectionSampler struct {
	entered  chan struct{}
	release  chan struct{}
	observed chan string
}

type gatedSampler struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (s *blockingProjectionSampler) Kind() sdk.Kind { return sdk.KindSampler }

func (s *blockingProjectionSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: []string{"status"}, Spans: true}
}

func (s *blockingProjectionSampler) Close() error { return nil }

func (s *blockingProjectionSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	close(s.entered)
	<-s.release
	s.observed <- string(batch.Traces[0].Tags[0].Values[0]) + "/" + string(batch.Traces[0].Spans[0])
	return sdk.Verdict{Keep: []bool{true}}, nil
}

func (s *gatedSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (s *gatedSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *gatedSampler) Close() error            { return nil }

func (s *gatedSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls.Add(1)
	s.entered <- struct{}{}
	<-s.release
	return retainAllVerdict(len(batch.Traces), nil), nil
}

func (s *durationEnvelopeSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (s *durationEnvelopeSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *durationEnvelopeSampler) Close() error            { return nil }

func (s *durationEnvelopeSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls.Add(1)
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		traceBlock := &batch.Traces[traceIdx]
		s.minTS.Store(traceBlock.MinTS)
		s.maxTS.Store(traceBlock.MaxTS)
		keep[traceIdx] = traceBlock.MaxTS-traceBlock.MinTS >= s.threshold
	}
	return sdk.Verdict{Keep: keep}, nil
}

func (s *wholeTraceErrorSampler) Kind() sdk.Kind { return sdk.KindSampler }

func (s *wholeTraceErrorSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: []string{"status"}}
}

func (s *wholeTraceErrorSampler) Close() error { return nil }

func (s *wholeTraceErrorSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls.Add(1)
	s.traceCount.Add(int64(len(batch.Traces)))
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		traceBlock := &batch.Traces[traceIdx]
		s.rowCount.Add(int64(traceBlock.Len()))
		statusColumn := traceBlock.Tag("status")
		if statusColumn == nil {
			continue
		}
		for rowIdx := 0; rowIdx < traceBlock.Len(); rowIdx++ {
			statusValue, statusErr := statusColumn.At(rowIdx)
			if statusErr == nil && !statusValue.IsNull() && statusValue.Str() == "error" {
				keep[traceIdx] = true
				break
			}
		}
	}
	return sdk.Verdict{Keep: keep}, nil
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
	if filter != nil && filter.chain != nil {
		defer filter.chain.close()
	}
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
	var droppedSet map[string]struct{}
	if dropped != nil {
		droppedSet = make(map[string]struct{}, dropped.len())
		for _, traceID := range dropped.ids {
			droppedSet[traceID] = struct{}{}
		}
		releaseDroppedTraceIDs(dropped)
	}
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
	return got, droppedSet
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

func splitTraceParts(tailStatus string) []*traces {
	const traceID = "trace-large"
	largeSpan := make([]byte, maxUncompressedSpanSize)
	return []*traces{{
		traceIDs:   []string{traceID, traceID},
		timestamps: []int64{int64(100 * time.Millisecond), int64(10 * time.Second)},
		tags: [][]*tagValue{
			{{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes("success")}},
			{{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes(tailStatus)}},
		},
		spans:   [][]byte{largeSpan, []byte("tail-span")},
		spanIDs: []string{"head-span", "tail-span"},
	}}
}

func multiBlockOversizedTraceParts() []*traces {
	parts := splitTraceParts("success")
	parts[0].spans[1] = make([]byte, maxUncompressedSpanSize)
	return parts
}

func appendTrace(parts []*traces, traceID, status string) []*traces {
	parts[0].traceIDs = append(parts[0].traceIDs, traceID)
	parts[0].timestamps = append(parts[0].timestamps, int64(20*time.Second))
	parts[0].tags = append(parts[0].tags, []*tagValue{{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes(status)}})
	parts[0].spans = append(parts[0].spans, []byte("span-"+traceID))
	parts[0].spanIDs = append(parts[0].spanIDs, "span-"+traceID)
	return parts
}

func projectedSingleTraceParts(statuses map[string]string) []*traces {
	parts := make([]*traces, 0, len(statuses))
	traceIDs := make([]string, 0, len(statuses))
	for traceID := range statuses {
		traceIDs = append(traceIDs, traceID)
	}
	sort.Strings(traceIDs)
	for _, traceID := range traceIDs {
		parts = append(parts, &traces{
			traceIDs:   []string{traceID},
			timestamps: []int64{1},
			tags: [][]*tagValue{{
				{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes(statuses[traceID])},
				{tag: "unrequested", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes("ignored")},
			}},
			spans:   [][]byte{[]byte("span-" + traceID)},
			spanIDs: []string{"span-" + traceID},
		})
	}
	return parts
}

func TestMergeFilter_ProjectsUniqueRawTraceWithoutSlowMerge(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	filter := &mergeFilter{
		chain:   newMergeChain("g", "s", []sdk.Sampler{sampler}, 0),
		timeout: time.Second,
	}

	got, dropped := mergeWithFilter(t, projectedSingleTraceParts(map[string]string{
		"trace-error": "error",
		"trace-ok":    "success",
	}), filter)

	require.Equal(t, []string{"trace-error"}, got)
	require.Equal(t, map[string]struct{}{"trace-ok": {}}, dropped)
	require.Equal(t, int64(1), sampler.calls.Load())
	require.Equal(t, int64(2), sampler.traceCount.Load())
	require.Equal(t, int64(2), sampler.rowCount.Load())
}

func TestMergeFilter_AssemblesEveryPhysicalBlockBeforeDecide(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{
		chain:              chain,
		timeout:            time.Second,
		decisionBatchLimit: 1,
		forceSlow:          true,
	}

	parts := appendTrace(splitTraceParts("error"), "trace-next", "success")
	got, dropped := mergeWithFilter(t, parts, filter)

	require.Equal(t, []string{"trace-large", "trace-large"}, got,
		"error evidence in the second physical block must retain every block")
	require.NotContains(t, dropped, "trace-large")
	require.Contains(t, dropped, "trace-next")
	require.Equal(t, int64(2), sampler.calls.Load(), "the tiny stage budget may flush only at complete trace boundaries")
	require.Equal(t, int64(2), sampler.traceCount.Load(), "each trace ID must appear exactly once across Decide batches")
	require.Equal(t, int64(3), sampler.rowCount.Load(), "the logical traces must contain every physical row")
}

func TestMergeFilter_AggregatesSplitTraceTimestampEnvelope(t *testing.T) {
	sampler := &durationEnvelopeSampler{threshold: int64(5 * time.Second)}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second}

	got, dropped := mergeWithFilter(t, splitTraceParts("success"), filter)

	require.Equal(t, []string{"trace-large", "trace-large"}, got)
	require.Empty(t, dropped)
	require.Equal(t, int64(1), sampler.calls.Load())
	require.Equal(t, int64(100*time.Millisecond), sampler.minTS.Load())
	require.Equal(t, int64(10*time.Second), sampler.maxTS.Load())
}

func TestMergeFilter_DropsEveryPhysicalBlockOfLogicalTrace(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{chain: chain, timeout: time.Second, forceSlow: true}

	got, dropped := mergeWithFilter(t, splitTraceParts("success"), filter)

	require.Empty(t, got, "one logical drop verdict must remove every physical block")
	require.Contains(t, dropped, "trace-large")
	require.Equal(t, int64(1), sampler.calls.Load())
	require.Equal(t, int64(1), sampler.traceCount.Load())
	require.Equal(t, int64(2), sampler.rowCount.Load())
}

func TestMergeFilter_OversizedTraceBypassesPartialEvaluation(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{
		chain:       chain,
		timeout:     time.Second,
		traceBudget: maxUncompressedSpanSize / 2,
		forceSlow:   true,
	}

	parts := appendTrace(splitTraceParts("success"), "trace-next", "error")
	got, dropped := mergeWithFilter(t, parts, filter)

	require.Equal(t, []string{"trace-large", "trace-large", "trace-next"}, got,
		"an oversized trace must fail open instead of being partially evaluated")
	require.Empty(t, dropped)
	require.Equal(t, int64(1), sampler.calls.Load(), "only the following bounded trace may reach Decide")
	require.Equal(t, int64(1), sampler.traceCount.Load())
	require.Equal(t, int64(1), sampler.rowCount.Load())
}

func TestTraceEvaluationStager_ExactBudgetDoesNotBypass(t *testing.T) {
	stagedTraceBlock := stagedTrace{
		isRaw:    true,
		traceID:  "trace-exact",
		rawSpans: []byte("exact-budget"),
	}
	budgetProbe := &traceEvaluationStager{filter: &mergeFilter{}}
	defer releaseTestStagerBuffers(budgetProbe)
	budgetProbe.stage(stagedTraceBlock)
	exactBudget := budgetProbe.currentTraceBytes
	stager := &traceEvaluationStager{
		filter: &mergeFilter{traceBudget: exactBudget},
	}
	defer releaseTestStagerBuffers(stager)

	stager.stage(stagedTraceBlock)

	require.Len(t, stager.staged, 1)
	require.Equal(t, exactBudget, stager.currentTraceBytes)
	require.Empty(t, stager.bypassedTraceID)
}

func TestStagedTraceBudgetIncludesStructuralOverhead(t *testing.T) {
	stagedBlock := stagedTrace{
		isRaw:          true,
		traceID:        "trace-accounted",
		rawSpans:       make([]byte, 3, 32),
		rawTags:        map[string][]byte{"service": make([]byte, 2, 16)},
		rawTagMetadata: map[string][]byte{"service": make([]byte, 1, 8)},
	}
	payloadBytes := uint64(cap(stagedBlock.rawSpans) + cap(stagedBlock.rawTags["service"]) + cap(stagedBlock.rawTagMetadata["service"]))

	require.Greater(t, stagedBlock.approxBytes(), payloadBytes,
		"the staging budget must reserve metadata, map, trace-group, and verdict structures in addition to payload capacity")
}

func TestStageRawTraceOwnsOneContiguousArena(t *testing.T) {
	sourceSpans := []byte("spans")
	sourceTag := []byte("tag-value")
	sourceMetadata := []byte("tag-metadata")
	metadata := &blockMetadata{traceID: "trace-arena", spans: &dataBlock{}}
	raw := rawBlock{
		bm: metadata, spans: sourceSpans,
		tags: map[string][]byte{"service": sourceTag}, tagMetadata: map[string][]byte{"service": sourceMetadata},
	}

	staged := stageRawTrace(&raw)
	defer releaseStagedTrace(&staged)

	require.Len(t, staged.rawArena.Buf, len(sourceSpans)+len(sourceTag)+len(sourceMetadata))
	require.Len(t, staged.rawMetadataBlocks.values, len(metadata.tags),
		"raw tag metadata descriptors must use one bounded pooled vector instead of one allocation per tag")
	clear(sourceSpans)
	clear(sourceTag)
	clear(sourceMetadata)
	require.Equal(t, []byte("spans"), staged.rawSpans)
	require.Equal(t, []byte("tag-value"), staged.rawTags["service"])
	require.Equal(t, []byte("tag-metadata"), staged.rawTagMetadata["service"])
}

func TestTraceEvaluationStagerCountsBatchStructures(t *testing.T) {
	stagedBlock := stagedTrace{isRaw: true, traceID: "trace-structures", rawSpans: []byte("span")}
	stager := &traceEvaluationStager{
		filter: &mergeFilter{chain: newTestChain(nil)},
	}
	defer releaseTestStagerBuffers(stager)

	stager.stage(stagedBlock)

	require.Greater(t, stager.stagedBytes, stagedBlock.approxBytes(),
		"the aggregate budget must include staged-slice capacity, a trace-group descriptor, and verdict state")
	require.Greater(t, stager.currentTraceBytes, stagedBlock.approxBytes(),
		"the per-trace budget must independently include the trace's staging structures")
}

func TestTraceEvaluationStagerCountsEvaluationSlotOncePerLogicalTrace(t *testing.T) {
	metadata := func(minTS, maxTS int64) *blockMetadata {
		return &blockMetadata{
			traceID: "trace-group", spans: &dataBlock{},
			timestamps: timestampsMetadata{min: minTS, max: maxTS, known: true},
		}
	}
	first := stagedTrace{isRaw: true, traceID: "trace-group", rawBM: metadata(1, 2)}
	second := stagedTrace{isRaw: true, traceID: "trace-group", rawBM: metadata(3, 4)}
	stager := &traceEvaluationStager{filter: &mergeFilter{chain: newTestChain(nil)}}
	defer releaseTestStagerBuffers(stager)

	stager.stage(first)
	firstTraceBytes := stager.currentTraceBytes
	stager.stage(second)
	secondBlockBytes := stager.currentTraceBytes - firstTraceBytes

	expectedSecondBlockBytes := second.approxBytes() + uint64(unsafe.Sizeof(stagedTrace{}))
	require.Equal(t, expectedSecondBlockBytes, secondBlockBytes,
		"trace IDs, decision masks, SDK blocks, and group descriptors are reserved once per logical trace")
}

func TestTraceEvaluationStagerReservesProjectedEvaluationCopies(t *testing.T) {
	block := &blockPointer{}
	block.bm.traceID = "trace-projected"
	block.block.spans = [][]byte{make([]byte, 3, 32)}
	block.block.spanIDs = []string{"span-id"}
	block.block.tags = []tag{{name: "status", values: [][]byte{make([]byte, 2, 16)}}}
	stagedBlock := stagedTrace{traceID: "trace-projected", slowBlock: block}
	metadataOnly := &traceEvaluationStager{filter: &mergeFilter{chain: newTestChain(nil)}}
	defer releaseTestStagerBuffers(metadataOnly)
	metadataOnly.stage(stagedBlock)
	projected := &traceEvaluationStager{filter: &mergeFilter{
		chain: newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{proj: sdk.Projection{
			Tags: []string{"status"}, SpanIDs: true, Spans: true,
		}}}, 0),
	}}
	defer releaseTestStagerBuffers(projected)

	projected.stage(stagedBlock)

	require.Greater(t, projected.currentTraceBytes, metadataOnly.currentTraceBytes,
		"the budget must reserve transient TraceBlock vectors and copied projected values")
}

func TestRawProjectionBudgetCoversSparseColumnsAndDecodedPayload(t *testing.T) {
	tagBuffer := pkgbytes.Buffer{}
	_, encodeErr := internalencoding.EncodeTagValues(&tagBuffer, [][]byte{[]byte("error")}, pbv1.ValueTypeStr)
	require.NoError(t, encodeErr)
	spanID := []byte("a-compressible-span-id-a-compressible-span-id")
	span := []byte("span-body")
	encodedSpans := encoding.EncodeBytesBlock(nil, [][]byte{spanID})
	encodedSpans = encoding.EncodeBytesBlock(encodedSpans, [][]byte{span})
	stagedBlock := stagedTrace{
		isRaw: true, traceID: "trace-projected", rawBM: &blockMetadata{
			traceID: "trace-projected", count: 1, spans: &dataBlock{},
			timestamps: timestampsMetadata{min: 1, max: 2, known: true},
			tags:       map[string]*dataBlock{"status": {}}, tagType: map[string]pbv1.ValueType{"status": pbv1.ValueTypeStr},
		},
		rawTags: map[string][]byte{"status": tagBuffer.Buf}, rawSpans: encodedSpans,
	}
	projection := sdk.Projection{Tags: []string{"missing-a", "status", "missing-b"}, SpanIDs: true, Spans: true}
	charged := stagedBlock.approxEvaluationBytes(projection)
	vectors := &stagedEvaluationVectors{}
	prepareStagedProjectionVectors(vectors, nil, []stagedTrace{stagedBlock}, []stagedTraceGroup{{
		traceID: "trace-projected", start: 0, end: 1, minTS: 1, maxTS: 2, validMetadata: true,
	}}, projection)
	assembled, valid := assembleRawTraceBlockInto(vectors, &stagedBlock, projection)
	require.True(t, valid)
	defer func() {
		for _, arena := range vectors.projectionArenas {
			releaseStagedByteArena(arena)
		}
	}()
	require.Equal(t, spanID, []byte(assembled.SpanIDs[0]))
	require.Equal(t, span, assembled.Spans[0])

	actual := uint64(cap(vectors.tagColumns))*uint64(unsafe.Sizeof(sdk.TagColumn{})) +
		uint64(cap(vectors.tagValues))*uint64(unsafe.Sizeof([]byte{})) +
		uint64(cap(vectors.spanIDs))*uint64(unsafe.Sizeof("")) +
		uint64(cap(vectors.spans))*uint64(unsafe.Sizeof([]byte{}))
	for _, arena := range vectors.projectionArenas {
		actual += uint64(len(arena.Buf))
	}
	require.GreaterOrEqual(t, charged, actual,
		"the hard staging estimate must cover sparse projection vectors and decoded span-ID, span, and tag bytes")
}

func TestMergeFilter_TraceCountBudgetBoundsDecideBatch(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	filter := &mergeFilter{
		chain:         newMergeChain("g", "s", []sdk.Sampler{sampler}, 0),
		timeout:       time.Second,
		maxTraceCount: 2,
		forceSlow:     true,
	}

	_, _ = mergeWithFilter(t, singleTraceParts([]string{"trace-a", "trace-b", "trace-c", "trace-d", "trace-e"}), filter)

	require.Equal(t, int64(3), sampler.calls.Load(), "five logical traces with a two-trace cap require three Decide batches")
	require.Equal(t, int64(5), sampler.traceCount.Load())
}

func TestTraceEvaluationStagerBuildsTraceGroupsIncrementally(t *testing.T) {
	stager := &traceEvaluationStager{filter: &mergeFilter{chain: newTestChain(nil)}}
	defer releaseTestStagerBuffers(stager)
	stager.stage(stagedTrace{
		isRaw: true, traceID: "trace-a", rawBM: &blockMetadata{
			traceID: "trace-a", spans: &dataBlock{}, timestamps: timestampsMetadata{min: 10, max: 20, known: true},
		},
	})
	stager.stage(stagedTrace{
		isRaw: true, traceID: "trace-a", rawBM: &blockMetadata{
			traceID: "trace-a", spans: &dataBlock{}, timestamps: timestampsMetadata{min: 5, max: 25, known: true},
		},
	})

	require.Len(t, stager.groups, 1)
	group := stager.groups[0]
	require.Equal(t, "trace-a", group.traceID)
	require.Equal(t, 0, group.start)
	require.Equal(t, 2, group.end)
	require.Equal(t, int64(5), group.minTS)
	require.Equal(t, int64(25), group.maxTS)
	require.Positive(t, group.accountedBytes)
	require.False(t, stager.invalidOrder)

	stager.stage(stagedTrace{
		isRaw: true, traceID: "trace-0", rawBM: &blockMetadata{
			traceID: "trace-0", spans: &dataBlock{}, timestamps: timestampsMetadata{min: 30, max: 40, known: true},
		},
	})
	require.True(t, stager.invalidOrder, "a new trace ID that sorts before the completed group must disable evaluation")
}

func TestStagingPoolsResetAllReferencesBeforeReuse(t *testing.T) {
	traceBuffer := stagedTraceBuffer{values: []stagedTrace{{
		traceID: "trace-a", rawSpans: []byte("span"), rawTags: map[string][]byte{"tag": []byte("value")},
	}}}
	groupBuffer := stagedTraceGroupBuffer{values: []stagedTraceGroup{{traceID: "trace-a", start: 1, end: 2, minTS: 3, maxTS: 4}}}
	evaluationVectors := stagedEvaluationVectors{
		traceBlocks: []sdk.TraceBlock{{TraceID: "trace-a", Spans: [][]byte{[]byte("span")}}},
		traceIDs:    []string{"trace-a"}, guardRanges: []stagedTraceRange{{start: 1, end: 2}}, decisionMask: []bool{true},
	}
	byteArena := acquireStagedByteArena(len("sensitive"))
	copy(byteArena.Buf, "sensitive")
	metadataBlocks := acquireStagedDataBlockBuffer(1)
	metadataBlocks.values[0] = dataBlock{offset: 1, size: 2}
	metadata := acquireStagedBlockMetadata()
	metadata.traceID = "trace-a"
	metadata.spans = &dataBlock{offset: 1, size: 2}
	metadata.tags = map[string]*dataBlock{"tag": {offset: 3, size: 4}}
	metadata.tagType = map[string]pbv1.ValueType{"tag": pbv1.ValueTypeStr}

	traceBuffer.reset()
	groupBuffer.reset()
	evaluationVectors.reset()
	releaseStagedByteArena(byteArena)
	releaseStagedDataBlockBuffer(metadataBlocks)
	releaseStagedBlockMetadata(metadata)

	require.Empty(t, traceBuffer.values)
	require.Empty(t, groupBuffer.values)
	require.Empty(t, evaluationVectors.traceBlocks)
	require.Empty(t, evaluationVectors.traceIDs)
	require.Empty(t, evaluationVectors.guardRanges)
	require.Empty(t, evaluationVectors.decisionMask)
	require.Empty(t, byteArena.Buf)
	require.Empty(t, metadataBlocks.values)
	require.Empty(t, metadata.traceID)
	require.Empty(t, metadata.tags)
	require.Empty(t, metadata.tagType)
}

func releaseTestStagerBuffers(stager *traceEvaluationStager) {
	clear(stager.staged)
	stager.staged = stager.staged[:0]
	clear(stager.groups)
	stager.groups = stager.groups[:0]
	stager.releaseBuffers()
}

func TestEvaluationVectorPoolDiscardsUnsafeAndOversizedBatches(t *testing.T) {
	unsafeVectors := acquireStagedEvaluationVectors(1, false)
	unsafeVectors.traceIDs = append(unsafeVectors.traceIDs, "still-in-use")
	require.False(t, releaseStagedEvaluationVectors(unsafeVectors, false))
	require.Equal(t, []string{"still-in-use"}, unsafeVectors.traceIDs,
		"a timed-out plugin may still read the vectors, so they must not be reset or pooled")

	oversizedVectors := acquireStagedEvaluationVectors(maxPooledEvaluationTraces+1, false)
	require.False(t, releaseStagedEvaluationVectors(oversizedVectors, true))
	require.Empty(t, oversizedVectors.traceIDs)

	reusableVectors := acquireStagedEvaluationVectors(1, false)
	reusableVectors.traceIDs = append(reusableVectors.traceIDs, "reset-me")
	reusableVectors.decisionMask[0] = true
	require.True(t, releaseStagedEvaluationVectors(reusableVectors, true))
	require.Empty(t, reusableVectors.traceIDs)
	require.Empty(t, reusableVectors.decisionMask)
}

func projectedStagedTrace(traceID, status, span string) ([]stagedTrace, []stagedTraceGroup) {
	block := &blockPointer{}
	block.bm.traceID = traceID
	block.bm.count = 1
	block.bm.timestamps = timestampsMetadata{min: 1, max: 2, known: true}
	block.block.minTS = 1
	block.block.maxTS = 2
	block.block.spanIDs = []string{"span-id"}
	block.block.spans = [][]byte{[]byte(span)}
	block.block.tags = []tag{{name: "status", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte(status)}}}
	return []stagedTrace{{traceID: traceID, slowBlock: block}}, []stagedTraceGroup{{
		traceID: traceID, start: 0, end: 1, minTS: 1, maxTS: 2, validMetadata: true,
	}}
}

func TestStagedEvaluationBatchPacksProjectedPayloadIntoArena(t *testing.T) {
	staged, groups := projectedStagedTrace("trace-arena", "error", "span-body")
	filter := &mergeFilter{chain: newMergeChain("g", "s", []sdk.Sampler{&fakeSampler{proj: sdk.Projection{
		Tags: []string{"status"}, SpanIDs: true, Spans: true,
	}}}, 0)}
	defer filter.chain.close()

	assembled, valid := assembleStagedEvaluationBatch(filter, staged, groups)
	require.True(t, valid)
	require.Len(t, assembled.vectors.projectionArenas, 1)
	require.Len(t, assembled.vectors.traceBlocks, 1)
	traceBlock := &assembled.vectors.traceBlocks[0]
	require.Equal(t, []byte("error"), traceBlock.Tags[0].Values[0])
	require.Equal(t, []byte("span-body"), traceBlock.Spans[0])
	require.Equal(t, "span-id", traceBlock.SpanIDs[0])

	staged[0].slowBlock.block.tags[0].values[0][0] = 'X'
	staged[0].slowBlock.block.spans[0][0] = 'X'
	staged[0].slowBlock.block.spanIDs[0] = "changed"
	require.Equal(t, []byte("error"), traceBlock.Tags[0].Values[0])
	require.Equal(t, []byte("span-body"), traceBlock.Spans[0])
	require.Equal(t, "span-id", traceBlock.SpanIDs[0])
	require.True(t, releaseStagedEvaluationVectors(assembled.vectors, true))
}

func TestTimedOutProjectionBatchNeverRecyclesArena(t *testing.T) {
	sampler := &blockingProjectionSampler{
		entered: make(chan struct{}), release: make(chan struct{}), observed: make(chan string, 1),
	}
	filter := &mergeFilter{
		chain: newMergeChain("g", "s", []sdk.Sampler{sampler}, 0), timeout: 10 * time.Millisecond,
	}
	defer filter.chain.close()
	staged, groups := projectedStagedTrace("trace-timeout", "error", "span-body")
	assembled, valid := assembleStagedEvaluationBatch(filter, staged, groups)
	require.True(t, valid)

	tracker := &dropTracker{}
	_, reusable := resolveStagedDrops(filter, staged, assembled, tracker)
	require.Nil(t, tracker.exact)
	require.False(t, reusable)
	<-sampler.entered
	require.False(t, releaseStagedEvaluationVectors(assembled.vectors, reusable))

	replacement := acquireStagedByteArena(len("error") + len("span-body"))
	for byteIdx := range replacement.Buf {
		replacement.Buf[byteIdx] = 'X'
	}
	releaseStagedByteArena(replacement)
	close(sampler.release)
	require.Equal(t, "error/span-body", <-sampler.observed)
}

func TestStagedByteArenaCacheBoundsAggregateRetention(t *testing.T) {
	poolName := fmt.Sprintf("trace-test-staged-byte-arena-%d", stagedByteArenaTestPoolID.Add(1))
	cache := newStagedByteArenaPool(poolName, 8)
	first := cache.get(6)
	second := cache.get(6)

	require.True(t, cache.put(first))
	require.False(t, cache.put(second), "the aggregate cache bound must reject individually small arenas once it is full")
	require.Equal(t, int64(6), cache.pool.RetainedSize())
	runtime.GC()

	reused := cache.get(5)
	require.Same(t, first, reused)
	require.Len(t, reused.Buf, 5)
	require.Zero(t, cache.pool.RetainedSize())
	cache.pool.Discard(reused)
	require.Zero(t, cache.pool.RefsCount())
}

func TestStagingInternalPoolsBalanceReferences(t *testing.T) {
	arena := acquireStagedByteArena(8)
	releaseStagedByteArena(arena)
	metadata := acquireStagedBlockMetadata()
	releaseStagedBlockMetadata(metadata)
	metadataBlocks := acquireStagedDataBlockBuffer(1)
	releaseStagedDataBlockBuffer(metadataBlocks)
	traceBuffer := acquireStagedTraceBuffer()
	releaseStagedTraceBuffer(traceBuffer)
	groupBuffer := acquireStagedTraceGroupBuffer()
	releaseStagedTraceGroupBuffer(groupBuffer)
	evaluation := acquireStagedEvaluationVectors(1, true)
	releaseStagedEvaluationVectors(evaluation, true)

	require.Zero(t, stagedByteArenas.pool.RefsCount())
	require.Zero(t, stagedBlockMetadataPool.RefsCount())
	require.Zero(t, stagedDataBlockPool.RefsCount())
	require.Zero(t, stagedTraceBufferPool.RefsCount())
	require.Zero(t, stagedTraceGroupPool.RefsCount())
	require.Zero(t, stagedEvaluationPool.RefsCount())
}

func TestMergeFilter_RawOversizedTracesBypassEvaluationInOrder(t *testing.T) {
	sampler := &durationEnvelopeSampler{threshold: int64(time.Hour)}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{
		chain:       chain,
		timeout:     time.Second,
		traceBudget: 1,
	}

	got, dropped := mergeWithFilter(t, singleTraceParts([]string{"trace-a", "trace-b"}), filter)

	require.Equal(t, []string{"trace-a", "trace-b"}, got)
	require.Empty(t, dropped)
	require.Zero(t, sampler.calls.Load(), "raw oversized traces must bypass Decide")
}

func TestMergeFilter_MultiBlockOversizedTraceAtEOFBypassesAsUnit(t *testing.T) {
	sampler := &wholeTraceErrorSampler{}
	chain := newMergeChain("g", "s", []sdk.Sampler{sampler}, 0)
	filter := &mergeFilter{
		chain:       chain,
		timeout:     time.Second,
		traceBudget: maxUncompressedSpanSize + maxUncompressedSpanSize/2,
		forceSlow:   true,
	}

	got, dropped := mergeWithFilter(t, multiBlockOversizedTraceParts(), filter)

	require.Equal(t, []string{"trace-large", "trace-large"}, got)
	require.Empty(t, dropped)
	require.Zero(t, sampler.calls.Load(), "the accumulated oversized trace must never partially reach Decide")
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

func TestMergeFilter_MergeMayContainMatureTrace(t *testing.T) {
	now := time.Now().UnixNano()
	grace := int64(time.Minute)
	frontier := now - grace
	makePW := func(minTS, maxTS int64) *partWrapper {
		return &partWrapper{p: &part{partMetadata: partMetadata{MinTimestamp: minTS, MaxTimestamp: maxTS}}}
	}
	// All selected parts can contain mature traces.
	cold := []*partWrapper{
		makePW(now-3*int64(time.Minute), now-2*int64(time.Minute)),
		makePW(now-4*int64(time.Minute), now-3*int64(time.Minute)),
	}
	require.True(t, mergeMayContainMatureTrace(cold, frontier), "all-cold parts can contain mature traces")

	// A hot part must not suppress a mature selected part.
	mixed := []*partWrapper{
		makePW(now-3*int64(time.Minute), now-2*int64(time.Minute)),
		makePW(now-30*int64(time.Second), now),
	}
	require.True(t, mergeMayContainMatureTrace(mixed, frontier), "one hot part must not bypass the whole merge")

	// No selected part can contain a mature trace.
	hot := []*partWrapper{
		makePW(now-10*int64(time.Second), now),
		makePW(now-30*int64(time.Second), now-20*int64(time.Second)),
	}
	require.False(t, mergeMayContainMatureTrace(hot, frontier), "all-hot parts cannot contain mature traces")

	require.False(t, mergeMayContainMatureTrace(nil, frontier), "empty part list cannot contain mature traces")
}

func TestMergeFilter_EvaluatesOnlyMatureTraceGroups(t *testing.T) {
	immatureCounter := &fakeMetricCounter{}
	sampler := &wholeTraceErrorSampler{}
	observation := &mergeEvaluationObservation{}
	filter := &mergeFilter{
		chain:       newMergeChain("g", "s", []sdk.Sampler{sampler}, 0),
		timeout:     time.Second,
		observation: observation,
		owner: &tsTable{metrics: &metrics{
			pipelineTracesDropped:   &fakeMetricCounter{},
			pipelineTracesEvaluated: &fakeMetricCounter{},
			pipelineTracesImmature:  immatureCounter,
		}},
		maturityFrontier: 10,
		filterImmature:   true,
	}
	parts := []*traces{{
		traceIDs:   []string{"trace-mature", "trace-immature"},
		timestamps: []int64{10, 11},
		tags: [][]*tagValue{
			{{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes("success")}},
			{{tag: "status", valueType: pbv1.ValueTypeStr, value: convert.StringToBytes("success")}},
		},
		spans:   [][]byte{[]byte("mature"), []byte("immature")},
		spanIDs: []string{"mature-span", "immature-span"},
	}}

	got, dropped := mergeWithFilter(t, parts, filter)

	require.Equal(t, []string{"trace-immature"}, got, "immature traces must remain unchanged")
	require.Equal(t, map[string]struct{}{"trace-mature": {}}, dropped)
	require.Equal(t, int64(1), sampler.calls.Load())
	require.Equal(t, int64(1), sampler.traceCount.Load(), "only mature traces reach Decide")
	require.Equal(t, 1, immatureCounter.callsWithLabels(), "each immature trace increments the metric")
	require.Equal(t, uint64(1), observation.immature.Load())
}

func TestMergeFilter_CompletesTraceBeforeMaturityDecision(t *testing.T) {
	immatureCounter := &fakeMetricCounter{}
	sampler := &wholeTraceErrorSampler{}
	filter := &mergeFilter{
		chain:   newMergeChain("g", "s", []sdk.Sampler{sampler}, 0),
		timeout: time.Second,
		owner: &tsTable{metrics: &metrics{
			pipelineTracesDropped:   &fakeMetricCounter{},
			pipelineTracesEvaluated: &fakeMetricCounter{},
			pipelineTracesImmature:  immatureCounter,
		}},
		maturityFrontier: int64(5 * time.Second),
		filterImmature:   true,
		forceSlow:        true,
	}
	parts := appendTrace(splitTraceParts("success"), "trace-next", "success")
	parts[0].timestamps[len(parts[0].timestamps)-1] = int64(time.Second)

	got, dropped := mergeWithFilter(t, parts, filter)

	require.Equal(t, []string{"trace-large", "trace-large"}, got,
		"a mature head block cannot make a logical trace with an immature tail eligible")
	require.Equal(t, map[string]struct{}{"trace-next": {}}, dropped)
	require.Equal(t, int64(1), sampler.calls.Load())
	require.Equal(t, int64(1), sampler.traceCount.Load(), "only the complete mature trace reaches Decide")
	require.Equal(t, 1, immatureCounter.callsWithLabels())
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
	chain.executionSlots = make(chan struct{}, 1)
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

func TestMergeChain_TimeoutDoesNotRecycleDecisionStorage(t *testing.T) {
	chain := newMergeChain("g", "s", []sdk.Sampler{&sleepSampler{d: 200 * time.Millisecond}}, 0)
	chain.executionSlots = make(chan struct{}, 1)
	traceX, buildErr := sdktest.NewTrace("x").Build()
	require.NoError(t, buildErr)
	batch := sdktest.Batch(traceX)
	decisionMask := []bool{false}

	verdict, reusable, executeErr := chain.executeObservedInto(batch, 10*time.Millisecond, nil, decisionMask)

	require.Error(t, executeErr)
	require.Equal(t, []bool{true}, verdict.Keep)
	require.False(t, reusable, "the timed-out worker may still access the caller-owned decision storage")
}

func TestMergeChain_EmptyChainSkipsExecutionLimit(t *testing.T) {
	executionSlots := make(chan struct{}, 1)
	executionSlots <- struct{}{}
	chain := newMergeChain("g", "s", []sdk.Sampler{nil}, 1)
	chain.executionSlots = executionSlots
	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "trace-a"}}}
	decisionMask := []bool{false}

	verdict, reusable, executeErr := chain.executeObservedInto(batch, time.Nanosecond, nil, decisionMask)

	require.NoError(t, executeErr)
	require.True(t, reusable)
	require.Equal(t, []bool{true}, verdict.Keep)
	require.Len(t, executionSlots, 1, "an empty chain must not consume an execution slot")
	require.Nil(t, chain.worker, "an empty chain must not create a worker")
	chain.mu.Lock()
	circuitOpen := chain.circuitOpen
	consecutiveTimeouts := chain.consecutiveTOs
	chain.mu.Unlock()
	require.False(t, circuitOpen)
	require.Zero(t, consecutiveTimeouts)
}

func TestMergeChain_FreshChainsShareStuckExecutionLimit(t *testing.T) {
	executionSlots := make(chan struct{}, 1)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-firstRelease:
		default:
			close(firstRelease)
		}
		select {
		case <-secondRelease:
		default:
			close(secondRelease)
		}
	})
	firstSampler := &gatedSampler{entered: make(chan struct{}, 1), release: firstRelease}
	firstChain := newMergeChain("g", "s", []sdk.Sampler{firstSampler}, 0)
	firstChain.executionSlots = executionSlots
	traceX, buildErr := sdktest.NewTrace("x").Build()
	require.NoError(t, buildErr)
	batch := sdktest.Batch(traceX)

	firstVerdict, firstReusable, firstErr := firstChain.executeObservedInto(batch, 20*time.Millisecond, nil, []bool{false})

	require.EqualError(t, firstErr, "timeout")
	require.Equal(t, []bool{true}, firstVerdict.Keep)
	require.False(t, firstReusable)
	require.Equal(t, int64(1), firstSampler.calls.Load())
	require.Len(t, executionSlots, 1, "a non-returning Decide call must retain its process-wide slot")

	secondSampler := &gatedSampler{entered: make(chan struct{}, 1), release: secondRelease}
	secondChain := newMergeChain("g", "s", []sdk.Sampler{secondSampler}, 0)
	secondChain.executionSlots = executionSlots
	secondVerdict, secondReusable, secondErr := secondChain.executeObservedInto(batch, 20*time.Millisecond, nil, []bool{false})

	require.EqualError(t, secondErr, "timeout")
	require.Equal(t, []bool{true}, secondVerdict.Keep)
	require.True(t, secondReusable, "a batch not handed to a worker remains reusable")
	require.Zero(t, secondSampler.calls.Load(), "a fresh chain must not create another stuck sampler call")
	close(firstRelease)
	require.Eventually(t, func() bool { return len(executionSlots) == 0 }, time.Second, time.Millisecond)
}

func TestMergeChain_CircuitBreakerOpens(t *testing.T) {
	chain := newMergeChain("g", "s", []sdk.Sampler{&sleepSampler{d: 200 * time.Millisecond}}, 2)
	chain.executionSlots = make(chan struct{}, 2)
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

type reusableVerdictSampler struct {
	keep []bool
}

func (rvs *reusableVerdictSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (rvs *reusableVerdictSampler) Project() sdk.Projection { return sdk.Projection{} }
func (rvs *reusableVerdictSampler) Close() error            { return nil }
func (rvs *reusableVerdictSampler) Decide(*sdk.TraceBatch) (sdk.Verdict, error) {
	return sdk.Verdict{Keep: rvs.keep}, nil
}

func BenchmarkMergeChainRunObserved(b *testing.B) {
	const traceCount = 512
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, traceCount)}
	chain := newMergeChain("g", "s", []sdk.Sampler{&reusableVerdictSampler{keep: make([]bool, traceCount)}}, 0)
	observation := &mergeEvaluationObservation{}
	decisionMask := make([]bool, traceCount)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		chain.runChain(batch, observation, decisionMask)
	}
}

func BenchmarkMergeChainExecuteObserved(b *testing.B) {
	const traceCount = 512
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, traceCount)}
	chain := newMergeChain("g", "s", []sdk.Sampler{&reusableVerdictSampler{keep: make([]bool, traceCount)}}, 0)
	b.Cleanup(chain.close)
	observation := &mergeEvaluationObservation{}
	decisionMask := make([]bool, traceCount)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, reusable, executeErr := chain.executeObservedInto(batch, time.Minute, observation, decisionMask)
		if executeErr != nil || !reusable {
			b.Fatalf("unexpected decision result: reusable=%t err=%v", reusable, executeErr)
		}
	}
}

func TestMergeChainObservedDecisionPathReusesExecutionStorage(t *testing.T) {
	const traceCount = 16
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, traceCount)}
	chain := newMergeChain("g", "s", []sdk.Sampler{&reusableVerdictSampler{keep: make([]bool, traceCount)}}, 0)
	t.Cleanup(chain.close)
	observation := &mergeEvaluationObservation{}
	decisionMask := make([]bool, traceCount)
	_, reusable, executeErr := chain.executeObservedInto(batch, time.Minute, observation, decisionMask)
	require.NoError(t, executeErr)
	require.True(t, reusable)

	var measuredErr error
	allocations := testing.AllocsPerRun(100, func() {
		_, reusable, measuredErr = chain.executeObservedInto(batch, time.Minute, observation, decisionMask)
		if !reusable {
			measuredErr = fmt.Errorf("decision storage was not reusable")
		}
	})
	require.NoError(t, measuredErr)
	require.LessOrEqual(t, allocations, 1.0, "healthy decisions should reuse the worker, timer, mask, and observation path")
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
	// Verify the compact exact lookup used by the merger filters SIDX-encoded
	// data: retained traces pass, dropped traces are filtered, and corrupt data
	// fails open.
	filter := &mergeFilter{
		chain:   newTestChain(map[string]struct{}{"traceB": {}}),
		timeout: time.Second,
	}
	_, dropped := mergeWithFilter(t, singleTraceParts([]string{"traceA", "traceB", "traceC"}), filter)
	require.Contains(t, dropped, "traceB")

	exactDropped := acquireDroppedTraceIDs()
	t.Cleanup(func() { releaseDroppedTraceIDs(exactDropped) })
	exactDropped.add("traceB")
	keepFn := exactDropped.keepEncoded

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
