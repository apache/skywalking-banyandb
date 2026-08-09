// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trace

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type implementationGuardFilter struct {
	membership traceFragmentMembership
}

func (f implementationGuardFilter) Lookup(_ string) (traceFragmentMembership, error) {
	return f.membership, nil
}

type implementationBlockingGuardFilter struct {
	entered chan struct{}
	release chan struct{}
}

func (f implementationBlockingGuardFilter) Lookup(_ string) (traceFragmentMembership, error) {
	close(f.entered)
	<-f.release
	return traceFragmentMembershipAbsent, nil
}

type implementationGuardPin struct {
	releases int
}

func (p *implementationGuardPin) Release() {
	p.releases++
}

type implementationDropSampler struct {
	dropID string
	calls  atomic.Int64
}

type implementationBlockingDropSampler struct {
	entered chan struct{}
	release chan struct{}
	dropID  string
	calls   atomic.Int64
}

func (s *implementationBlockingDropSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (s *implementationBlockingDropSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *implementationBlockingDropSampler) Close() error            { return nil }
func (s *implementationBlockingDropSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls.Add(1)
	close(s.entered)
	<-s.release
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		keep[traceIdx] = batch.Traces[traceIdx].TraceID != s.dropID
	}
	return sdk.Verdict{Keep: keep}, nil
}

func (s *implementationDropSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (s *implementationDropSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *implementationDropSampler) Close() error            { return nil }
func (s *implementationDropSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls.Add(1)
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		keep[traceIdx] = batch.Traces[traceIdx].TraceID != s.dropID
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestAssembleStagedTraceBlockAggregatesMetadataOnlyBlocks(t *testing.T) {
	staged := []stagedTrace{
		{
			isRaw:   true,
			traceID: "trace-a",
			rawBM: &blockMetadata{
				traceID:    "trace-a",
				timestamps: timestampsMetadata{min: 200, max: 210, known: true},
			},
		},
		{
			isRaw:   true,
			traceID: "trace-a",
			rawBM: &blockMetadata{
				traceID:    "trace-a",
				timestamps: timestampsMetadata{min: 100, max: 110, known: true},
			},
		},
	}

	group := stagedTraceGroup{traceID: "trace-a", start: 0, end: 2, minTS: 100, maxTS: 210, validMetadata: true}
	assembled, complete := assembleStagedTraceBlock(group, staged, sdk.Projection{})

	require.True(t, complete)
	assert.Equal(t, "trace-a", assembled.TraceID)
	assert.Equal(t, int64(100), assembled.MinTS)
	assert.Equal(t, int64(210), assembled.MaxTS)
	assert.Empty(t, assembled.Tags)
	assert.Empty(t, assembled.SpanIDs)
	assert.Empty(t, assembled.Spans)
}

func TestAssembleStagedTraceBlockAggregatesEveryProjectedRow(t *testing.T) {
	first := &blockPointer{
		block: block{
			spans:   [][]byte{[]byte("span-1")},
			spanIDs: []string{"span-id-1"},
			tags: []tag{{
				name:      "service",
				values:    [][]byte{[]byte("service-a")},
				valueType: pbv1.ValueTypeStr,
			}},
			minTS: 100,
			maxTS: 110,
		},
		bm: blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 100, max: 110, known: true},
		},
	}
	second := &blockPointer{
		block: block{
			spans:   [][]byte{[]byte("span-2")},
			spanIDs: []string{"span-id-2"},
			tags: []tag{{
				name:      "status",
				values:    [][]byte{[]byte("ok")},
				valueType: pbv1.ValueTypeStr,
			}},
			minTS: 200,
			maxTS: 210,
		},
		bm: blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 200, max: 210, known: true},
		},
	}
	staged := []stagedTrace{
		{traceID: "trace-a", slowBlock: first},
		{traceID: "trace-a", slowBlock: second},
	}

	group := stagedTraceGroup{traceID: "trace-a", start: 0, end: 2, minTS: 100, maxTS: 210, validMetadata: true}
	assembled, complete := assembleStagedTraceBlock(group, staged, sdk.Projection{
		Tags:    []string{"service", "status"},
		SpanIDs: true,
		Spans:   true,
	})

	require.True(t, complete)
	assert.Equal(t, int64(100), assembled.MinTS)
	assert.Equal(t, int64(210), assembled.MaxTS)
	assert.Equal(t, []string{"span-id-1", "span-id-2"}, assembled.SpanIDs)
	assert.Equal(t, [][]byte{[]byte("span-1"), []byte("span-2")}, assembled.Spans)
	require.Len(t, assembled.Tags, 2)
	assert.Equal(t, "service", assembled.Tags[0].Name)
	assert.Equal(t, [][]byte{[]byte("service-a"), nil}, assembled.Tags[0].Values)
	assert.Equal(t, "status", assembled.Tags[1].Name)
	assert.Equal(t, [][]byte{nil, []byte("ok")}, assembled.Tags[1].Values)
}

func TestAssembleStagedTraceBlockFailsOpenForUnknownBounds(t *testing.T) {
	staged := []stagedTrace{{
		isRaw:   true,
		traceID: "trace-a",
		rawBM: &blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 100, max: 110},
		},
	}}

	group := stagedTraceGroup{traceID: "trace-a", start: 0, end: 1, minTS: 100, maxTS: 110}
	_, complete := assembleStagedTraceBlock(group, staged, sdk.Projection{})

	assert.False(t, complete)
}

func TestUnknownBlockTimestampBoundsSurviveReadRewriteAndMerge(t *testing.T) {
	sourceSpans := &pkgbytes.Buffer{}
	sourceWriter := &writer{}
	sourceWriter.init(sourceSpans)
	sourceWriters := &writers{spanWriter: *sourceWriter}
	source := block{
		spans:                  [][]byte{[]byte("unknown-span")},
		spanIDs:                []string{"unknown-span-id"},
		minTS:                  100,
		maxTS:                  110,
		timestampBoundsUnknown: true,
	}
	var sourceMetadata blockMetadata
	source.mustWriteTo("trace-a", &sourceMetadata, sourceWriters)
	require.False(t, sourceMetadata.timestamps.known)

	encodedMetadata := sourceMetadata.marshal(nil)
	var persistedMetadata blockMetadata
	remaining, unmarshalErr := persistedMetadata.unmarshal(encodedMetadata, nil)
	require.NoError(t, unmarshalErr)
	require.Empty(t, remaining)
	require.False(t, persistedMetadata.timestamps.known)

	sourcePart := &part{
		primary:     &pkgbytes.Buffer{},
		spans:       sourceSpans,
		tagMetadata: make(map[string]fs.Reader),
		tags:        make(map[string]fs.Reader),
	}
	var sourceReaders seqReaders
	sourceReaders.init(sourcePart)
	defer sourceReaders.reset()
	var decoder encoding.BytesBlockDecoder
	var decoded block
	decoded.mustSeqReadFrom(&decoder, &sourceReaders, persistedMetadata)
	require.True(t, decoded.timestampBoundsUnknown)

	rewrittenSpans := &pkgbytes.Buffer{}
	rewrittenWriter := &writer{}
	rewrittenWriter.init(rewrittenSpans)
	var rewrittenMetadata blockMetadata
	decoded.mustWriteTo("trace-a", &rewrittenMetadata, &writers{spanWriter: *rewrittenWriter})
	assert.False(t, rewrittenMetadata.timestamps.known)

	known := &blockPointer{
		block: block{
			spans:   [][]byte{[]byte("known-span")},
			spanIDs: []string{"known-span-id"},
			minTS:   90,
			maxTS:   95,
		},
		bm: blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 90, max: 95, known: true},
		},
	}
	unknown := &blockPointer{
		block: decoded,
		bm:    persistedMetadata,
	}
	var merged blockPointer
	mergeTwoBlocks(&merged, known, unknown)

	assert.True(t, merged.timestampBoundsUnknown)
	assert.False(t, merged.bm.timestamps.known)
}

func TestTraceFragmentGuardClonesCatalogAndBoundsConfirmedDrops(t *testing.T) {
	pin := &implementationGuardPin{}
	outside := []traceFragmentGuardPart{{
		Filter:       implementationGuardFilter{membership: traceFragmentMembershipAbsent},
		ID:           1,
		MinTimestamp: 100,
		MaxTimestamp: 110,
		BoundsKnown:  true,
	}}
	guard := newTraceFragmentGuard(traceFragmentGuardConfig{
		Grace:             10 * time.Nanosecond,
		MaxBloomProbes:    4,
		MaxConfirmedDrops: 1,
	}, traceFragmentGuardCatalog{
		Pin:                    pin,
		OutsideParts:           outside,
		BaseEpoch:              7,
		CoverageMinTimestamp:   math.MinInt64,
		CoverageMaxTimestamp:   math.MaxInt64,
		EnforcedMaxFragmentGap: 10 * time.Nanosecond,
		Complete:               true,
		CoverageKnown:          true,
		TemporalSafety:         traceFragmentTemporalSafetyMaxGapEnforced,
	})
	defer guard.Close()

	outside[0] = traceFragmentGuardPart{
		Filter:       implementationGuardFilter{membership: traceFragmentMembershipMaybePresent},
		ID:           2,
		MinTimestamp: 1_000,
		MaxTimestamp: 2_000,
		BoundsKnown:  true,
	}
	first := guard.Resolve(context.Background(), traceFragmentGuardTrace{
		TraceID: "trace-a",
		Blocks: []traceFragmentGuardBlock{{
			MinTimestamp: 100,
			MaxTimestamp: 110,
			BoundsKnown:  true,
		}},
		Complete: true,
	}, traceFragmentSamplerActionDrop)
	require.Equal(t, traceFragmentGuardActionDrop, first.Action)
	require.NotNil(t, first.ConfirmedDrop)

	second := guard.Resolve(context.Background(), traceFragmentGuardTrace{
		TraceID: "trace-b",
		Blocks: []traceFragmentGuardBlock{{
			MinTimestamp: 100,
			MaxTimestamp: 110,
			BoundsKnown:  true,
		}},
		Complete: true,
	}, traceFragmentSamplerActionDrop)
	assert.Equal(t, traceFragmentGuardActionDefer, second.Action)
	assert.Equal(t, traceFragmentGuardReasonBudgetExhausted, second.Reason)
}

func TestTraceFragmentGuardAfterCloseFailsOpen(t *testing.T) {
	pin := &implementationGuardPin{}
	guard := newTraceFragmentGuard(traceFragmentGuardConfig{
		Grace:          time.Nanosecond,
		MaxBloomProbes: 1,
	}, traceFragmentGuardCatalog{
		Pin:                    pin,
		BaseEpoch:              1,
		CoverageMinTimestamp:   math.MinInt64,
		CoverageMaxTimestamp:   math.MaxInt64,
		EnforcedMaxFragmentGap: time.Nanosecond,
		Complete:               true,
		CoverageKnown:          true,
		TemporalSafety:         traceFragmentTemporalSafetyMaxGapEnforced,
	})

	guard.Close()
	guard.Close()

	dropDecision := guard.Resolve(context.Background(), traceFragmentGuardTrace{
		TraceID: "trace-a",
		Blocks: []traceFragmentGuardBlock{{
			MinTimestamp: 100,
			MaxTimestamp: 110,
			BoundsKnown:  true,
		}},
		Complete: true,
	}, traceFragmentSamplerActionDrop)
	revalidation := guard.RevalidateDrops(context.Background(), traceFragmentGuardRevalidationRequest{
		CurrentEpoch:            1,
		DeltaCatalogComplete:    true,
		OwnershipUnchanged:      true,
		SelectedInputsUnchanged: true,
		PublicationFenceHeld:    true,
	})

	assert.Equal(t, traceFragmentGuardActionDefer, dropDecision.Action)
	assert.Equal(t, traceFragmentGuardReasonCatalogUnpinned, dropDecision.Reason)
	assert.False(t, revalidation.Publish)
	assert.Equal(t, traceFragmentGuardReasonCatalogUnpinned, revalidation.Reason)
	assert.Equal(t, 1, pin.releases)
}

func TestTraceFragmentGuardCloseWaitsForActiveLookup(t *testing.T) {
	pin := &implementationGuardPin{}
	filterData := implementationBlockingGuardFilter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	guard := newTraceFragmentGuard(traceFragmentGuardConfig{
		Grace:          time.Nanosecond,
		MaxBloomProbes: 1,
	}, traceFragmentGuardCatalog{
		Pin: pin,
		OutsideParts: []traceFragmentGuardPart{{
			Filter:       filterData,
			ID:           1,
			MinTimestamp: 100,
			MaxTimestamp: 110,
			BoundsKnown:  true,
		}},
		BaseEpoch:              1,
		CoverageMinTimestamp:   math.MinInt64,
		CoverageMaxTimestamp:   math.MaxInt64,
		EnforcedMaxFragmentGap: time.Nanosecond,
		Complete:               true,
		CoverageKnown:          true,
		TemporalSafety:         traceFragmentTemporalSafetyMaxGapEnforced,
	})
	resolveDone := make(chan traceFragmentGuardDecision, 1)
	go func() {
		resolveDone <- guard.Resolve(context.Background(), traceFragmentGuardTrace{
			TraceID: "trace-a",
			Blocks: []traceFragmentGuardBlock{{
				MinTimestamp: 100,
				MaxTimestamp: 110,
				BoundsKnown:  true,
			}},
			Complete: true,
		}, traceFragmentSamplerActionDrop)
	}()
	<-filterData.entered

	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	go func() {
		close(closeStarted)
		guard.Close()
		close(closeDone)
	}()
	<-closeStarted
	select {
	case <-closeDone:
		t.Fatal("guard closed while a pinned filter lookup was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(filterData.release)

	decision := <-resolveDone
	<-closeDone
	assert.Equal(t, traceFragmentGuardActionDrop, decision.Action)
	assert.Equal(t, 1, pin.releases)
}

func TestRuntimeTraceFragmentGuardDefersOutsideBloomPositive(t *testing.T) {
	const group = "runtime-positive"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationDropSampler{dropID: "trace-a"}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTable(t, group)

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	waitForImplementationFilePartCount(t, tst, 2)
	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	allParts := waitForImplementationFileParts(t, tst, 3)
	selected := allParts[:2]
	defer releaseImplementationParts(allParts)

	mergedIDs := implementationPartIDs(selected)
	closeCh := make(chan struct{})
	defer close(closeCh)
	_, mergeErr := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, selected, mergedIDs, tst.mergeCh, closeCh, mergeTypeFile, mergeLaneFast, nil,
	)

	require.NoError(t, mergeErr)
	assert.Positive(t, sampler.calls.Load())
	assert.Equal(t, uint64(3), snapshotTotalCount(tst), "the selected fragment must survive while the same trace remains outside")
}

func TestRuntimeTraceFragmentGuardDropsAfterOutsideBloomNegative(t *testing.T) {
	const group = "runtime-negative"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationDropSampler{dropID: "trace-a"}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTable(t, group)

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	waitForImplementationFilePartCount(t, tst, 2)
	tst.mustAddTraces(tracesWithIDs("outside-c"), nil)
	allParts := waitForImplementationFileParts(t, tst, 3)
	selected := allParts[:2]
	outside := allParts[2]
	defer releaseImplementationParts(allParts)
	require.False(t, outside.p.traceIDFilter.filter.MightContain([]byte("trace-a")),
		"the fixture must exercise a Bloom-negative outside candidate")

	mergedIDs := implementationPartIDs(selected)
	closeCh := make(chan struct{})
	defer close(closeCh)
	_, mergeErr := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, selected, mergedIDs, tst.mergeCh, closeCh, mergeTypeFile, mergeLaneFast, nil,
	)

	require.NoError(t, mergeErr)
	assert.Positive(t, sampler.calls.Load())
	assert.Equal(t, uint64(2), snapshotTotalCount(tst), "the confirmed selected trace should be removed while unrelated outside data remains")
}

func TestRuntimeTraceFragmentGuardPrevalidatesOutsideIntroducerAndRetriesLosslessly(t *testing.T) {
	const group = "runtime-prevalidate"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationBlockingDropSampler{
		dropID:  "trace-a",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTable(t, group)

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	selected := waitForImplementationFileParts(t, tst, 2)
	defer releaseImplementationParts(selected)

	pendingIntroductions := make(chan *mergerIntroduction)
	type mergeResult struct {
		part *partWrapper
		err  error
	}
	resultCh := make(chan mergeResult, 1)
	closeCh := make(chan struct{})
	defer close(closeCh)
	go func() {
		mergedPart, mergeErr := tst.mergePartsThenSendIntroduction(
			snapshotCreatorMerger, selected, implementationPartIDs(selected), pendingIntroductions,
			closeCh, mergeTypeFile, mergeLaneFast, nil,
		)
		resultCh <- mergeResult{part: mergedPart, err: mergeErr}
	}()

	select {
	case <-sampler.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for sampler")
	}
	writeDone := make(chan struct{})
	go func() {
		tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
		close(writeDone)
	}()
	select {
	case <-writeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent introduction was blocked by guard work")
	}
	close(sampler.release)

	var losslessPending *mergerIntroduction
	select {
	case losslessPending = <-pendingIntroductions:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for lossless retry output")
	}
	require.Nil(t, losslessPending.guard, "a positive delta must discard the filtered output before publication")
	select {
	case tst.mergeCh <- losslessPending:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out forwarding lossless retry output")
	}

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.NotNil(t, result.part)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for lossless retry")
	}
	assert.Equal(t, int64(1), sampler.calls.Load())
	assert.Equal(t, uint64(3), snapshotTotalCount(tst))
	waitForImplementationFilePartCount(t, tst, 2)
}

func TestRuntimeTraceFragmentGuardRetriesLosslesslyWhenPublicationEpochChanges(t *testing.T) {
	const group = "runtime-delta"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationDropSampler{dropID: "trace-a"}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTable(t, group)

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	selected := waitForImplementationFileParts(t, tst, 2)
	defer releaseImplementationParts(selected)
	mergedIDs := implementationPartIDs(selected)

	pendingIntroductions := make(chan *mergerIntroduction)
	type mergeResult struct {
		part *partWrapper
		err  error
	}
	resultCh := make(chan mergeResult, 1)
	closeCh := make(chan struct{})
	defer close(closeCh)
	go func() {
		mergedPart, mergeErr := tst.mergePartsThenSendIntroduction(
			snapshotCreatorMerger, selected, mergedIDs, pendingIntroductions, closeCh, mergeTypeFile, mergeLaneFast, nil,
		)
		resultCh <- mergeResult{part: mergedPart, err: mergeErr}
	}()

	var pending *mergerIntroduction
	select {
	case pending = <-pendingIntroductions:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for provisional merge output")
	}
	firstOutputPath := pending.newPart.p.path
	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	require.Eventually(t, func() bool {
		return snapshotTotalCount(tst) == 3
	}, 10*time.Second, 20*time.Millisecond)
	select {
	case tst.mergeCh <- pending:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out forwarding provisional output to the introducer")
	}

	var losslessPending *mergerIntroduction
	select {
	case losslessPending = <-pendingIntroductions:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for lossless retry output")
	}
	require.Nil(t, losslessPending.guard, "the retry must bypass destructive sampling")
	select {
	case tst.mergeCh <- losslessPending:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out forwarding lossless retry output to the introducer")
	}

	var result mergeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for lossless retry publication")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.part)
	assert.False(t, tst.fileSystem.IsExist(firstOutputPath), "the rejected filtered output must be removed")
	assert.Positive(t, sampler.calls.Load())
	assert.Equal(t, uint64(3), snapshotTotalCount(tst), "the retry must retain both selected traces and the concurrent fragment")
	waitForImplementationFilePartCount(t, tst, 2)
}

func TestRuntimeTraceFragmentGuardBypassesSamplingWithoutGapProof(t *testing.T) {
	const group = "runtime-no-gap-proof"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationDropSampler{dropID: "trace-a"}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTableWithMaxGap(t, group, 0)

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	parts := waitForImplementationFileParts(t, tst, 2)
	defer releaseImplementationParts(parts)

	assert.Nil(t, tst.buildHotMergeFilter(parts))
	tst.option.maxTraceFragmentGap = 2 * time.Millisecond
	assert.Nil(t, tst.buildHotMergeFilter(parts), "a fragment-gap contract larger than merge grace cannot authorize pruning")
	tst.option.maxTraceFragmentGap = time.Millisecond
	segmentTimeRange := tst.segmentTimeRange
	tst.segmentTimeRange = timestamp.TimeRange{}
	assert.Nil(t, tst.buildHotMergeFilter(parts), "unknown segment coverage must bypass sampler staging")
	tst.segmentTimeRange = segmentTimeRange
	tst.option.maxTraceFragmentGap = 0

	closeCh := make(chan struct{})
	defer close(closeCh)
	_, mergeErr := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, parts, implementationPartIDs(parts), tst.mergeCh, closeCh, mergeTypeFile, mergeLaneFast, nil,
	)
	require.NoError(t, mergeErr)
	assert.Zero(t, sampler.calls.Load())
	assert.Equal(t, uint64(2), snapshotTotalCount(tst))
}

func TestRuntimeTraceFragmentGuardUsesFragmentGapForSegmentInterior(t *testing.T) {
	const group = "runtime-guard-gap"
	resetRegistries()
	t.Cleanup(resetRegistries)
	sampler := &implementationDropSampler{dropID: "trace-a"}
	deregister := registerSampler(group, sampler)
	t.Cleanup(deregister)
	tst := newImplementationGuardTable(t, group)
	tst.option.mergeGraceDefault = 2 * time.Hour

	tst.mustAddTraces(tracesWithIDs("trace-a"), nil)
	waitForImplementationFilePartCount(t, tst, 1)
	tst.mustAddTraces(tracesWithIDs("trace-b"), nil)
	parts := waitForImplementationFileParts(t, tst, 2)
	defer releaseImplementationParts(parts)

	filter := tst.buildHotMergeFilter(parts)
	require.NotNil(t, filter, "the 2h maturity grace must not be reused as the 1ms boundary expansion")
	require.NotNil(t, filter.guard)
	require.Equal(t, resolveStageBudget(tst.option), filter.stagingHardLimit)
	require.Positive(t, filter.decisionBatchLimit)
	require.LessOrEqual(t, filter.decisionBatchLimit, filter.stagingHardLimit)
	require.Equal(t, maxStagedTraceCountFromBudget(filter.decisionBatchLimit), filter.maxTraceCount)
	filter.guard.Close()

	tst.option.maxTraceFragmentGap = 2 * time.Second
	assert.Nil(t, tst.buildHotMergeFilter(parts), "sampling must bypass when the guarded segment has no safe interior")
}

func newImplementationGuardTable(t *testing.T, group string) *tsTable {
	t.Helper()
	return newImplementationGuardTableWithMaxGap(t, group, time.Millisecond)
}

func newImplementationGuardTableWithMaxGap(t *testing.T, group string, maxTraceFragmentGap time.Duration) *tsTable {
	t.Helper()
	tmpPath, cleanup := test.Space(require.New(t))
	t.Cleanup(cleanup)
	tst, tableErr := newTSTable(
		fs.NewLocalFileSystem(),
		tmpPath,
		common.Position{Database: group},
		logger.GetLogger(group),
		timestamp.NewInclusiveTimeRange(time.Unix(-1, 0), time.Unix(1, 0)),
		option{
			flushTimeout:              0,
			mergePolicy:               newMergePolicy(3, 1, run.Bytes(0)),
			protector:                 protector.Nop{},
			decideTimeout:             time.Second,
			decideTimeoutCircuitBreak: 3,
			mergeGraceDefault:         time.Millisecond,
			maxTraceFragmentGap:       maxTraceFragmentGap,
			nativePipelineEnabled:     true,
		},
		nil,
	)
	require.NoError(t, tableErr)
	t.Cleanup(func() {
		require.NoError(t, tst.Close())
	})
	return tst
}

func waitForImplementationFilePartCount(t *testing.T, tst *tsTable, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		snapshotData := tst.currentSnapshot()
		if snapshotData == nil {
			return false
		}
		defer snapshotData.decRef()
		fileParts := 0
		for partIdx := range snapshotData.parts {
			partData := snapshotData.parts[partIdx]
			if partData.mp == nil && partData.p.partMetadata.TotalCount > 0 {
				fileParts++
			}
		}
		return fileParts == expected
	}, 10*time.Second, 20*time.Millisecond)
}

func waitForImplementationFileParts(t *testing.T, tst *tsTable, expected int) []*partWrapper {
	t.Helper()
	waitForImplementationFilePartCount(t, tst, expected)
	snapshotData := tst.currentSnapshot()
	require.NotNil(t, snapshotData)
	defer snapshotData.decRef()
	parts := make([]*partWrapper, 0, expected)
	for partIdx := range snapshotData.parts {
		partData := snapshotData.parts[partIdx]
		if partData.mp != nil || partData.p.partMetadata.TotalCount == 0 {
			continue
		}
		partData.incRef()
		parts = append(parts, partData)
	}
	sort.Slice(parts, func(leftIdx, rightIdx int) bool {
		return parts[leftIdx].ID() < parts[rightIdx].ID()
	})
	require.Len(t, parts, expected)
	return parts
}

func implementationPartIDs(parts []*partWrapper) map[uint64]struct{} {
	partIDs := make(map[uint64]struct{}, len(parts))
	for partIdx := range parts {
		partIDs[parts[partIdx].ID()] = struct{}{}
	}
	return partIDs
}

func releaseImplementationParts(parts []*partWrapper) {
	for partIdx := range parts {
		parts[partIdx].decRef()
	}
}
