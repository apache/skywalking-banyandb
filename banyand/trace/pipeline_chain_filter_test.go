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
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	testtrace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// latencyStatusSampler drops a trace iff its duration tag is below the latency
// threshold AND its status tag equals successValue. It reads the duration and
// status tags from row 0 and fails open (retain) on missing/null columns.
type latencyStatusSampler struct {
	durationTag        string
	statusTag          string
	successValue       string
	latencyThresholdMs int64
}

func (s *latencyStatusSampler) Kind() sdk.Kind { return sdk.KindSampler }

func (s *latencyStatusSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: []string{s.durationTag, s.statusTag}}
}

func (s *latencyStatusSampler) Close() error { return nil }

func (s *latencyStatusSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = !s.shouldDrop(&batch.Traces[i])
	}
	return sdk.Verdict{Keep: keep}, nil
}

func (s *latencyStatusSampler) shouldDrop(tb *sdk.TraceBlock) bool {
	durCol := tb.Tag(s.durationTag)
	statusCol := tb.Tag(s.statusTag)
	if durCol == nil || statusCol == nil || tb.Len() == 0 {
		return false
	}
	durVal, durErr := durCol.At(0)
	if durErr != nil || durVal.IsNull() {
		return false
	}
	statusVal, statusErr := statusCol.At(0)
	if statusErr != nil || statusVal.IsNull() {
		return false
	}
	return durVal.Int64() < s.latencyThresholdMs && statusVal.Str() == s.successValue
}

// panicSampler panics in Decide to exercise the chain's fail-open recover path.
type panicSampler struct {
	durationTag string
	statusTag   string
}

func (s *panicSampler) Kind() sdk.Kind { return sdk.KindSampler }

func (s *panicSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: []string{s.durationTag, s.statusTag}}
}

func (s *panicSampler) Close() error { return nil }

func (s *panicSampler) Decide(_ *sdk.TraceBatch) (sdk.Verdict, error) {
	panic("panicSampler always panics")
}

type filterTestServices struct {
	trace           Service
	metadataService metadata.Service
	pipeline        queue.Queue
	rootPath        string
}

type filterPreloadSvc struct {
	metaSvc metadata.Service
}

func (p *filterPreloadSvc) Name() string { return "preload-trace-filter" }

func (p *filterPreloadSvc) PreRun(ctx context.Context) error {
	return testtrace.PreloadSchema(ctx, p.metaSvc.SchemaRegistry())
}

func setUpWithPipelineFlags(t *testing.T, extraFlags ...string) (*filterTestServices, func()) {
	t.Helper()
	gomega.RegisterTestingT(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	pipeline := queue.Local()
	metadataService, err := service.NewService()
	require.NoError(t, err)
	metricSvc := obsservice.NewMetricService(metadataService, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	traceService, err := NewService(metadataService, pipeline, metricSvc, pm)
	require.NoError(t, err)
	preloadSvc := &filterPreloadSvc{metaSvc: metadataService}
	metaPath, metaDeferFn, err := test.NewSpace()
	require.NoError(t, err)
	schemaPorts, err := test.AllocateFreePorts(1)
	require.NoError(t, err)
	rootPath, rootDeferFn, err := test.NewSpace()
	require.NoError(t, err)
	var setupFlags []string
	setupFlags = append(setupFlags,
		"--schema-server-root-path="+metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", schemaPorts[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--trace-root-path="+rootPath,
		"--trace-max-merge-parts=2",
	)
	setupFlags = append(setupFlags, extraFlags...)
	moduleDeferFn := test.SetupModules(setupFlags, pipeline, metadataService, preloadSvc, traceService)
	return &filterTestServices{
			trace:           traceService,
			metadataService: metadataService,
			pipeline:        pipeline,
			rootPath:        rootPath,
		}, func() {
			moduleDeferFn()
			metaDeferFn()
			rootDeferFn()
		}
}

func setupFilterTestGroup(t *testing.T, svcs *filterTestServices, groupName, traceName string) {
	t.Helper()
	ctx := context.TODO()
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: groupName},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  7,
			},
		},
	}
	_, createGroupErr := svcs.metadataService.GroupRegistry().CreateGroup(ctx, g)
	require.NoError(t, createGroupErr)
	tags := []*databasev1.TraceTagSpec{
		{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
		{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	traceSchema := &databasev1.Trace{
		Metadata:         &commonv1.Metadata{Name: traceName, Group: groupName},
		Tags:             tags,
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	}
	_, createTraceErr := svcs.metadataService.TraceRegistry().CreateTrace(ctx, traceSchema)
	require.NoError(t, createTraceErr)
	require.Eventually(t, func() bool {
		_, loadErr := svcs.trace.Trace(&commonv1.Metadata{Name: traceName, Group: groupName})
		return loadErr == nil
	}, flags.EventuallyTimeout, 100*time.Millisecond)
}

type traceSpec struct {
	traceID  string
	status   string
	durMs    int64
	tsOffset time.Duration
}

func writeFilterTraces(t *testing.T, svcs *filterTestServices, traceName, groupName string, baseTime time.Time, specs []traceSpec) {
	t.Helper()
	half := len(specs) / 2
	if half == 0 {
		half = 1
	}
	writeBatch := func(batch []traceSpec, startIdx int) {
		bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
		defer bp.Close()
		for idx, spec := range batch {
			offset := spec.tsOffset
			if offset == 0 {
				offset = time.Duration(startIdx+idx) * 100 * time.Millisecond
			}
			ts := baseTime.Add(offset).Truncate(time.Millisecond)
			tagNames := []string{"trace_id", "span_id", "timestamp", "service_id", "duration"}
			tags := []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: spec.traceID}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "sp-" + spec.traceID}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-1"}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: spec.durMs}}},
			}
			if spec.status != "" {
				tagNames = append(tagNames, "status")
				tags = append(tags, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: spec.status}}})
			}
			req := &tracev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: traceName, Group: groupName},
				Tags:     tags,
				Span:     []byte("span-" + spec.traceID),
				TagSpec:  &tracev1.TagSpec{TagNames: tagNames},
			}
			bp.Publish(context.TODO(), data.TopicTraceWrite,
				bus.NewMessage(bus.MessageID(time.Now().UnixNano()+int64(idx)), &tracev1.InternalWriteRequest{Request: req}))
		}
	}

	initialParts := collectDataInfoPartCount(svcs, groupName)
	writeBatch(specs[:half], 0)
	require.Eventually(t, func() bool {
		return collectDataInfoPartCount(svcs, groupName) > initialParts
	}, 3*flags.EventuallyTimeout, 200*time.Millisecond, "first batch must produce a file part")

	afterFirstBatch := collectDataInfoPartCount(svcs, groupName)
	writeBatch(specs[half:], half)
	require.Eventually(t, func() bool {
		return collectDataInfoPartCount(svcs, groupName) > afterFirstBatch
	}, 3*flags.EventuallyTimeout, 200*time.Millisecond, "second batch must produce a file part")
}

func collectDataInfoPartCount(svcs *filterTestServices, group string) int64 {
	info, err := svcs.trace.CollectDataInfo(context.TODO(), group)
	if err != nil || info == nil {
		return 0
	}
	var total int64
	for _, seg := range info.SegmentInfo {
		for _, shard := range seg.ShardInfo {
			total += shard.PartCount
		}
	}
	return total
}

func diskSurvivors(t *testing.T, svcTrace Service, groupName string) []string {
	t.Helper()
	g, ok := svcTrace.LoadGroup(groupName)
	require.True(t, ok, "group %s must be loaded", groupName)
	tsdb, ok := g.SupplyTSDB().(storage.TSDB[*tsTable, option])
	require.True(t, ok, "SupplyTSDB must return the trace TSDB")
	segments, selectErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	require.NoError(t, selectErr)
	seen := make(map[string]struct{})
	for _, seg := range segments {
		tables, _ := seg.Tables()
		for _, tst := range tables {
			tst.RLock()
			snp := tst.snapshot
			tst.RUnlock()
			if snp == nil {
				continue
			}
			snp.incRef()
			for _, pw := range snp.parts {
				if pw.p == nil {
					continue
				}
				pmi := generatePartMergeIter()
				pmi.mustInitFromPart(pw.p)
				br := generateBlockReader()
				br.init([]*partMergeIter{pmi})
				for br.nextBlockMetadata() {
					seen[br.block.bm.traceID] = struct{}{}
				}
				releaseBlockReader(br)
				releasePartMergeIter(pmi)
			}
			snp.decRef()
		}
		seg.DecRef()
	}
	survivors := make([]string, 0, len(seen))
	for id := range seen {
		survivors = append(survivors, id)
	}
	sort.Strings(survivors)
	return survivors
}

// diskSurvivorSpans returns a map of traceID → first span bytes for every trace
// found in the on-disk snapshot. It decodes the full block data so any
// decoder-buffer aliasing corruption would surface as wrong/empty bytes.
func diskSurvivorSpans(t *testing.T, svcTrace Service, groupName string) map[string][]byte {
	t.Helper()
	g, ok := svcTrace.LoadGroup(groupName)
	require.True(t, ok, "group %s must be loaded", groupName)
	tsdb, ok := g.SupplyTSDB().(storage.TSDB[*tsTable, option])
	require.True(t, ok, "SupplyTSDB must return the trace TSDB")
	segments, selectErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	require.NoError(t, selectErr)
	result := make(map[string][]byte)
	for _, seg := range segments {
		tables, _ := seg.Tables()
		for _, tst := range tables {
			tst.RLock()
			snp := tst.snapshot
			tst.RUnlock()
			if snp == nil {
				continue
			}
			snp.incRef()
			for _, pw := range snp.parts {
				if pw.p == nil {
					continue
				}
				pmi := generatePartMergeIter()
				pmi.mustInitFromPart(pw.p)
				br := generateBlockReader()
				br.init([]*partMergeIter{pmi})
				dec := generateColumnValuesDecoder()
				for br.nextBlockMetadata() {
					br.loadBlockData(dec)
					traceID := br.block.bm.traceID
					if len(br.block.block.spans) > 0 && result[traceID] == nil {
						result[traceID] = append([]byte(nil), br.block.block.spans[0]...)
					}
				}
				releaseColumnValuesDecoder(dec)
				releaseBlockReader(br)
				releasePartMergeIter(pmi)
			}
			snp.decRef()
		}
		seg.DecRef()
	}
	return result
}

func waitForMergeToSinglePart(t *testing.T, svcs *filterTestServices, groupName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return collectDataInfoPartCount(svcs, groupName) <= 1
	}, 3*flags.EventuallyTimeout, 500*time.Millisecond, "merge must reduce parts to 1")
}

func instantFilterSpecs() []traceSpec {
	return []traceSpec{
		{traceID: "t-drop-1", durMs: 100, status: "success", tsOffset: 0},
		{traceID: "t-drop-2", durMs: 499, status: "success", tsOffset: 100 * time.Millisecond},
		{traceID: "t-keep-boundary", durMs: 500, status: "success", tsOffset: 200 * time.Millisecond},
		{traceID: "t-keep-highlat", durMs: 800, status: "success", tsOffset: 300 * time.Millisecond},
		{traceID: "t-keep-err-fast", durMs: 50, status: "error", tsOffset: 400 * time.Millisecond},
		{traceID: "t-keep-err-slow", durMs: 900, status: "error", tsOffset: 500 * time.Millisecond},
		{traceID: "t-keep-nostatus", durMs: 100, status: "", tsOffset: 600 * time.Millisecond},
	}
}

func TestInMergeFilter_Instant_Standalone(t *testing.T) {
	svcs, teardown := setUpWithPipelineFlags(t,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-merge-grace-default=0",
		"--trace-pipeline-decide-timeout=5s",
		"--trace-pipeline-decide-timeout-circuit-break=0",
	)
	defer teardown()

	const (
		groupName = "filter-instant-group"
		traceName = "filter-instant-trace"
	)
	setupFilterTestGroup(t, svcs, groupName, traceName)

	sampler := &latencyStatusSampler{
		durationTag:        "duration",
		statusTag:          "status",
		successValue:       "success",
		latencyThresholdMs: 500,
	}
	deregister := registerSampler(groupName, sampler)
	defer deregister()

	baseTime := time.Now().Add(-2 * time.Hour)
	specs := instantFilterSpecs()
	keepSet := map[string]bool{
		"t-keep-boundary": true, "t-keep-highlat": true,
		"t-keep-err-fast": true, "t-keep-err-slow": true, "t-keep-nostatus": true,
	}
	writeFilterTraces(t, svcs, traceName, groupName, baseTime, specs)
	waitForMergeToSinglePart(t, svcs, groupName)

	// Drop/keep correctness is covered by the out-of-process integration suite
	// (test/integration/{standalone,distributed}/pipeline). This test retains
	// the on-disk payload-integrity guard: it decodes full block data so any
	// decoder-buffer aliasing corruption surfaces as wrong or empty bytes.
	spans := diskSurvivorSpans(t, svcs.trace, groupName)
	for id := range keepSet {
		wantSpan := []byte("span-" + id)
		require.Equal(t, wantSpan, spans[id], "kept trace %s must have intact span payload", id)
	}
}

// TestInMergeFilter_ChunkedStaging_TinyBudget forces the bounded-chunk staging
// path: a 1-byte stage budget makes mergeBlocks flush a chunk at every trace
// boundary (one trace per chunk) — the maximal-churn case. It asserts the
// chunked path yields the same drop/keep outcome as a single-batch flush and
// keeps each surviving trace's span payload intact across chunk boundaries,
// guarding the per-chunk deep-copy lifetime, the ascending-order write
// requirement, and the cross-chunk droppedSet that drives sidx pruning.
func TestInMergeFilter_ChunkedStaging_TinyBudget(t *testing.T) {
	// Force a 1-byte staging budget so a chunk flushes at every trace boundary
	// (one trace per chunk) — the maximal-churn case. testStageBudgetOverride is
	// the test-only seam; production derives the budget from the memory limit.
	// Reset after teardown (defer LIFO) so a final merge still sees the override.
	testStageBudgetOverride = 1
	defer func() { testStageBudgetOverride = 0 }()

	svcs, teardown := setUpWithPipelineFlags(t,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-merge-grace-default=0",
		"--trace-pipeline-decide-timeout=5s",
		"--trace-pipeline-decide-timeout-circuit-break=0",
	)
	defer teardown()

	const (
		groupName = "filter-chunked-group"
		traceName = "filter-chunked-trace"
	)
	setupFilterTestGroup(t, svcs, groupName, traceName)

	sampler := &latencyStatusSampler{
		durationTag:        "duration",
		statusTag:          "status",
		successValue:       "success",
		latencyThresholdMs: 500,
	}
	deregister := registerSampler(groupName, sampler)
	defer deregister()

	baseTime := time.Now().Add(-2 * time.Hour)
	specs := instantFilterSpecs()
	keepSet := map[string]bool{
		"t-keep-boundary": true, "t-keep-highlat": true,
		"t-keep-err-fast": true, "t-keep-err-slow": true, "t-keep-nostatus": true,
	}
	dropSet := map[string]bool{"t-drop-1": true, "t-drop-2": true}

	writeFilterTraces(t, svcs, traceName, groupName, baseTime, specs)
	waitForMergeToSinglePart(t, svcs, groupName)

	survivors := diskSurvivors(t, svcs.trace, groupName)
	survivorSet := make(map[string]bool, len(survivors))
	for _, id := range survivors {
		survivorSet[id] = true
	}
	for id := range keepSet {
		require.True(t, survivorSet[id], "chunked: kept trace %s must survive the merge", id)
	}
	for id := range dropSet {
		require.False(t, survivorSet[id], "chunked: dropped trace %s must be pruned even with per-trace chunking", id)
	}

	spans := diskSurvivorSpans(t, svcs.trace, groupName)
	for id := range keepSet {
		require.Equal(t, []byte("span-"+id), spans[id], "chunked: kept trace %s span payload must be intact across chunk boundaries", id)
	}
}

// TestStageBudgetFromLimit verifies the flag-free, memory-limit-derived staging
// budget: a disabled protector falls back to the lane-split threshold, a tiny
// limit clamps up to the floor, a huge limit clamps down to the fast/slow lane
// ceiling, and every budget stays within [floor, ceiling].
func TestStageBudgetFromLimit(t *testing.T) {
	floor := uint64(defaultStageBudgetFloor)
	ceiling := computeSmallMergeThreshold()
	require.LessOrEqual(t, floor, ceiling, "floor must not exceed ceiling")

	require.Equal(t, uint64(defaultSmallMergeThreshold), stageBudgetFromLimit(0),
		"disabled protector (limit 0) falls back to the lane-split threshold")
	require.Equal(t, floor, stageBudgetFromLimit(1), "a tiny limit clamps up to the floor")
	require.Equal(t, ceiling, stageBudgetFromLimit(1<<50), "a huge limit clamps down to the ceiling")

	for _, limit := range []uint64{64 << 20, 512 << 20, 4 << 30, 64 << 30} {
		b := stageBudgetFromLimit(limit)
		require.GreaterOrEqual(t, b, floor, "limit=%d: budget below floor", limit)
		require.LessOrEqual(t, b, ceiling, "limit=%d: budget above ceiling", limit)
	}
}

func TestInMergeFilter_DisabledFlag_LegacyIdentity(t *testing.T) {
	svcs, teardown := setUpWithPipelineFlags(t,
		"--trace-pipeline-merge-grace-default=0",
	)
	defer teardown()

	const (
		groupName = "filter-disabled-group"
		traceName = "filter-disabled-trace"
	)
	setupFilterTestGroup(t, svcs, groupName, traceName)

	sampler := &latencyStatusSampler{
		durationTag:        "duration",
		statusTag:          "status",
		successValue:       "success",
		latencyThresholdMs: 500,
	}
	deregister := registerSampler(groupName, sampler)
	defer deregister()

	baseTime := time.Now().Add(-2 * time.Hour)
	specs := instantFilterSpecs()

	writeFilterTraces(t, svcs, traceName, groupName, baseTime, specs)
	waitForMergeToSinglePart(t, svcs, groupName)

	survivors := diskSurvivors(t, svcs.trace, groupName)
	survivorSet := make(map[string]bool, len(survivors))
	for _, id := range survivors {
		survivorSet[id] = true
	}
	for _, spec := range specs {
		require.True(t, survivorSet[spec.traceID], "flag disabled: trace %s must be present on disk", spec.traceID)
	}
}

func TestInMergeFilter_FailOpenNegativeControl(t *testing.T) {
	svcs, teardown := setUpWithPipelineFlags(t,
		"--trace-pipeline-native-plugin-enabled=true",
		"--trace-pipeline-merge-grace-default=0",
		"--trace-pipeline-decide-timeout=5s",
		"--trace-pipeline-decide-timeout-circuit-break=0",
	)
	defer teardown()

	const (
		groupName = "filter-failopen-group"
		traceName = "filter-failopen-trace"
	)
	setupFilterTestGroup(t, svcs, groupName, traceName)

	deregister := registerSampler(groupName, &panicSampler{durationTag: "duration", statusTag: "status"})
	defer deregister()

	baseTime := time.Now().Add(-2 * time.Hour)
	specs := instantFilterSpecs()

	writeFilterTraces(t, svcs, traceName, groupName, baseTime, specs)
	waitForMergeToSinglePart(t, svcs, groupName)

	survivors := diskSurvivors(t, svcs.trace, groupName)
	survivorSet := make(map[string]bool, len(survivors))
	for _, id := range survivors {
		survivorSet[id] = true
	}
	for _, spec := range specs {
		require.True(t, survivorSet[spec.traceID], "panicking sampler must fail open: trace %s on disk", spec.traceID)
	}
}
