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

package querybench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
)

const (
	benchTraceGroupName = "bench-trace-group"
	benchTraceName      = "sw"

	traceTagTraceID           = "trace_id"
	traceTagState             = "state"
	traceTagServiceID         = "service_id"
	traceTagServiceInstanceID = "service_instance_id"
	traceTagEndpointID        = "endpoint_id"
	traceTagDuration          = "duration"
	traceTagSpanID            = "span_id"
	traceTagTimestamp         = "timestamp"

	traceIndexTimestamp  = "timestamp"
	traceIndexDuration   = "duration"
	traceQueryLimit      = 50
	traceSegmentCount    = 16
	traceWriteBatchSpans = 20000
)

func preloadTraceBenchSchema(shardNum int) func(context.Context, schema.Registry) error {
	if shardNum <= 0 {
		shardNum = defaultShardNum
	}
	return func(ctx context.Context, registry schema.Registry) error {
		updatedAt := timestamppb.New(time.Date(2021, 4, 15, 1, 30, 15, 10*int(time.Millisecond), time.UTC))
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: benchTraceGroupName},
			Catalog:  commonv1.Catalog_CATALOG_TRACE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: uint32(shardNum),
				SegmentInterval: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY,
					Num:  1,
				},
				Ttl: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY,
					Num:  30,
				},
			},
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateGroup(ctx, group); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create trace benchmark group: %w", createErr)
		}
		trace := &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: benchTraceName, Group: benchTraceGroupName},
			Tags: []*databasev1.TraceTagSpec{
				{Name: traceTagTraceID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: traceTagState, Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: traceTagServiceID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: traceTagServiceInstanceID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: traceTagEndpointID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: traceTagDuration, Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: traceTagSpanID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: traceTagTimestamp, Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			},
			TraceIdTagName:   traceTagTraceID,
			TimestampTagName: traceTagTimestamp,
			SpanIdTagName:    traceTagSpanID,
			UpdatedAt:        updatedAt,
		}
		if _, createErr := registry.CreateTrace(ctx, trace); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create trace benchmark resource: %w", createErr)
		}
		for _, rule := range []*databasev1.IndexRule{
			{
				Metadata:  &commonv1.Metadata{Name: traceIndexTimestamp, Group: benchTraceGroupName},
				Tags:      []string{traceTagServiceID, traceTagServiceInstanceID, traceTagState, traceTagTimestamp},
				Type:      databasev1.IndexRule_TYPE_TREE,
				UpdatedAt: updatedAt,
			},
			{
				Metadata:  &commonv1.Metadata{Name: traceIndexDuration, Group: benchTraceGroupName},
				Tags:      []string{traceTagServiceID, traceTagServiceInstanceID, traceTagState, traceTagDuration},
				Type:      databasev1.IndexRule_TYPE_TREE,
				UpdatedAt: updatedAt,
			},
		} {
			if _, createErr := registry.CreateIndexRule(ctx, rule); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
				return fmt.Errorf("create trace benchmark index rule %s: %w", rule.GetMetadata().GetName(), createErr)
			}
		}
		binding := &databasev1.IndexRuleBinding{
			Metadata:  &commonv1.Metadata{Name: "bench-sw-index-rule-binding", Group: benchTraceGroupName},
			Rules:     []string{traceIndexDuration, traceIndexTimestamp},
			Subject:   &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_TRACE, Name: benchTraceName},
			BeginAt:   timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			ExpireAt:  timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateIndexRuleBinding(ctx, binding); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create trace benchmark index rule binding: %w", createErr)
		}
		return nil
	}
}

func writeTraceData(ctx context.Context, conn *grpc.ClientConn, cfg Config, cardinality int, base time.Time) (writeSummary, error) {
	client := tracev1.NewTraceServiceClient(conn)
	shape := deriveTraceShape(cardinality, cfg.SpansPerTrace, cfg.SpanDist)
	writers := cfg.Writers
	if writers <= 0 {
		writers = 1
	}
	tracesPerWriter := int(math.Ceil(float64(shape.TraceCount) / float64(writers)))
	group, groupCtx := errgroup.WithContext(ctx)
	started := time.Now()
	for writerIdx := 0; writerIdx < writers; writerIdx++ {
		writerIndex := writerIdx
		startTrace := writerIndex * tracesPerWriter
		endTrace := (writerIndex + 1) * tracesPerWriter
		if endTrace > shape.TraceCount {
			endTrace = shape.TraceCount
		}
		if startTrace >= endTrace {
			continue
		}
		group.Go(func() error {
			return writeTraceRange(groupCtx, client, cfg, shape, startTrace, endTrace, base)
		})
	}
	if waitErr := group.Wait(); waitErr != nil {
		return writeSummary{}, waitErr
	}
	elapsed := time.Since(started)
	rowsPerSec := 0.0
	if elapsed > 0 {
		rowsPerSec = float64(shape.TotalSpans) / elapsed.Seconds()
	}
	return writeSummary{Rows: shape.TotalSpans, Duration: elapsed, RowsPerSec: rowsPerSec}, nil
}

func writeTraceRange(
	ctx context.Context,
	client tracev1.TraceServiceClient,
	cfg Config,
	shape traceShape,
	startTrace, endTrace int,
	base time.Time,
) error {
	chunks := splitTraceWriteChunks(shape, startTrace, endTrace)
	targetMatches := int(math.Round(float64(shape.TraceCount) * cfg.FilterSelectivity))
	if targetMatches < 1 {
		targetMatches = 1
	}
	serviceCardinality := traceServiceCardinality(cfg.Cardinality)
	for _, chunk := range chunks {
		if writeErr := writeTraceChunk(ctx, client, cfg, shape, chunk.startTrace, chunk.endTrace, base, targetMatches, serviceCardinality); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

type traceWriteChunk struct {
	startTrace int
	endTrace   int
}

func splitTraceWriteChunks(shape traceShape, startTrace, endTrace int) []traceWriteChunk {
	if startTrace >= endTrace {
		return nil
	}
	chunks := make([]traceWriteChunk, 0, max(1, (endTrace-startTrace)/1000))
	for chunkStart := startTrace; chunkStart < endTrace; {
		chunkEnd := chunkStart
		chunkSpans := 0
		for chunkEnd < endTrace && (chunkSpans == 0 || chunkSpans+shape.SpansByTrace[chunkEnd] <= traceWriteBatchSpans) {
			chunkSpans += shape.SpansByTrace[chunkEnd]
			chunkEnd++
		}
		chunks = append(chunks, traceWriteChunk{startTrace: chunkStart, endTrace: chunkEnd})
		chunkStart = chunkEnd
	}
	return chunks
}

func writeTraceChunk(
	ctx context.Context,
	client tracev1.TraceServiceClient,
	cfg Config,
	shape traceShape,
	startTrace, endTrace int,
	base time.Time,
	targetMatches int,
	serviceCardinality int,
) error {
	stream, streamErr := client.Write(ctx)
	if streamErr != nil {
		return fmt.Errorf("open trace write stream: %w", streamErr)
	}
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				recvErrCh <- nil
				return
			}
			if recvErr != nil {
				recvErrCh <- fmt.Errorf("receive trace write response: %w", recvErr)
				return
			}
			if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				recvErrCh <- fmt.Errorf("trace write failed version=%d status=%s", resp.GetVersion(), resp.GetStatus())
				return
			}
		}
	}()
	metadata := &commonv1.Metadata{Name: benchTraceName, Group: benchTraceGroupName}
	requestMetadata := metadata
	for traceIdx := startTrace; traceIdx < endTrace; traceIdx++ {
		traceID := traceIDForIndex(traceIdx)
		serviceID := traceServiceID(traceIdx, targetMatches, serviceCardinality)
		for spanIdx := 0; spanIdx < shape.SpansByTrace[traceIdx]; spanIdx++ {
			globalSpanIdx := shape.Offsets[traceIdx] + spanIdx
			version := uint64(globalSpanIdx + 1)
			req := &tracev1.WriteRequest{
				Metadata: requestMetadata,
				Tags:     traceTags(base, version, traceIdx, spanIdx, traceID, serviceID, cfg.Cardinality),
				Span:     traceSpanPayload(cfg.SpanBytes, traceID, spanIdx),
				Version:  version,
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return fmt.Errorf("send trace write request: %w", sendErr)
			}
			requestMetadata = nil
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return fmt.Errorf("close trace write stream: %w", closeErr)
	}
	return <-recvErrCh
}

func runTraceScenarioBenchmark(ctx context.Context, conn *grpc.ClientConn, cfg Config, scenario Scenario, cardinality int, mode string, base time.Time) (Result, error) {
	shape := deriveTraceShape(cardinality, cfg.SpansPerTrace, cfg.SpanDist)
	result := Result{
		Engine:              engineTrace,
		Mode:                mode,
		Scenario:            scenario,
		Cardinality:         cardinality,
		TracesTotal:         shape.TraceCount,
		SpansPerTrace:       cfg.SpansPerTrace,
		MeanSpansPerTrace:   shape.MeanSpans,
		SpanDist:            cfg.SpanDist,
		ServiceCardinality:  traceServiceCardinality(cardinality),
		EndpointCardinality: traceEndpointCardinality(cardinality),
		FilterSelectivity:   cfg.FilterSelectivity,
		TraceIDBatch:        cfg.TraceIDBatch,
		ShardNum:            cfg.ShardNum,
		DataNodes:           cfg.DataNodes,
		SpanBytes:           cfg.SpanBytes,
		QueryMemoryMiB:      cfg.QueryMemoryMiB,
		SegmentCount:        traceSegmentCount,
		QueryIterations:     cfg.QueryIterations,
		QueryWorkers:        cfg.QueryWorkers,
		Correctness:         "baseline",
	}
	req, requestErr := buildTraceScenarioQuery(scenario, cfg, shape, base)
	if requestErr != nil {
		return result, requestErr
	}
	expectedTraces := expectedTraceResultCount(scenario, cfg, shape)
	if visibilityErr := waitForTraceVisibility(ctx, conn, req, scenario, cardinality, expectedTraces); visibilityErr != nil {
		return result, visibilityErr
	}
	profiler, profileErr := startProfileWithVariant(cfg.ReportDir, scenario, cardinality, mode, result.variantKey(), cfg.Profile)
	if profileErr != nil {
		return result, profileErr
	}
	queryCountBefore := vtrace.QueryCount()
	before := captureProcessSnapshot()
	querySummary, queryErr := runTraceScenarioQueries(ctx, conn, req, cfg, cardinality)
	after := captureProcessSnapshot()
	queryCountAfter := vtrace.QueryCount()
	profiles, stopProfileErr := profiler.stop()
	if queryErr != nil {
		return result, queryErr
	}
	if stopProfileErr != nil {
		return result, stopProfileErr
	}
	result.VecQueryCountDelta = queryCountAfter - queryCountBefore
	if mode == modeVec && result.VecQueryCountDelta <= 0 {
		return result, fmt.Errorf("trace vectorized query path did not execute: query_count_delta=%d", result.VecQueryCountDelta)
	}
	latencyStats, qps := summarizeLatencies(querySummary.Latencies, querySummary.Elapsed)
	result.ResponseRows = querySummary.Rows
	result.ResponseTraces = querySummary.Traces
	result.ResponseSpans = querySummary.Rows
	result.ApproxResultHash = querySummary.Hash
	result.SampleTraceText = querySummary.SampleTraceText
	result.Latency = latencyStats
	result.QPS = qps
	result.Resources = resourceDelta(before, after)
	result.Allocations = allocationDelta(before, after, len(querySummary.Latencies))
	result.Profiles = profiles
	return result, nil
}

type traceQueryRunSummary struct {
	SampleTraceText string
	Latencies       []time.Duration
	Rows            int
	Traces          int
	Hash            uint64
	Elapsed         time.Duration
}

func buildTraceScenarioQuery(scenario Scenario, cfg Config, shape traceShape, base time.Time) (*tracev1.QueryRequest, error) {
	req := &tracev1.QueryRequest{
		Groups: []string{benchTraceGroupName},
		Name:   benchTraceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Unix(0, 0)),
			End:   timestamppb.New(base.Add(365 * 24 * time.Hour)),
		},
		TagProjection: []string{
			traceTagTraceID, traceTagServiceID, traceTagServiceInstanceID, traceTagEndpointID,
			traceTagDuration, traceTagState, traceTagSpanID, traceTagTimestamp,
		},
	}
	switch scenario {
	case ScenarioTraceByID:
		traceIDs := traceQueryIDs(shape.TraceCount, cfg.TraceIDBatch)
		req.Criteria = traceCondition(traceTagTraceID, modelv1.Condition_BINARY_OP_IN, strArrayTagValue(traceIDs))
		req.Limit = uint32(len(traceIDs))
	case ScenarioTraceTagFilter:
		req.Criteria = traceCondition(traceTagServiceID, modelv1.Condition_BINARY_OP_EQ, strTagValue("svc-0"))
		req.OrderBy = &modelv1.QueryOrder{IndexRuleName: traceIndexTimestamp, Sort: modelv1.Sort_SORT_DESC}
		req.Limit = traceQueryLimit
	default:
		return nil, fmt.Errorf("unsupported trace scenario %q", scenario)
	}
	return req, nil
}

func expectedTraceResultCount(scenario Scenario, cfg Config, shape traceShape) int {
	switch scenario {
	case ScenarioTraceByID:
		batch := cfg.TraceIDBatch
		if batch <= 0 {
			batch = 1
		}
		if batch > shape.TraceCount {
			return shape.TraceCount
		}
		return batch
	case ScenarioTraceTagFilter:
		targetMatches := int(math.Round(float64(shape.TraceCount) * cfg.FilterSelectivity))
		if targetMatches < 1 {
			targetMatches = 1
		}
		if targetMatches > traceQueryLimit {
			return traceQueryLimit
		}
		return targetMatches
	default:
		return 1
	}
}

func waitForTraceVisibility(ctx context.Context, conn *grpc.ClientConn, req *tracev1.QueryRequest, scenario Scenario, cardinality, expectedTraces int) error {
	client := tracev1.NewTraceServiceClient(conn)
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	var lastTraceCount int
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		queryReq := proto.Clone(req).(*tracev1.QueryRequest)
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
		resp, queryErr := client.Query(queryCtx, queryReq)
		cancel()
		if queryErr == nil {
			lastTraceCount = len(resp.GetTraces())
		}
		if queryErr == nil && lastTraceCount >= expectedTraces {
			return nil
		}
		if queryErr != nil {
			lastErr = queryErr
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s trace visibility: expected_traces=%d last_traces=%d: %w", scenario, expectedTraces, lastTraceCount, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s trace visibility: expected_traces=%d last_traces=%d", scenario, expectedTraces, lastTraceCount)
}

func runTraceScenarioQueries(ctx context.Context, conn *grpc.ClientConn, req *tracev1.QueryRequest, cfg Config, cardinality int) (traceQueryRunSummary, error) {
	client := tracev1.NewTraceServiceClient(conn)
	for warmupIdx := 0; warmupIdx < cfg.WarmupIterations; warmupIdx++ {
		warmupCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
		_, warmupErr := client.Query(warmupCtx, proto.Clone(req).(*tracev1.QueryRequest))
		cancel()
		if warmupErr != nil {
			return traceQueryRunSummary{}, fmt.Errorf("trace warmup query %d failed: %w", warmupIdx, warmupErr)
		}
	}
	var sampleTraceText string
	sampleCtx, sampleCancel := context.WithTimeout(ctx, queryTimeout(cardinality))
	sampleResp, sampleErr := client.Query(sampleCtx, proto.Clone(req).(*tracev1.QueryRequest))
	sampleCancel()
	if sampleErr != nil {
		return traceQueryRunSummary{}, fmt.Errorf("trace sample query failed: %w", sampleErr)
	}
	if traces := sampleResp.GetTraces(); len(traces) > 0 {
		var b strings.Builder
		for traceIdx, trace := range traces {
			fmt.Fprintf(&b, "# trace[%d]\n%s", traceIdx, prototext.Format(trace))
		}
		sampleTraceText = b.String()
	}
	iterations := cfg.QueryIterations
	jobs := make(chan int, iterations)
	for iteration := 0; iteration < iterations; iteration++ {
		jobs <- iteration
	}
	close(jobs)
	latencies := make(chan time.Duration, iterations)
	traceCounts := make(chan int, iterations)
	spanCounts := make(chan int, iterations)
	hashes := make(chan uint64, iterations)
	group, groupCtx := errgroup.WithContext(ctx)
	started := time.Now()
	for workerIdx := 0; workerIdx < cfg.QueryWorkers; workerIdx++ {
		group.Go(func() error {
			for range jobs {
				queryReq := proto.Clone(req).(*tracev1.QueryRequest)
				queryCtx, cancel := context.WithTimeout(groupCtx, queryTimeout(cardinality))
				queryStart := time.Now()
				resp, queryErr := client.Query(queryCtx, queryReq)
				latency := time.Since(queryStart)
				cancel()
				if queryErr != nil {
					return fmt.Errorf("trace query failed: %w", queryErr)
				}
				traceCount, spanCount := countTraceResponse(resp)
				latencies <- latency
				traceCounts <- traceCount
				spanCounts <- spanCount
				hashes <- hashTraceResponse(resp, req.GetOrderBy() != nil)
			}
			return nil
		})
	}
	if waitErr := group.Wait(); waitErr != nil {
		return traceQueryRunSummary{}, waitErr
	}
	elapsed := time.Since(started)
	close(latencies)
	close(traceCounts)
	close(spanCounts)
	close(hashes)
	out := traceQueryRunSummary{Elapsed: elapsed, SampleTraceText: sampleTraceText}
	for latency := range latencies {
		out.Latencies = append(out.Latencies, latency)
	}
	firstTraceCount := -1
	for traceCount := range traceCounts {
		if firstTraceCount < 0 {
			firstTraceCount = traceCount
			continue
		}
		if traceCount != firstTraceCount {
			return traceQueryRunSummary{}, fmt.Errorf("trace counts diverged across iterations: first=%d got=%d", firstTraceCount, traceCount)
		}
	}
	if firstTraceCount >= 0 {
		out.Traces = firstTraceCount
	}
	firstSpanCount := -1
	for spanCount := range spanCounts {
		if firstSpanCount < 0 {
			firstSpanCount = spanCount
			continue
		}
		if spanCount != firstSpanCount {
			return traceQueryRunSummary{}, fmt.Errorf("span counts diverged across iterations: first=%d got=%d", firstSpanCount, spanCount)
		}
	}
	if firstSpanCount >= 0 {
		out.Rows = firstSpanCount
	}
	hashSet := false
	var firstHash uint64
	for hashValue := range hashes {
		if !hashSet {
			firstHash = hashValue
			hashSet = true
			continue
		}
		if hashValue != firstHash {
			return traceQueryRunSummary{}, fmt.Errorf("trace response hashes diverged across iterations: first=0x%x got=0x%x", firstHash, hashValue)
		}
	}
	out.Hash = firstHash
	return out, nil
}

func traceTags(base time.Time, version uint64, traceIdx, spanIdx int, traceID, serviceID string, cardinality int) []*modelv1.TagValue {
	instanceCount := traceInstanceCardinality(cardinality)
	endpointCount := traceEndpointCardinality(cardinality)
	state := int64(0)
	if traceIdx%20 == 0 {
		state = 1
	}
	duration := int64(1000 + traceIdx*10 + spanIdx)
	timestamp := base.Add(time.Duration(version) * time.Second)
	return []*modelv1.TagValue{
		strTagValue(traceID),
		intTagValue(state),
		strTagValue(serviceID),
		strTagValue(fmt.Sprintf("%s-inst-%d", serviceID, spanIdx%instanceCount)),
		strTagValue(fmt.Sprintf("%s-endpoint-%d", serviceID, spanIdx%endpointCount)),
		intTagValue(duration),
		strTagValue(fmt.Sprintf("%s-span-%04d", traceID, spanIdx)),
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(timestamp)}},
	}
}

func traceSpanPayload(spanBytes int, traceID string, spanIdx int) []byte {
	if spanBytes <= 0 {
		spanBytes = defaultSpanBytes
	}
	prefix := []byte(fmt.Sprintf("%s:%04d:", traceID, spanIdx))
	if len(prefix) >= spanBytes {
		return prefix[:spanBytes]
	}
	payload := make([]byte, 0, spanBytes)
	payload = append(payload, prefix...)
	payload = append(payload, bytes.Repeat([]byte("x"), spanBytes-len(prefix))...)
	return payload
}

func traceCondition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: name, Op: op, Value: value}}}
}

func strArrayTagValue(values []string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}

func intTagValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}}
}

func traceQueryIDs(traceCount, requestedBatch int) []string {
	if requestedBatch <= 0 {
		requestedBatch = 1
	}
	if requestedBatch > traceCount {
		requestedBatch = traceCount
	}
	start := traceCount/2 - requestedBatch/2
	if start < 0 {
		start = 0
	}
	ids := make([]string, 0, requestedBatch)
	for idx := 0; idx < requestedBatch; idx++ {
		ids = append(ids, traceIDForIndex(start+idx))
	}
	return ids
}

func traceIDForIndex(traceIdx int) string {
	return fmt.Sprintf("trace-%010d", traceIdx)
}

func traceServiceID(traceIdx, targetMatches, serviceCardinality int) string {
	if traceIdx < targetMatches {
		return "svc-0"
	}
	if serviceCardinality <= 1 {
		return "svc-0"
	}
	return fmt.Sprintf("svc-%d", 1+(traceIdx-targetMatches)%(serviceCardinality-1))
}

func traceServiceCardinality(cardinality int) int {
	switch {
	case cardinality <= 1000:
		return 8
	case cardinality <= 10000:
		return 20
	case cardinality <= 100000:
		return 40
	default:
		return 50
	}
}

func traceInstanceCardinality(cardinality int) int {
	switch {
	case cardinality <= 1000:
		return 2
	case cardinality <= 10000:
		return 3
	default:
		return 5
	}
}

func traceEndpointCardinality(cardinality int) int {
	switch {
	case cardinality <= 1000:
		return 5
	case cardinality <= 10000:
		return 8
	default:
		return 12
	}
}

func countTraceResponse(resp *tracev1.QueryResponse) (int, int) {
	traceCount := len(resp.GetTraces())
	spanCount := 0
	for _, trace := range resp.GetTraces() {
		spanCount += len(trace.GetSpans())
	}
	return traceCount, spanCount
}

func hashTraceResponse(resp *tracev1.QueryResponse, preserveTraceOrder bool) uint64 {
	h := fnv.New64a()
	traces := append([]*tracev1.Trace(nil), resp.GetTraces()...)
	if !preserveTraceOrder {
		sort.Slice(traces, func(left, right int) bool {
			return traces[left].GetTraceId() < traces[right].GetTraceId()
		})
	}
	for _, trace := range traces {
		clone := proto.Clone(trace).(*tracev1.Trace)
		sort.Slice(clone.Spans, func(left, right int) bool {
			leftSpan := clone.Spans[left]
			rightSpan := clone.Spans[right]
			if leftSpan.GetSpanId() != rightSpan.GetSpanId() {
				return leftSpan.GetSpanId() < rightSpan.GetSpanId()
			}
			leftBytes, leftErr := proto.MarshalOptions{Deterministic: true}.Marshal(leftSpan)
			rightBytes, rightErr := proto.MarshalOptions{Deterministic: true}.Marshal(rightSpan)
			if leftErr != nil || rightErr != nil {
				return left < right
			}
			return string(leftBytes) < string(rightBytes)
		})
		body, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
		if marshalErr != nil {
			continue
		}
		_, _ = h.Write(body)
	}
	return h.Sum64()
}
