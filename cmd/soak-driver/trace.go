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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// Trace soak fixture constants. The parity fixture group keeps its data for
// the duration of the run (long TTL), while the load group rolls a short TTL
// so background write-load traffic never accumulates unbounded.
const (
	traceFixtureGroup = "bench-trace-fixture"
	traceLoadGroup    = "bench-trace-load"
	traceName         = "sw"

	traceFixtureTraces = 200
	traceFixtureSpans  = 5
	traceSpanBytes     = 1024

	traceTagTraceID    = "trace_id"
	traceTagState      = "state"
	traceTagServiceID  = "service_id"
	traceTagInstanceID = "service_instance_id"
	traceTagEndpointID = "endpoint_id"
	traceTagDuration   = "duration"
	traceTagSpanID     = "span_id"
	traceTagTimestamp  = "timestamp"

	traceIndexTimestamp = "timestamp"
	traceIndexDuration  = "duration"

	// traceSvcZeroFraction is the share of generated traces assigned to
	// service_id "svc-0". The catalog's svc-0 filters select exactly this
	// fraction, so it must stay in lock-step with the generator.
	traceSvcZeroFraction = 0.5
	traceServiceCount    = 8
	traceInstanceCount   = 4
	traceEndpointCount   = 6
)

// traceCatalogEntry holds one query template from the trace JSON catalog.
type traceCatalogEntry struct {
	Request *tracev1.QueryRequest
	ID      string
}

// traceBaselineRecord is persisted to disk after trace record-baseline runs.
// Traces holds proto-JSON encoded tracev1.Trace messages.
type traceBaselineRecord struct {
	QueryName string            `json:"query_name"`
	Traces    []json.RawMessage `json:"traces"`
	Groups    []string          `json:"groups"`
	UntilMs   int64             `json:"until_ms"`
	Ordered   bool              `json:"ordered"`
}

// loadTraceCatalog reads the trace JSON catalog and returns its entries. The
// on-disk shape reuses rawCatalogEntry ({id, request}); the request bytes are
// decoded with protojson so proto oneofs (Criteria) and enums (Sort, BinaryOp)
// round-trip correctly.
func loadTraceCatalog(path string) ([]traceCatalogEntry, error) {
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, fmt.Errorf("read trace catalog %s: %w", path, readErr)
	}
	var rawEntries []rawCatalogEntry
	if unmarshalErr := json.Unmarshal(raw, &rawEntries); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshal trace catalog array: %w", unmarshalErr)
	}
	entries := make([]traceCatalogEntry, 0, len(rawEntries))
	for idx, rawEntry := range rawEntries {
		if rawEntry.ID == "" {
			return nil, fmt.Errorf("trace catalog entry %d: missing id", idx)
		}
		req := new(tracev1.QueryRequest)
		if protoErr := protojson.Unmarshal(rawEntry.Request, req); protoErr != nil {
			return nil, fmt.Errorf("unmarshal trace catalog entry %q: %w", rawEntry.ID, protoErr)
		}
		entries = append(entries, traceCatalogEntry{ID: rawEntry.ID, Request: req})
	}
	return entries, nil
}

// buildTraceQueryRequest injects a [until-window, until] TimeRange into a
// cloned catalog request. The catalog's pinned Limit is preserved verbatim —
// each shape carries the limit the parity check expects.
func buildTraceQueryRequest(entry traceCatalogEntry, untilMs int64, window time.Duration) *tracev1.QueryRequest {
	untilTime := time.UnixMilli(untilMs)
	beginTime := untilTime.Add(-window)
	req, _ := proto.Clone(entry.Request).(*tracev1.QueryRequest)
	req.TimeRange = &modelv1.TimeRange{
		Begin: timestamppb.New(beginTime),
		End:   timestamppb.New(untilTime),
	}
	return req
}

// traceCatalogEntryOrdered reports whether the catalog query pins a result
// order via order_by. Ordered queries are diffed position-by-position;
// unordered queries are sorted before comparison.
func traceCatalogEntryOrdered(entry traceCatalogEntry) bool {
	return entry.Request.GetOrderBy().GetIndexRuleName() != ""
}

// traceStrTagValue wraps a string in a model TagValue.
func traceStrTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

// traceIntTagValue wraps an int64 in a model TagValue.
func traceIntTagValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}}
}

// traceIDForIndex returns the deterministic trace ID for a trace index. The
// format matches the querybench generator so the catalog's pinned IDs select
// real fixture traces.
func traceIDForIndex(traceIdx int) string {
	return fmt.Sprintf("trace-%010d", traceIdx)
}

// traceServiceIDForIndex returns the per-TRACE service_id. The first
// traceSvcZeroFraction of traces are svc-0 so the svc-0 catalog filters select
// an exact, reproducible fraction; the remainder spread across svc-1..N.
func traceServiceIDForIndex(traceIdx, traceCount int) string {
	svcZeroCount := int(float64(traceCount) * traceSvcZeroFraction)
	if traceIdx < svcZeroCount {
		return "svc-0"
	}
	return fmt.Sprintf("svc-%d", 1+(traceIdx-svcZeroCount)%(traceServiceCount-1))
}

// traceSpanState returns the per-span state. 95% of spans are state 0 (OK);
// the remainder are state 1 (error), spread deterministically.
func traceSpanState(globalSpanIdx int) int64 {
	if globalSpanIdx%20 == 0 {
		return 1
	}
	return 0
}

// traceSpanPayload builds a ~spanBytes raw span body prefixed with identity so
// each span's payload is unique yet reproducible.
func traceSpanPayload(spanBytes int, traceID string, spanIdx int) []byte {
	if spanBytes <= 0 {
		spanBytes = traceSpanBytes
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

// traceSpanTags builds the ordered tag values for one span. The order matches
// the Trace schema tag spec: trace_id, state, service_id, service_instance_id,
// endpoint_id, duration, span_id, timestamp.
func traceSpanTags(base time.Time, version uint64, traceIdx, spanIdx int, traceID, serviceID string) []*modelv1.TagValue {
	globalSpanIdx := traceIdx*traceFixtureSpans + spanIdx
	state := traceSpanState(globalSpanIdx)
	duration := int64(1000 + traceIdx*10 + spanIdx)
	timestamp := base.Add(time.Duration(version) * time.Second)
	return []*modelv1.TagValue{
		traceStrTagValue(traceID),
		traceIntTagValue(state),
		traceStrTagValue(serviceID),
		traceStrTagValue(fmt.Sprintf("%s-inst-%d", serviceID, spanIdx%traceInstanceCount)),
		traceStrTagValue(fmt.Sprintf("%s-endpoint-%d", serviceID, spanIdx%traceEndpointCount)),
		traceIntTagValue(duration),
		traceStrTagValue(fmt.Sprintf("%s-span-%04d", traceID, spanIdx)),
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(timestamp)}},
	}
}

// traceCreateGroup creates a CATALOG_TRACE group idempotently.
func traceCreateGroup(ctx context.Context, client databasev1.GroupRegistryServiceClient, name string, ttlDays uint32) error {
	_, createErr := client.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name},
			Catalog:  commonv1.Catalog_CATALOG_TRACE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY,
					Num:  1,
				},
				Ttl: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY,
					Num:  ttlDays,
				},
			},
		},
	})
	if createErr != nil && !traceAlreadyExists(createErr) {
		return fmt.Errorf("create trace group %s: %w", name, createErr)
	}
	return nil
}

// traceAlreadyExists reports whether err is an idempotent "already exists" error.
func traceAlreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exist")
}

// traceSeedFixture creates the two trace groups (parity fixture with a long
// retain TTL, plus a rolling short-TTL load group), a Trace "sw" clone, the
// timestamp/duration index rules, and the binding — all idempotently. It then
// writes the deterministic fixture into the parity group and prints T1_MS, the
// highest span timestamp (unix ms), to stdout.
func traceSeedFixture(ctx context.Context, conn *grpc.ClientConn, traces, spans int) error {
	if traces <= 0 {
		traces = traceFixtureTraces
	}
	if spans <= 0 {
		spans = traceFixtureSpans
	}
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	// Parity fixture group: long TTL so the data stays queryable for the run.
	if groupErr := traceCreateGroup(ctx, groupClient, traceFixtureGroup, 30); groupErr != nil {
		return groupErr
	}
	// Load group: short TTL so rolling background writes self-expire.
	if groupErr := traceCreateGroup(ctx, groupClient, traceLoadGroup, 1); groupErr != nil {
		return groupErr
	}

	traceClient := databasev1.NewTraceRegistryServiceClient(conn)
	indexRuleClient := databasev1.NewIndexRuleRegistryServiceClient(conn)
	bindingClient := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	for _, group := range []string{traceFixtureGroup, traceLoadGroup} {
		if schemaErr := traceCreateSchema(ctx, traceClient, indexRuleClient, bindingClient, group); schemaErr != nil {
			return schemaErr
		}
	}

	base := traceFixtureBaseTime(traces * spans)
	highestMs, writeErr := traceWriteFixture(ctx, conn, traceFixtureGroup, traces, spans, base, 0)
	if writeErr != nil {
		return writeErr
	}
	fmt.Printf("[trace seed-fixture] wrote %d traces x %d spans to %s/%s\n", traces, spans, traceFixtureGroup, traceName)
	fmt.Printf("T1_MS=%d\n", highestMs)
	fmt.Printf("%d\n", highestMs)
	return nil
}

// traceCreateSchema creates the Trace resource, its index rules, and binding
// for one group, idempotently.
func traceCreateSchema(
	ctx context.Context,
	traceClient databasev1.TraceRegistryServiceClient,
	indexRuleClient databasev1.IndexRuleRegistryServiceClient,
	bindingClient databasev1.IndexRuleBindingRegistryServiceClient,
	group string,
) error {
	trace := &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: traceName, Group: group},
		Tags: []*databasev1.TraceTagSpec{
			{Name: traceTagTraceID, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: traceTagState, Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: traceTagServiceID, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: traceTagInstanceID, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: traceTagEndpointID, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: traceTagDuration, Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: traceTagSpanID, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: traceTagTimestamp, Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName:   traceTagTraceID,
		TimestampTagName: traceTagTimestamp,
		SpanIdTagName:    traceTagSpanID,
	}
	if _, createErr := traceClient.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{Trace: trace}); createErr != nil && !traceAlreadyExists(createErr) {
		return fmt.Errorf("create trace resource in %s: %w", group, createErr)
	}

	rules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: traceIndexTimestamp, Group: group},
			Tags:     []string{traceTagServiceID, traceTagInstanceID, traceTagState, traceTagTimestamp},
			Type:     databasev1.IndexRule_TYPE_TREE,
		},
		{
			Metadata: &commonv1.Metadata{Name: traceIndexDuration, Group: group},
			Tags:     []string{traceTagServiceID, traceTagInstanceID, traceTagState, traceTagDuration},
			Type:     databasev1.IndexRule_TYPE_TREE,
		},
	}
	for _, rule := range rules {
		if _, createErr := indexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule}); createErr != nil && !traceAlreadyExists(createErr) {
			return fmt.Errorf("create trace index rule %s in %s: %w", rule.GetMetadata().GetName(), group, createErr)
		}
	}

	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "sw-index-rule-binding", Group: group},
		Rules:    []string{traceIndexDuration, traceIndexTimestamp},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_TRACE, Name: traceName},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	if _, createErr := bindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: binding}); createErr != nil && !traceAlreadyExists(createErr) {
		return fmt.Errorf("create trace index rule binding in %s: %w", group, createErr)
	}
	return nil
}

// traceFixtureBaseTime anchors the deterministic span timestamps to recent
// wall-clock time. Span timestamps are base + version*second for version up to
// totalSpans, so the newest lands ~1 minute in the past and the oldest ~totalSpans
// seconds before that. They must fall inside the group's TTL retention window
// (which is relative to now) or BanyanDB silently drops them and queries return
// empty — a fixed calendar base lands outside retention once wall-clock moves on.
// Within-run determinism does not depend on the calendar instant: Phase 0 seeds +
// snapshots and Phase 1 restores that snapshot, so both phases see identical data.
func traceFixtureBaseTime(totalSpans int) time.Time {
	const marginSec = 60
	return time.Now().UTC().Truncate(time.Second).Add(-time.Duration(totalSpans+marginSec) * time.Second)
}

// traceWriteFixture writes traces*spans deterministic spans into group via the
// streaming TraceService.Write RPC, checking every per-response status. Version
// is monotonic across all spans, offset by versionBase so distinct write runs
// (e.g. write-load) never collide. Returns the highest span timestamp in ms.
func traceWriteFixture(ctx context.Context, conn *grpc.ClientConn, group string, traces, spans int, base time.Time, versionBase uint64) (int64, error) {
	client := tracev1.NewTraceServiceClient(conn)
	stream, streamErr := client.Write(ctx)
	if streamErr != nil {
		return 0, fmt.Errorf("open trace write stream: %w", streamErr)
	}

	recvErrCh := make(chan error, 1)
	go func() {
		var firstBad string
		bad := 0
		for {
			resp, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				if bad > 0 {
					recvErrCh <- fmt.Errorf("trace write rejected %d spans (first status: %q)", bad, firstBad)
					return
				}
				recvErrCh <- nil
				return
			}
			if recvErr != nil {
				recvErrCh <- fmt.Errorf("receive trace write response: %w", recvErr)
				return
			}
			if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				bad++
				if firstBad == "" {
					firstBad = resp.GetStatus()
				}
			}
		}
	}()

	metadata := &commonv1.Metadata{Name: traceName, Group: group}
	requestMetadata := metadata
	var highestMs int64
	for traceIdx := 0; traceIdx < traces; traceIdx++ {
		traceID := traceIDForIndex(traceIdx)
		serviceID := traceServiceIDForIndex(traceIdx, traces)
		for spanIdx := 0; spanIdx < spans; spanIdx++ {
			globalSpanIdx := traceIdx*spans + spanIdx
			version := versionBase + uint64(globalSpanIdx) + 1
			tags := traceSpanTags(base, version, traceIdx, spanIdx, traceID, serviceID)
			tsMs := base.Add(time.Duration(version) * time.Second).UnixMilli()
			if tsMs > highestMs {
				highestMs = tsMs
			}
			req := &tracev1.WriteRequest{
				Metadata: requestMetadata,
				Tags:     tags,
				Span:     traceSpanPayload(traceSpanBytes, traceID, spanIdx),
				Version:  version,
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return 0, fmt.Errorf("send trace write request: %w", sendErr)
			}
			requestMetadata = nil
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return 0, fmt.Errorf("close trace write stream: %w", closeErr)
	}
	if recvErr := <-recvErrCh; recvErr != nil {
		return 0, recvErr
	}
	return highestMs, nil
}

// traceRecordBaseline runs the catalog over [T1-window, T1], persists the
// resulting traces as proto-JSON, and exits non-zero if any catalog query
// returns an empty result (an empty baseline is not a usable parity reference).
func traceRecordBaseline(ctx context.Context, conn *grpc.ClientConn, catalogPath, outPath string, untilMs int64, window time.Duration) error {
	entries, loadErr := loadTraceCatalog(catalogPath)
	if loadErr != nil {
		return loadErr
	}
	client := tracev1.NewTraceServiceClient(conn)
	records := make([]traceBaselineRecord, 0, len(entries))
	var emptyQueries []string
	for _, entry := range entries {
		req := buildTraceQueryRequest(entry, untilMs, window)
		queryName := entry.ID
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, queryErr := client.Query(queryCtx, req)
		cancel()
		if queryErr != nil {
			return fmt.Errorf("record-baseline %s: query failed: %w", queryName, queryErr)
		}
		rec := traceBaselineRecord{
			QueryName: queryName,
			Groups:    entry.Request.GetGroups(),
			UntilMs:   untilMs,
			Ordered:   traceCatalogEntryOrdered(entry),
		}
		for _, trace := range resp.GetTraces() {
			rawTrace, marshalErr := protojson.Marshal(trace)
			if marshalErr != nil {
				return fmt.Errorf("marshal trace for %s: %w", queryName, marshalErr)
			}
			rec.Traces = append(rec.Traces, json.RawMessage(rawTrace))
		}
		records = append(records, rec)
		if len(rec.Traces) == 0 {
			emptyQueries = append(emptyQueries, queryName)
		}
		fmt.Printf("[trace record-baseline] %s: %d traces\n", queryName, len(rec.Traces))
	}

	out, createErr := os.Create(outPath)
	if createErr != nil {
		return fmt.Errorf("create output %s: %w", outPath, createErr)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(records); encErr != nil {
		return fmt.Errorf("encode trace baseline: %w", encErr)
	}
	fmt.Printf("[trace record-baseline] written to %s\n", outPath)
	if len(emptyQueries) > 0 {
		return fmt.Errorf("record-baseline: %d catalog queries returned no traces: %s", len(emptyQueries), strings.Join(emptyQueries, ", "))
	}
	return nil
}

// traceReplayAndDiff re-runs the catalog against the recorded baseline and
// writes a diffReport (reusing the measure-path report shape so soak-monitor.sh
// grep patterns match). It returns a non-zero error on any divergence.
func traceReplayAndDiff(ctx context.Context, conn *grpc.ClientConn, catalogPath, baselinePath, reportPath string, window time.Duration) error {
	raw, readErr := os.ReadFile(baselinePath)
	if readErr != nil {
		return fmt.Errorf("read trace baseline %s: %w", baselinePath, readErr)
	}
	var records []traceBaselineRecord
	if unmarshalErr := json.Unmarshal(raw, &records); unmarshalErr != nil {
		return fmt.Errorf("unmarshal trace baseline: %w", unmarshalErr)
	}
	entries, loadErr := loadTraceCatalog(catalogPath)
	if loadErr != nil {
		return loadErr
	}
	client := tracev1.NewTraceServiceClient(conn)
	report := diffReport{
		RunAt: time.Now().UTC().Format(time.RFC3339),
		Pass:  true,
	}
	baselineMap := make(map[string]traceBaselineRecord, len(records))
	for _, rec := range records {
		baselineMap[rec.QueryName] = rec
	}

	for _, entry := range entries {
		queryName := entry.ID
		rec, ok := baselineMap[queryName]
		if !ok {
			fmt.Printf("[trace replay-and-diff] %s: SKIP (no baseline)\n", queryName)
			continue
		}
		req := buildTraceQueryRequest(entry, rec.UntilMs, window)
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, queryErr := client.Query(queryCtx, req)
		cancel()
		if queryErr != nil {
			report.Divergences = append(report.Divergences, divergence{QueryName: queryName})
			report.Pass = false
			fmt.Printf("[trace replay-and-diff] %s: FAIL (%v)\n", queryName, queryErr)
			continue
		}
		report.QueriesRun++

		baselineTraces, parseErr := decodeBaselineTraces(rec.Traces, queryName)
		if parseErr != nil {
			return parseErr
		}
		div, matched := compareTraceResults(queryName, baselineTraces, resp.GetTraces(), rec.Ordered)
		if !matched {
			report.Divergences = append(report.Divergences, div)
			report.Pass = false
		}
	}

	outFile, createErr := os.Create(reportPath)
	if createErr != nil {
		return fmt.Errorf("create trace report %s: %w", reportPath, createErr)
	}
	defer outFile.Close()
	enc := json.NewEncoder(outFile)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(report); encErr != nil {
		return fmt.Errorf("encode trace report: %w", encErr)
	}
	fmt.Printf("[trace replay-and-diff] %d queries run, %d divergences — %s\n",
		report.QueriesRun, len(report.Divergences), map[bool]string{true: "PASS", false: "FAIL"}[report.Pass])
	if !report.Pass {
		return fmt.Errorf("parity check failed: %d divergences found", len(report.Divergences))
	}
	return nil
}

// decodeBaselineTraces decodes the proto-JSON trace blobs from a baseline record.
func decodeBaselineTraces(raws []json.RawMessage, queryName string) ([]*tracev1.Trace, error) {
	traces := make([]*tracev1.Trace, 0, len(raws))
	for idx, raw := range raws {
		trace := new(tracev1.Trace)
		if parseErr := protojson.Unmarshal(raw, trace); parseErr != nil {
			return nil, fmt.Errorf("unmarshal baseline trace %d for %s: %w", idx, queryName, parseErr)
		}
		traces = append(traces, trace)
	}
	return traces, nil
}

// compareTraceResults diffs baseline vs replay traces. Spans within each trace
// are sorted by (span_id, marshaled bytes); unordered queries also sort traces
// by trace_id, while ordered queries preserve position. It returns a divergence
// and false on the first mismatch (length or content); the divergence carries up
// to three first-diff samples for the report.
func compareTraceResults(queryName string, baseline, replay []*tracev1.Trace, ordered bool) (divergence, bool) {
	div := divergence{QueryName: queryName, BaselineLen: len(baseline), ReplayLen: len(replay)}
	if len(baseline) != len(replay) {
		return div, false
	}
	baselineNorm := normalizeTraces(baseline, ordered)
	replayNorm := normalizeTraces(replay, ordered)
	matched := true
	for idx := range baselineNorm {
		if !proto.Equal(baselineNorm[idx], replayNorm[idx]) {
			matched = false
			if len(div.FirstDiffs) < 3 {
				div.FirstDiffs = append(div.FirstDiffs, pointDiff{
					Index:    idx,
					Baseline: baselineNorm[idx].String(),
					Replay:   replayNorm[idx].String(),
				})
			}
		}
	}
	return div, matched
}

// normalizeTraces returns clones with spans sorted by (span_id, bytes); when
// not ordered, traces are also sorted by trace_id so set comparison is stable.
func normalizeTraces(traces []*tracev1.Trace, ordered bool) []*tracev1.Trace {
	clones := make([]*tracev1.Trace, 0, len(traces))
	for _, trace := range traces {
		clone, _ := proto.Clone(trace).(*tracev1.Trace)
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
			return bytes.Compare(leftBytes, rightBytes) < 0
		})
		clones = append(clones, clone)
	}
	if !ordered {
		sort.Slice(clones, func(left, right int) bool {
			return clones[left].GetTraceId() < clones[right].GetTraceId()
		})
	}
	return clones
}

// traceWriteLoad runs continuous deterministic writes into the rolling load
// group, OUTSIDE the parity window (separate group), rate-capped at rps. It
// returns the number of spans written. Writes proceed in batches of one full
// fixture sweep, advancing versionBase so spans stay unique.
func traceWriteLoad(ctx context.Context, conn *grpc.ClientConn, traces, spans, rps int, duration time.Duration) (int, error) {
	if traces <= 0 {
		traces = traceFixtureTraces
	}
	if spans <= 0 {
		spans = traceFixtureSpans
	}
	if rps <= 0 {
		rps = 1000
	}
	spansPerSweep := traces * spans
	// Load traffic goes to a SEPARATE group (traceLoadGroup) that the parity
	// catalog never queries, so group isolation — not timestamp offsetting —
	// keeps it from polluting parity. Anchor it to recent time so it lands
	// inside the load group's (short) TTL retention window.
	base := traceFixtureBaseTime(spansPerSweep)
	deadline := time.Now().Add(duration)
	totalSpans := 0
	var versionBase uint64
	interval := time.Second / time.Duration(rps)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return totalSpans, ctx.Err()
		default:
		}
		sweepStart := time.Now()
		if _, writeErr := traceWriteFixture(ctx, conn, traceLoadGroup, traces, spans, base, versionBase); writeErr != nil {
			return totalSpans, writeErr
		}
		totalSpans += spansPerSweep
		versionBase += uint64(spansPerSweep)
		// Rate-cap: a sweep emits spansPerSweep spans, which at rps should
		// take spansPerSweep*interval. Sleep off any remaining budget.
		elapsed := time.Since(sweepStart)
		budget := time.Duration(spansPerSweep) * interval
		if elapsed < budget {
			time.Sleep(budget - elapsed)
		}
		fmt.Printf("[trace write-load] sweep done: %d spans total\n", totalSpans)
	}
	return totalSpans, nil
}
