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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

// Stream soak fixture constants. The parity fixture group keeps its data for the
// run (long TTL); the load group rolls a short TTL so background write-load
// traffic self-expires. Mirrors the trace soak layout.
const (
	streamFixtureGroup = "bench-stream-fixture"
	streamLoadGroup    = "bench-stream-load"
	streamName         = "sw"

	streamFixtureSeries          = 8
	streamFixtureElementsPerSeri = 40

	streamTagFamily     = "searchable"
	streamTagServiceID  = "service_id"
	streamTagState      = "state"
	streamTagDuration   = "duration"
	streamIndexDuration = "duration"

	// streamIndexDurationID is the explicit IndexRule id. It is the field key the
	// index-order query resolves against, so it must be non-zero. Each soak group
	// is dedicated to this fixture, so a fixed id is safe.
	streamIndexDurationID = 1

	// streamSchemaSettle is how long seed-fixture waits after creating the stream
	// schema before writing, so the data node's schema watch has applied the
	// duration index rule + binding. Elements written before the rule lands never
	// get tree-index entries, which no amount of later waiting repairs.
	streamSchemaSettle = 5 * time.Second

	// streamVisibleTimeout caps the post-write visibility poll. It covers the
	// async data flush AND the (slower) local element-index flush that the
	// index-order query depends on.
	streamVisibleTimeout = 90 * time.Second

	// streamSvcZeroFraction is the share of series assigned to svc-0; the
	// catalog's svc-0 filters select exactly this set, so it stays in lock-step
	// with the generator.
	streamSvcZeroFraction = 0.5
)

// streamCatalogEntry holds one query template from the stream JSON catalog.
type streamCatalogEntry struct {
	Request *streamv1.QueryRequest
	ID      string
}

// streamBaselineRecord is persisted after stream record-baseline runs. Elements
// holds proto-JSON encoded streamv1.Element messages.
type streamBaselineRecord struct {
	QueryName string            `json:"query_name"`
	Elements  []json.RawMessage `json:"elements"`
	Groups    []string          `json:"groups"`
	UntilMs   int64             `json:"until_ms"`
	Ordered   bool              `json:"ordered"`
}

// loadStreamCatalog reads the stream JSON catalog ({id, request} entries) and
// decodes each request via protojson so proto oneofs/enums round-trip.
func loadStreamCatalog(path string) ([]streamCatalogEntry, error) {
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, fmt.Errorf("read stream catalog %s: %w", path, readErr)
	}
	var rawEntries []rawCatalogEntry
	if unmarshalErr := json.Unmarshal(raw, &rawEntries); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshal stream catalog array: %w", unmarshalErr)
	}
	entries := make([]streamCatalogEntry, 0, len(rawEntries))
	for idx, rawEntry := range rawEntries {
		if rawEntry.ID == "" {
			return nil, fmt.Errorf("stream catalog entry %d: missing id", idx)
		}
		req := new(streamv1.QueryRequest)
		if protoErr := protojson.Unmarshal(rawEntry.Request, req); protoErr != nil {
			return nil, fmt.Errorf("unmarshal stream catalog entry %q: %w", rawEntry.ID, protoErr)
		}
		entries = append(entries, streamCatalogEntry{ID: rawEntry.ID, Request: req})
	}
	return entries, nil
}

// buildStreamQueryRequest injects a [until-window, until] TimeRange into a cloned
// catalog request. The pinned Limit and OrderBy are preserved verbatim.
func buildStreamQueryRequest(entry streamCatalogEntry, untilMs int64, window time.Duration) *streamv1.QueryRequest {
	untilTime := time.UnixMilli(untilMs)
	beginTime := untilTime.Add(-window)
	req, _ := proto.Clone(entry.Request).(*streamv1.QueryRequest)
	req.TimeRange = &modelv1.TimeRange{
		Begin: timestamppb.New(beginTime),
		End:   timestamppb.New(untilTime),
	}
	return req
}

// streamCatalogEntryOrdered reports whether the catalog query pins a result order.
func streamCatalogEntryOrdered(entry streamCatalogEntry) bool {
	return entry.Request.GetOrderBy().GetIndexRuleName() != ""
}

// streamStrTagValue wraps a string in a model TagValue.
func streamStrTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

// streamIntTagValue wraps an int64 in a model TagValue.
func streamIntTagValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}}
}

// streamServiceIDForIndex returns the per-series service_id. The first
// streamSvcZeroFraction of series are svc-0 so the svc-0 catalog filters select
// an exact, reproducible fraction.
func streamServiceIDForIndex(seriesIdx, seriesCount int) string {
	svcZeroCount := int(float64(seriesCount) * streamSvcZeroFraction)
	if seriesIdx < svcZeroCount {
		return "svc-0"
	}
	return fmt.Sprintf("svc-%d", seriesIdx)
}

// streamAlreadyExists reports whether err is an idempotent "already exists" error.
func streamAlreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exist")
}

// streamCreateGroup creates a CATALOG_STREAM group idempotently.
func streamCreateGroup(ctx context.Context, client databasev1.GroupRegistryServiceClient, name string, ttlDays uint32) error {
	_, createErr := client.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
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
	if createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream group %s: %w", name, createErr)
	}
	return nil
}

// streamCreateSchema creates the Stream resource, its duration index rule, and
// binding for one group, idempotently.
func streamCreateSchema(
	ctx context.Context,
	streamClient databasev1.StreamRegistryServiceClient,
	indexRuleClient databasev1.IndexRuleRegistryServiceClient,
	bindingClient databasev1.IndexRuleBindingRegistryServiceClient,
	group string,
) error {
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: streamName, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{streamTagServiceID}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: streamTagFamily,
			Tags: []*databasev1.TagSpec{
				{Name: streamTagServiceID, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: streamTagState, Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: streamTagDuration, Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}},
	}
	if _, createErr := streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: stream}); createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream resource in %s: %w", group, createErr)
	}

	// Type and Id both matter for an index-ORDER (sortable) stream index:
	//   - TYPE_INVERTED is the sortable local element index the stream index-order
	//     path queries via Index().Sort(). No stream index rule uses TYPE_TREE (see
	//     pkg/test/stream/testdata/index_rules) — a TYPE_TREE rule yields an index
	//     the sort path never populates, so the ordered query returns 0 rows.
	//   - Metadata.Id is the numeric IndexRuleID used as the index field key
	//     (query_by_idx.go builds index.FieldKey{IndexRuleID: rule.Metadata.Id}).
	//     Leaving it unset means id=0 and the ordered query finds no field.
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: streamIndexDurationID, Name: streamIndexDuration, Group: group},
		Tags:     []string{streamTagDuration},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	}
	if _, createErr := indexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule}); createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream index rule %s in %s: %w", rule.GetMetadata().GetName(), group, createErr)
	}

	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "sw-index-rule-binding", Group: group},
		Rules:    []string{streamIndexDuration},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: streamName},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	if _, createErr := bindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: binding}); createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream index rule binding in %s: %w", group, createErr)
	}
	return nil
}

// streamFixtureBaseTime anchors deterministic element timestamps to recent
// wall-clock time so they fall inside the group's TTL window.
func streamFixtureBaseTime(totalElements int) time.Time {
	const marginSec = 60
	return time.Now().UTC().Truncate(time.Millisecond).Add(-time.Duration(totalElements+marginSec) * time.Second)
}

// streamSeedFixture creates the two stream groups (long-TTL parity fixture +
// short-TTL rolling load), the "sw" Stream, the duration index rule and binding,
// then writes the deterministic fixture into the parity group and prints T1_MS.
func streamSeedFixture(ctx context.Context, conn *grpc.ClientConn, series, elementsPerSeries int) error {
	if series <= 0 {
		series = streamFixtureSeries
	}
	if elementsPerSeries <= 0 {
		elementsPerSeries = streamFixtureElementsPerSeri
	}
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	if groupErr := streamCreateGroup(ctx, groupClient, streamFixtureGroup, 30); groupErr != nil {
		return groupErr
	}
	if groupErr := streamCreateGroup(ctx, groupClient, streamLoadGroup, 1); groupErr != nil {
		return groupErr
	}

	streamClient := databasev1.NewStreamRegistryServiceClient(conn)
	indexRuleClient := databasev1.NewIndexRuleRegistryServiceClient(conn)
	bindingClient := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	for _, group := range []string{streamFixtureGroup, streamLoadGroup} {
		if schemaErr := streamCreateSchema(ctx, streamClient, indexRuleClient, bindingClient, group); schemaErr != nil {
			return schemaErr
		}
	}

	// The data node applies index rules through its schema watch. Elements written
	// BEFORE the duration rule + binding land carry no tree-index entries, which is
	// permanent for those elements — the index-order catalog query would then return
	// 0 rows forever, no matter how long the baseline waits. Settle before writing.
	fmt.Printf("[stream seed-fixture] waiting %s for the index rule + binding to reach the data node...\n", streamSchemaSettle)
	select {
	case <-time.After(streamSchemaSettle):
	case <-ctx.Done():
		return ctx.Err()
	}

	base := streamFixtureBaseTime(series * elementsPerSeries)
	highestMs, writeErr := streamWriteFixture(ctx, conn, streamFixtureGroup, series, elementsPerSeries, base, 0)
	if writeErr != nil {
		return writeErr
	}
	fmt.Printf("[stream seed-fixture] wrote %d series x %d elements to %s/%s\n", series, elementsPerSeries, streamFixtureGroup, streamName)

	// Poll until queryable (stream flush is async ~5s). Query svc-0 series over
	// the seed window until at least one element is visible.
	if visErr := streamWaitVisible(ctx, conn, base, highestMs); visErr != nil {
		return visErr
	}
	fmt.Printf("T1_MS=%d\n", highestMs)
	fmt.Printf("%d\n", highestMs)
	return nil
}

// streamElementID returns the deterministic element id for (series, element).
func streamElementID(serviceID string, elementIdx int) string {
	return fmt.Sprintf("%s-%06d", serviceID, elementIdx)
}

// streamWriteFixture writes series*elementsPerSeries deterministic elements into
// group via the streaming StreamService.Write RPC, checking every response
// status. Returns the highest element timestamp in ms.
func streamWriteFixture(ctx context.Context, conn *grpc.ClientConn, group string, series, elementsPerSeries int, base time.Time, versionBase int) (int64, error) {
	client := streamv1.NewStreamServiceClient(conn)
	stream, streamErr := client.Write(ctx)
	if streamErr != nil {
		return 0, fmt.Errorf("open stream write: %w", streamErr)
	}

	recvErrCh := make(chan error, 1)
	go func() {
		var firstBad string
		bad := 0
		for {
			resp, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				if bad > 0 {
					recvErrCh <- fmt.Errorf("stream write rejected %d elements (first status: %q)", bad, firstBad)
					return
				}
				recvErrCh <- nil
				return
			}
			if recvErr != nil {
				recvErrCh <- fmt.Errorf("receive stream write response: %w", recvErr)
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

	metadata := &commonv1.Metadata{Name: streamName, Group: group}
	requestMetadata := metadata
	var highestMs int64
	globalIdx := 0
	for seriesIdx := 0; seriesIdx < series; seriesIdx++ {
		serviceID := streamServiceIDForIndex(seriesIdx, series)
		for elementIdx := 0; elementIdx < elementsPerSeries; elementIdx++ {
			version := versionBase + globalIdx + 1
			ts := base.Add(time.Duration(version) * time.Second)
			tsMs := ts.UnixMilli()
			if tsMs > highestMs {
				highestMs = tsMs
			}
			state := int64(0)
			if globalIdx%20 == 0 {
				state = 1
			}
			duration := int64(1000 + seriesIdx*10 + elementIdx)
			req := &streamv1.WriteRequest{
				Metadata: requestMetadata,
				Element: &streamv1.ElementValue{
					ElementId: streamElementID(serviceID, version),
					Timestamp: timestamppb.New(ts),
					TagFamilies: []*modelv1.TagFamilyForWrite{{
						Tags: []*modelv1.TagValue{
							streamStrTagValue(serviceID),
							streamIntTagValue(state),
							streamIntTagValue(duration),
						},
					}},
				},
				MessageId: uint64(time.Now().UnixNano()) + uint64(globalIdx),
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return 0, fmt.Errorf("send stream write: %w", sendErr)
			}
			requestMetadata = nil
			globalIdx++
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return 0, fmt.Errorf("close stream write: %w", closeErr)
	}
	if recvErr := <-recvErrCh; recvErr != nil {
		return 0, recvErr
	}
	return highestMs, nil
}

// streamWaitVisible polls svc-0 until the seeded elements are queryable through
// BOTH shapes the catalog uses: a plain time-range query (data flushed) and an
// index-ORDER query keyed on the duration tree index (local element index
// flushed AND the index rule applied). Gating on the plain query alone is not
// enough: data becomes visible before the tree index does, so record-baseline
// would then see 0 rows for the index-order query and abort the run.
func streamWaitVisible(ctx context.Context, conn *grpc.ClientConn, base time.Time, highestMs int64) error {
	client := streamv1.NewStreamServiceClient(conn)
	timeRange := &modelv1.TimeRange{
		Begin: timestamppb.New(base.Add(-time.Second)),
		End:   timestamppb.New(time.UnixMilli(highestMs).Add(time.Second)),
	}
	plainReq := &streamv1.QueryRequest{
		Name:       streamName,
		Groups:     []string{streamFixtureGroup},
		TimeRange:  timeRange,
		Projection: streamProjection(),
		Limit:      100,
	}
	orderedReq := &streamv1.QueryRequest{
		Name:       streamName,
		Groups:     []string{streamFixtureGroup},
		TimeRange:  timeRange,
		Projection: streamProjection(),
		Limit:      100,
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: streamIndexDuration,
			Sort:          modelv1.Sort_SORT_DESC,
		},
	}
	count := func(req *streamv1.QueryRequest) int {
		qctx, qcancel := context.WithTimeout(ctx, 5*time.Second)
		defer qcancel()
		resp, qerr := client.Query(qctx, req)
		if qerr != nil || resp == nil {
			return 0
		}
		return len(resp.GetElements())
	}
	var plainSeen, orderedSeen int
	deadline := time.Now().Add(streamVisibleTimeout)
	for time.Now().Before(deadline) {
		plainSeen, orderedSeen = count(plainReq), count(orderedReq)
		if plainSeen > 0 && orderedSeen > 0 {
			fmt.Printf("[stream seed-fixture] %d elements visible (plain), %d visible via the %s index order\n",
				plainSeen, orderedSeen, streamIndexDuration)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	if plainSeen > 0 {
		return fmt.Errorf("stream seed-fixture: data is visible (%d elements) but the %s index-order query still returns 0 after %s — "+
			"the element (tree) index did not become queryable; the index rule/binding may not have been applied before the writes landed",
			plainSeen, streamIndexDuration, streamVisibleTimeout)
	}
	return fmt.Errorf("stream seed-fixture: no elements visible after %s — flush/index lag", streamVisibleTimeout)
}

// streamProjection returns the searchable tag projection every catalog query and
// the visibility poll share.
func streamProjection() *modelv1.TagProjection {
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{{
			Name: streamTagFamily,
			Tags: []string{streamTagServiceID, streamTagState, streamTagDuration},
		}},
	}
}

// streamRecordBaseline runs the catalog over [T1-window, T1], persists the
// elements as proto-JSON, and exits non-zero if any catalog query is empty.
func streamRecordBaseline(ctx context.Context, conn *grpc.ClientConn, catalogPath, outPath string, untilMs int64, window time.Duration) error {
	entries, loadErr := loadStreamCatalog(catalogPath)
	if loadErr != nil {
		return loadErr
	}
	client := streamv1.NewStreamServiceClient(conn)
	records := make([]streamBaselineRecord, 0, len(entries))
	var emptyQueries []string
	for _, entry := range entries {
		req := buildStreamQueryRequest(entry, untilMs, window)
		queryName := entry.ID
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, queryErr := client.Query(queryCtx, req)
		cancel()
		if queryErr != nil {
			return fmt.Errorf("record-baseline %s: query failed: %w", queryName, queryErr)
		}
		rec := streamBaselineRecord{
			QueryName: queryName,
			Groups:    entry.Request.GetGroups(),
			UntilMs:   untilMs,
			Ordered:   streamCatalogEntryOrdered(entry),
		}
		for _, element := range resp.GetElements() {
			rawElement, marshalErr := protojson.Marshal(element)
			if marshalErr != nil {
				return fmt.Errorf("marshal element for %s: %w", queryName, marshalErr)
			}
			rec.Elements = append(rec.Elements, json.RawMessage(rawElement))
		}
		records = append(records, rec)
		if len(rec.Elements) == 0 {
			emptyQueries = append(emptyQueries, queryName)
		}
		fmt.Printf("[stream record-baseline] %s: %d elements\n", queryName, len(rec.Elements))
	}

	out, createErr := os.Create(outPath)
	if createErr != nil {
		return fmt.Errorf("create output %s: %w", outPath, createErr)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(records); encErr != nil {
		return fmt.Errorf("encode stream baseline: %w", encErr)
	}
	fmt.Printf("[stream record-baseline] written to %s\n", outPath)
	if len(emptyQueries) > 0 {
		return fmt.Errorf("record-baseline: %d catalog queries returned no elements: %s", len(emptyQueries), strings.Join(emptyQueries, ", "))
	}
	return nil
}

// streamReplayAndDiff re-runs the catalog against the baseline and writes a
// diffReport (reusing the shared shape so soak-monitor.sh grep matches). Returns
// a non-zero error on any divergence.
func streamReplayAndDiff(ctx context.Context, conn *grpc.ClientConn, catalogPath, baselinePath, reportPath string, window time.Duration) error {
	raw, readErr := os.ReadFile(baselinePath)
	if readErr != nil {
		return fmt.Errorf("read stream baseline %s: %w", baselinePath, readErr)
	}
	var records []streamBaselineRecord
	if unmarshalErr := json.Unmarshal(raw, &records); unmarshalErr != nil {
		return fmt.Errorf("unmarshal stream baseline: %w", unmarshalErr)
	}
	entries, loadErr := loadStreamCatalog(catalogPath)
	if loadErr != nil {
		return loadErr
	}
	client := streamv1.NewStreamServiceClient(conn)
	report := diffReport{RunAt: time.Now().UTC().Format(time.RFC3339), Pass: true}
	baselineMap := make(map[string]streamBaselineRecord, len(records))
	for _, rec := range records {
		baselineMap[rec.QueryName] = rec
	}

	for _, entry := range entries {
		queryName := entry.ID
		rec, ok := baselineMap[queryName]
		if !ok {
			fmt.Printf("[stream replay-and-diff] %s: SKIP (no baseline)\n", queryName)
			continue
		}
		req := buildStreamQueryRequest(entry, rec.UntilMs, window)
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, queryErr := client.Query(queryCtx, req)
		cancel()
		if queryErr != nil {
			report.Divergences = append(report.Divergences, divergence{QueryName: queryName})
			report.Pass = false
			fmt.Printf("[stream replay-and-diff] %s: FAIL (%v)\n", queryName, queryErr)
			continue
		}
		report.QueriesRun++

		baselineElements, parseErr := decodeBaselineElements(rec.Elements, queryName)
		if parseErr != nil {
			return parseErr
		}
		div, matched := compareStreamResults(queryName, baselineElements, resp.GetElements(), rec.Ordered)
		if !matched {
			report.Divergences = append(report.Divergences, div)
			report.Pass = false
		}
	}

	outFile, createErr := os.Create(reportPath)
	if createErr != nil {
		return fmt.Errorf("create stream report %s: %w", reportPath, createErr)
	}
	defer outFile.Close()
	enc := json.NewEncoder(outFile)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(report); encErr != nil {
		return fmt.Errorf("encode stream report: %w", encErr)
	}
	fmt.Printf("[stream replay-and-diff] %d queries run, %d divergences — %s\n",
		report.QueriesRun, len(report.Divergences), map[bool]string{true: "PASS", false: "FAIL"}[report.Pass])
	if !report.Pass {
		return fmt.Errorf("parity check failed: %d divergences found", len(report.Divergences))
	}
	return nil
}

// decodeBaselineElements decodes the proto-JSON element blobs from a baseline record.
func decodeBaselineElements(raws []json.RawMessage, queryName string) ([]*streamv1.Element, error) {
	elements := make([]*streamv1.Element, 0, len(raws))
	for idx, raw := range raws {
		element := new(streamv1.Element)
		if parseErr := protojson.Unmarshal(raw, element); parseErr != nil {
			return nil, fmt.Errorf("unmarshal baseline element %d for %s: %w", idx, queryName, parseErr)
		}
		elements = append(elements, element)
	}
	return elements, nil
}

// compareStreamResults diffs baseline vs replay elements. Unordered queries sort
// by element_id before comparison; ordered queries preserve position. Returns a
// divergence and false on the first mismatch.
func compareStreamResults(queryName string, baseline, replay []*streamv1.Element, ordered bool) (divergence, bool) {
	div := divergence{QueryName: queryName, BaselineLen: len(baseline), ReplayLen: len(replay)}
	if len(baseline) != len(replay) {
		return div, false
	}
	baselineNorm := normalizeElements(baseline, ordered)
	replayNorm := normalizeElements(replay, ordered)
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

// normalizeElements returns clones; when not ordered, sorts by element_id so set
// comparison is stable.
func normalizeElements(elements []*streamv1.Element, ordered bool) []*streamv1.Element {
	clones := make([]*streamv1.Element, 0, len(elements))
	for _, element := range elements {
		clone, _ := proto.Clone(element).(*streamv1.Element)
		clones = append(clones, clone)
	}
	if !ordered {
		sort.Slice(clones, func(left, right int) bool {
			return clones[left].GetElementId() < clones[right].GetElementId()
		})
	}
	return clones
}

// streamWriteLoad runs continuous deterministic writes into the rolling load
// group, outside the parity window, rate-capped at rps. Returns elements written.
func streamWriteLoad(ctx context.Context, conn *grpc.ClientConn, series, elementsPerSeries, rps int, duration time.Duration) (int, error) {
	if series <= 0 {
		series = streamFixtureSeries
	}
	if elementsPerSeries <= 0 {
		elementsPerSeries = streamFixtureElementsPerSeri
	}
	if rps <= 0 {
		rps = 1000
	}
	perSweep := series * elementsPerSeries
	base := streamFixtureBaseTime(perSweep)
	deadline := time.Now().Add(duration)
	total := 0
	versionBase := 0
	interval := time.Second / time.Duration(rps)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}
		sweepStart := time.Now()
		if _, writeErr := streamWriteFixture(ctx, conn, streamLoadGroup, series, elementsPerSeries, base, versionBase); writeErr != nil {
			return total, writeErr
		}
		total += perSweep
		versionBase += perSweep
		elapsed := time.Since(sweepStart)
		budget := time.Duration(perSweep) * interval
		if elapsed < budget {
			time.Sleep(budget - elapsed)
		}
		fmt.Printf("[stream write-load] sweep done: %d elements total\n", total)
	}
	return total, nil
}
