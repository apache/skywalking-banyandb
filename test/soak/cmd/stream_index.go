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
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

// Log-shaped stream fixture for the native inverted-index soak.
//
// The generic stream fixture (stream.go) is 8 series x 40 elements — a unit
// test, not a soak. This fixture models a real log stream: a skewed level
// distribution, a per-element trace id (point lookups), log-normal durations
// (numeric-trie range scans) and templated messages (shared terms across
// documents, so the postings lists are actually shared rather than 1-element
// singletons).
//
// Groups are deliberately named differently from the generic stream fixture
// (streamFixtureGroup/streamLoadGroup) so a pilot run never collides with an
// in-flight stream soak on a shared host: a persisted schema from one run
// would otherwise make the other run's registry Create return AlreadyExists
// and silently keep the stale schema.
const (
	streamIdxFixtureGroup = "soak-idx-fixture"
	streamIdxLoadGroup    = "soak-idx-load"

	streamIdxTagFamilySearch = "searchable"
	streamIdxTagFamilyMetric = "metrics"

	streamIdxTagServiceID = "service_id"
	streamIdxTagPod       = "pod"
	streamIdxTagLevel     = "level"
	streamIdxTagTraceID   = "trace_id"
	streamIdxTagMessage   = "message"
	streamIdxTagDuration  = "duration"
	streamIdxTagStatus    = "status_code"
	streamIdxTagByteSize  = "byte_size"

	// streamIdxSortRule is the index rule the SORT_* catalog queries pin in
	// orderBy.indexRuleName. Only duration carries a sort rule (no_sort=false);
	// everything else sets no_sort=true so the encoder skips its doc-value
	// column (nativeadapter/adapter.go:38 sets Field.Sort = !NoSort).
	streamIdxSortRule = "ir_duration"

	streamIdxDefaultSeries   = 200
	streamIdxDefaultElements = 200
	streamIdxFixtureSeed     = 20261012
	streamIdxFixtureSpread   = 24 * time.Hour
	streamIdxTrailingMargin  = 60
	// streamIdxDefaultChunks is the number of seed write batches, and therefore
	// roughly the number of segments the fixture produces. 96 gives one slice
	// per 15 minutes of the 24h window, so a 15-minute query window always has
	// neighbors it can be classified DISJOINT from — without this the whole
	// fixture lands in one segment and the time pruner can never skip anything.
	streamIdxDefaultChunks = 96

	// streamIdxVisibleProbeWindow bounds the seed-fixture visibility poll.
	streamIdxVisibleProbeWindow = time.Minute
	streamIdxSchemaSettle       = 8 * time.Second
	streamIdxVisibleTimeout     = 300 * time.Second
)

// logLevel weights model a real service's log volume: mostly INFO, a tail of
// ERROR. The skew is the point — it produces both very large postings (INFO)
// and small ones (ERROR), which is the crossover the posting-decode path has
// to get right.
type logLevel struct {
	name   string
	weight float64
}

var streamIdxLevels = []logLevel{
	{"INFO", 0.80},
	{"WARN", 0.15},
	{"ERROR", 0.05},
}

var streamIdxStatusCodes = []struct {
	code   int64
	weight float64
}{
	{200, 0.88},
	{404, 0.07},
	{500, 0.05},
}

// streamIdxMessageVocabulary is shared across documents on purpose: every term
// below is posted by many elements, so the dictionary accumulates a real
// posting list per term instead of the 1-element posting that unique values
// would produce.
var streamIdxMessageVocabulary = []string{
	"connection", "reset", "pool", "exhausted", "cache", "miss",
	"timeout", "upstream", "downstream", "handshake", "retry", "backoff",
	"checkpoint", "flushed", "lag", "replication", "election", "quorum",
}

// streamIdxIndexRuleDef describes one IndexRule of the fixture. Id must be
// non-zero: it is the numeric IndexRuleID the element index uses as its field
// key, so a zero id makes the field unfindable.
type streamIdxIndexRuleDef struct {
	name     string
	analyzer string
	tags     []string
	id       uint32
	noSort   bool
}

var streamIdxIndexRules = []streamIdxIndexRuleDef{
	{name: "ir_level", tags: []string{streamIdxTagLevel}, analyzer: "keyword", id: 10, noSort: true},
	{name: "ir_pod", tags: []string{streamIdxTagPod}, analyzer: "keyword", id: 11, noSort: true},
	{name: "ir_trace_id", tags: []string{streamIdxTagTraceID}, analyzer: "keyword", id: 12, noSort: true},
	// keyword, NOT "simple": an analyzed element-index field is currently
	// unqueryable on the native path (write tokenizes, read cannot reproduce the
	// terms), and BINARY_OP_MATCH returns no rows on any field. See
	// docs/soak/stream-index-findings.md.
	{name: "ir_message", tags: []string{streamIdxTagMessage}, analyzer: "keyword", id: 13, noSort: true},
	{name: "ir_duration", tags: []string{streamIdxTagDuration}, analyzer: "keyword", id: 14},
	{name: "ir_status_code", tags: []string{streamIdxTagStatus}, analyzer: "keyword", id: 15, noSort: true},
}

// streamIdxServiceID returns the service for a series index. Two naming
// families (svc-/api-) keep the dictionary from degenerating into one long
// shared prefix, which would make prefix expansion unrepresentative.
func streamIdxServiceID(seriesIdx int) string {
	if seriesIdx%2 == 0 {
		return fmt.Sprintf("svc-%04d", seriesIdx)
	}
	return fmt.Sprintf("api-%04d", seriesIdx)
}

// streamIdxPodForSeries returns a stable pod for the series. Ten pods per
// service keeps series cardinality and pod cardinality independent.
func streamIdxPodForSeries(seriesIdx int) string {
	return fmt.Sprintf("pod-%03d", seriesIdx%10)
}

// streamIdxPickLevel draws a level from the skewed distribution.
func streamIdxPickLevel(rnd *rand.Rand) string {
	pick := rnd.Float64()
	for _, lvl := range streamIdxLevels {
		if pick < lvl.weight {
			return lvl.name
		}
		pick -= lvl.weight
	}
	return streamIdxLevels[len(streamIdxLevels)-1].name
}

// streamIdxPickStatusCode draws a status code from the skewed distribution.
func streamIdxPickStatusCode(rnd *rand.Rand) int64 {
	pick := rnd.Float64()
	for _, sc := range streamIdxStatusCodes {
		if pick < sc.weight {
			return sc.code
		}
		pick -= sc.weight
	}
	return streamIdxStatusCodes[len(streamIdxStatusCodes)-1].code
}

// streamIdxDuration draws a log-normal-ish duration in milliseconds. A heavy
// right tail is what makes the numeric trie interesting: a range query over
// duration selects few candidates out of many, which is the case
// time_prune_bench_test.go's BenchmarkPointLookupCrossover sweeps.
func streamIdxDuration(rnd *rand.Rand) int64 {
	// Box-Muller, then exp. mu=5.5/sigma=1.1 gives median e^5.5 ≈ 245 ms, so
	// P(duration>1500) ≈ 5% and P(duration>=1000) ≈ 10% — the catalog's
	// numeric-range and sort+filter thresholds land on real percentiles rather
	// than on the far tail. (mu=3 put >1500ms at ~4e-5, i.e. zero rows at any
	// fixture size the pilot can write.)
	u1 := rnd.Float64()
	if u1 < 1e-9 {
		u1 = 1e-9
	}
	u2 := rnd.Float64()
	gauss := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	dur := int64(math.Exp(5.5 + 1.1*gauss))
	if dur < 1 {
		dur = 1
	}
	return dur
}

// streamIdxMessage builds a templated message from the shared vocabulary.
//
// Deliberately NO random component: the vocabulary is 16 words, so the message
// takes 16*16 = 256 distinct values and every one of them is posted by ~1/256
// of the fixture. That gives the catalog an addressable term with a large,
// genuinely shared posting — the structure a real log-message secondary index
// has. (An earlier revision interpolated the duration, which made every message
// unique and left the field with only 1-element postings.)
func streamIdxMessage(verbIdx, targetIdx int) string {
	vocab := streamIdxMessageVocabulary
	return fmt.Sprintf("%s to %s", vocab[verbIdx%len(vocab)], vocab[targetIdx%len(vocab)])
}

// streamIdxCreateSchema creates the Stream, its index rules and binding for one
// group, idempotently. Mirrors streamCreateSchema but with the log-shaped
// schema and multiple rules.
func streamIdxCreateSchema(
	ctx context.Context,
	streamClient databasev1.StreamRegistryServiceClient,
	indexRuleClient databasev1.IndexRuleRegistryServiceClient,
	bindingClient databasev1.IndexRuleBindingRegistryServiceClient,
	group string,
) error {
	// The entity is service_id alone. Keeping the entity narrow means every
	// other tag is filtered through the ELEMENT index (the secondary index
	// under test), while a service_id criterion resolves through the SERIES
	// index — which is exactly the two-index split the soak is meant to
	// observe separately.
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: streamName, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{streamIdxTagServiceID}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: streamIdxTagFamilySearch,
				Tags: []*databasev1.TagSpec{
					{Name: streamIdxTagServiceID, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamIdxTagPod, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamIdxTagLevel, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamIdxTagTraceID, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamIdxTagMessage, Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
			{
				Name: streamIdxTagFamilyMetric,
				Tags: []*databasev1.TagSpec{
					{Name: streamIdxTagDuration, Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: streamIdxTagStatus, Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: streamIdxTagByteSize, Type: databasev1.TagType_TAG_TYPE_INT},
				},
			},
		},
	}
	if _, createErr := streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: stream}); createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream resource in %s: %w", group, createErr)
	}

	ruleNames := make([]string, 0, len(streamIdxIndexRules))
	for _, def := range streamIdxIndexRules {
		rule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Id: def.id, Name: def.name, Group: group},
			Tags:     def.tags,
			// TYPE_INVERTED only. A TYPE_TREE rule yields an index the sort
			// path never populates, so an ordered query against it returns 0
			// rows (see streamCreateSchema).
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: def.analyzer,
			NoSort:   def.noSort,
		}
		req := &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule}
		if _, createErr := indexRuleClient.Create(ctx, req); createErr != nil && !streamAlreadyExists(createErr) {
			return fmt.Errorf("create index rule %s in %s: %w", def.name, group, createErr)
		}
		ruleNames = append(ruleNames, def.name)
	}

	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "sw-idx-rule-binding", Group: group},
		Rules:    ruleNames,
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: streamName},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	bindingReq := &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: binding}
	if _, createErr := bindingClient.Create(ctx, bindingReq); createErr != nil && !streamAlreadyExists(createErr) {
		return fmt.Errorf("create stream index rule binding in %s: %w", group, createErr)
	}
	return nil
}

// streamIdxBaseTime anchors the fixture so its elements end just inside the
// now-based margin, keeping every element within the group's TTL window.
func streamIdxBaseTime() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond).Add(-streamIdxFixtureSpread - streamIdxTrailingMargin*time.Second)
}

// streamIdxElementTimestamp spreads element globalIdx across the fixture
// window. Spreading (rather than 1s-per-element as the generic fixture does) is
// what produces several time ranges a query can be disjoint from, which is the
// class="disjoint" branch of the native time pruner.
func streamIdxElementTimestamp(base time.Time, globalIdx, totalElements int) time.Time {
	if totalElements <= 1 {
		return base
	}
	offsetMs := int64(globalIdx) * int64(streamIdxFixtureSpread/time.Millisecond) / int64(totalElements)
	return base.Add(time.Duration(offsetMs) * time.Millisecond)
}

// streamIdxWriteFixture writes series*elementsPerSeries log-shaped elements into
// group, split across `chunks` Write RPCs. Returns the highest element timestamp
// in ms.
//
// WHY THE SPLIT: the native engine derives a segment's timeMin/timeMax from the
// documents it holds (owner.go, newMemorySegment), and a segment is cut per
// write batch. Writing the whole fixture down ONE stream therefore produces a
// single segment spanning the entire 24 h window, and a query window can then
// only ever classify that segment as contained or overlap — `disjoint` becomes
// structurally unreachable, and the native time pruner never gets to skip
// anything. Emitting one batch per contiguous slice of the timeline instead
// yields segments with narrow, non-overlapping bounds, which is what a narrow
// query window needs in order to prune.
//
// The element order is unchanged, so the fixture's generated values are
// bit-identical to the single-stream version for the same seed.
func streamIdxWriteFixture(
	ctx context.Context,
	conn *grpc.ClientConn,
	group string,
	series, elementsPerSeries, chunks int,
	base time.Time,
	versionBase int,
) (int64, error) {
	total := series * elementsPerSeries
	if total <= 0 {
		return 0, nil
	}
	chunkElements := streamIdxChunkSize(total, chunks)
	// math/rand is deliberate and not a lint oversight: the fixture MUST be
	// reproducible, because replay-and-diff compares element sets and any
	// nondeterminism here surfaces as a phantom divergence. crypto/rand would
	// make every soak run incomparable to its own baseline.
	//nolint:gosec // G404: reproducible fixture data, not cryptography.
	rnd := rand.New(rand.NewSource(streamIdxFixtureSeed + int64(versionBase)))
	var highestMs int64
	for start := 0; start < total; start += chunkElements {
		end := start + chunkElements
		if end > total {
			end = total
		}
		chunkHighest, err := streamIdxWriteChunk(ctx, conn, group, series, elementsPerSeries, start, end, total, base, versionBase, rnd)
		if err != nil {
			return 0, err
		}
		if chunkHighest > highestMs {
			highestMs = chunkHighest
		}
	}
	return highestMs, nil
}

// streamIdxChunkSize divides total into `chunks` contiguous slices, never
// dropping below one element.
func streamIdxChunkSize(total, chunks int) int {
	if chunks <= 1 || total <= 1 {
		return total
	}
	if chunks > total {
		chunks = total
	}
	size := (total + chunks - 1) / chunks
	if size < 1 {
		size = 1
	}
	return size
}

// streamIdxWriteChunk sends globalIdx in [start, end) as ONE Write RPC, so the
// engine admits it as its own batch and cuts its own segment. `rnd` is threaded
// through from the caller so the generated values keep their sequence across
// chunk boundaries.
func streamIdxWriteChunk(
	ctx context.Context,
	conn *grpc.ClientConn,
	group string,
	series, elementsPerSeries, start, end, total int,
	base time.Time,
	versionBase int,
	rnd *rand.Rand,
) (int64, error) {
	client := streamv1.NewStreamServiceClient(conn)
	stream, streamErr := client.Write(ctx)
	if streamErr != nil {
		return 0, fmt.Errorf("open stream write: %w", streamErr)
	}
	// Shared, checked drain. Per-record rejections are not transport errors —
	// they surface as a non-SUCCEED status and vanish if the response stream is
	// discarded (see test/CLAUDE.md).
	recvErrCh := streamIdxStartDrain(stream)

	metadata := &commonv1.Metadata{Name: streamName, Group: group}
	requestMetadata := metadata
	var highestMs int64
	for globalIdx := start; globalIdx < end; globalIdx++ {
		seriesIdx := globalIdx / elementsPerSeries
		if seriesIdx >= series {
			break
		}
		version := versionBase + globalIdx + 1
		ts := streamIdxElementTimestamp(base, globalIdx, total)
		tsMs := ts.UnixMilli()
		if tsMs > highestMs {
			highestMs = tsMs
		}
		level := streamIdxPickLevel(rnd)
		status := streamIdxPickStatusCode(rnd)
		duration := streamIdxDuration(rnd)
		// The trace id is unique per element, so filter_trace_point
		// resolves to exactly one element and stays a stable parity anchor.
		traceID := fmt.Sprintf("trace-%08d-%06d", version, seriesIdx%1000)
		message := streamIdxMessage(rnd.Intn(len(streamIdxMessageVocabulary)), rnd.Intn(len(streamIdxMessageVocabulary)))
		byteSize := int64(256 + rnd.Intn(4096))

		req := &streamv1.WriteRequest{
			Metadata: requestMetadata,
			Element: &streamv1.ElementValue{
				ElementId: fmt.Sprintf("%s-%08d", streamIdxServiceID(seriesIdx), version),
				Timestamp: timestamppb.New(ts),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							streamStrTagValue(streamIdxServiceID(seriesIdx)),
							streamStrTagValue(streamIdxPodForSeries(seriesIdx)),
							streamStrTagValue(level),
							streamStrTagValue(traceID),
							streamStrTagValue(message),
						},
					},
					{
						Tags: []*modelv1.TagValue{
							streamIntTagValue(duration),
							streamIntTagValue(status),
							streamIntTagValue(byteSize),
						},
					},
				},
			},
			MessageId: uint64(time.Now().UnixNano()) + uint64(globalIdx),
		}
		if sendErr := stream.Send(req); sendErr != nil {
			return 0, fmt.Errorf("send stream write: %w", sendErr)
		}
		requestMetadata = nil
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return 0, fmt.Errorf("close stream write: %w", closeErr)
	}
	if recvErr := <-recvErrCh; recvErr != nil {
		return 0, recvErr
	}
	return highestMs, nil
}

// streamIdxStartDrain consumes the response stream, checking every status and
// reporting the first non-SUCCEED status alongside the rejection count.
func streamIdxStartDrain(stream streamv1.StreamService_WriteClient) <-chan error {
	recvErrCh := make(chan error, 1)
	go func() {
		var firstBad string
		bad := 0
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					break
				}
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
		if bad > 0 {
			recvErrCh <- fmt.Errorf("stream write rejected %d elements (first status: %q)", bad, firstBad)
			return
		}
		recvErrCh <- nil
	}()
	return recvErrCh
}

// streamIdxProjection projects both tag families.
func streamIdxProjection() *modelv1.TagProjection {
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{
				Name: streamIdxTagFamilySearch,
				Tags: []string{streamIdxTagServiceID, streamIdxTagPod, streamIdxTagLevel, streamIdxTagTraceID, streamIdxTagMessage},
			},
			{
				Name: streamIdxTagFamilyMetric,
				Tags: []string{streamIdxTagDuration, streamIdxTagStatus, streamIdxTagByteSize},
			},
		},
	}
}

// streamIdxWaitVisible polls until BOTH a plain time-range query and an
// index-ORDER query return rows. Gating on the plain query alone is not
// enough: data becomes visible before the element index does, and a baseline
// recorded in that window silently captures 0 rows for every ordered query.
func streamIdxWaitVisible(ctx context.Context, conn *grpc.ClientConn, base time.Time, highestMs int64) error {
	client := streamv1.NewStreamServiceClient(conn)
	_ = base
	// Poll with a NARROW window, never a full-window scan. At 500k elements an
	// unfiltered 24h scan reserves more than the whole query budget
	// (protector limit / 4) and is rejected with ErrQueryResourceExhausted, so
	// seed-fixture used to sit until its visibility timeout while every poll
	// failed for a reason that had nothing to do with the data being visible.
	//
	// The window is one minute of a 24h fixture — a few hundred elements at any
	// scale the pilot uses. Deliberately NOT also filtered on service_id: the
	// chunks slice globalIdx and seriesIdx is globalIdx/elementsPerSeries, so
	// the final chunk holds only the LAST few series. Pinning a specific series
	// therefore finds nothing in the tail of the fixture.
	visibleTimeRange := &modelv1.TimeRange{
		Begin: timestamppb.New(time.UnixMilli(highestMs).Add(-streamIdxVisibleProbeWindow)),
		End:   timestamppb.New(time.UnixMilli(highestMs).Add(time.Second)),
	}
	plainReq := &streamv1.QueryRequest{
		Name:       streamName,
		Groups:     []string{streamIdxFixtureGroup},
		TimeRange:  visibleTimeRange,
		Projection: streamIdxProjection(),
		Limit:      100,
	}
	orderedReq := &streamv1.QueryRequest{
		Name:       streamName,
		Groups:     []string{streamIdxFixtureGroup},
		TimeRange:  visibleTimeRange,
		Projection: streamIdxProjection(),
		Limit:      100,
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: streamIdxSortRule,
			Sort:          modelv1.Sort_SORT_DESC,
		},
	}
	count := func(req *streamv1.QueryRequest) int {
		qctx, qcancel := context.WithTimeout(ctx, 10*time.Second)
		defer qcancel()
		resp, qerr := client.Query(qctx, req)
		if qerr != nil || resp == nil {
			return 0
		}
		return len(resp.GetElements())
	}
	var plainSeen, orderedSeen int
	deadline := time.Now().Add(streamIdxVisibleTimeout)
	for time.Now().Before(deadline) {
		plainSeen, orderedSeen = count(plainReq), count(orderedReq)
		if plainSeen > 0 && orderedSeen > 0 {
			fmt.Printf("[stream-index seed-fixture] %d elements visible (plain), %d via the %s index order\n",
				plainSeen, orderedSeen, streamIdxSortRule)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	if plainSeen > 0 {
		return fmt.Errorf("stream-index seed-fixture: data is visible (%d elements) but the %s index-order query still returns 0 after %s — "+
			"the element index did not become queryable; the index rules/binding may not have applied before the writes landed",
			plainSeen, streamIdxSortRule, streamIdxVisibleTimeout)
	}
	return fmt.Errorf("stream-index seed-fixture: no elements visible after %s — flush/index lag", streamIdxVisibleTimeout)
}

// streamIdxSeedFixture creates both groups, the log-shaped schema, writes the
// deterministic fixture into the parity group and prints T1_MS.
func streamIdxSeedFixture(ctx context.Context, conn *grpc.ClientConn, series, elementsPerSeries, chunks int) error {
	if series <= 0 {
		series = streamIdxDefaultSeries
	}
	if elementsPerSeries <= 0 {
		elementsPerSeries = streamIdxDefaultElements
	}
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	if groupErr := streamCreateGroup(ctx, groupClient, streamIdxFixtureGroup, 30); groupErr != nil {
		return groupErr
	}
	if groupErr := streamCreateGroup(ctx, groupClient, streamIdxLoadGroup, 1); groupErr != nil {
		return groupErr
	}

	streamClient := databasev1.NewStreamRegistryServiceClient(conn)
	indexRuleClient := databasev1.NewIndexRuleRegistryServiceClient(conn)
	bindingClient := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	for _, group := range []string{streamIdxFixtureGroup, streamIdxLoadGroup} {
		if schemaErr := streamIdxCreateSchema(ctx, streamClient, indexRuleClient, bindingClient, group); schemaErr != nil {
			return schemaErr
		}
	}

	// Elements written BEFORE the index rules + binding land carry no index
	// entries, permanently — no amount of later waiting repairs it.
	fmt.Printf("[stream-index seed-fixture] waiting %s for %d index rules + binding to reach the data node...\n",
		streamIdxSchemaSettle, len(streamIdxIndexRules))
	select {
	case <-time.After(streamIdxSchemaSettle):
	case <-ctx.Done():
		return ctx.Err()
	}

	base := streamIdxBaseTime()
	total := series * elementsPerSeries
	chunkElements := streamIdxChunkSize(total, chunks)
	fmt.Printf("[stream-index seed-fixture] writing %d elements (%d series x %d) over %s in %d chunks of ~%d\n",
		total, series, elementsPerSeries, streamIdxFixtureSpread, chunks, chunkElements)
	writeStart := time.Now()
	highestMs, writeErr := streamIdxWriteFixture(ctx, conn, streamIdxFixtureGroup, series, elementsPerSeries, chunks, base, 0)
	if writeErr != nil {
		return writeErr
	}
	fmt.Printf("[stream-index seed-fixture] wrote %d elements in %s\n", total, time.Since(writeStart).Round(time.Millisecond))

	if visErr := streamIdxWaitVisible(ctx, conn, base, highestMs); visErr != nil {
		return visErr
	}
	fmt.Printf("T1_MS=%d\n", highestMs)
	fmt.Printf("%d\n", highestMs)
	return nil
}

// streamIdxWriteLoad runs continuous deterministic log-shaped writes into the
// rolling load group, rate-capped at rps. Returns elements written.
func streamIdxWriteLoad(ctx context.Context, conn *grpc.ClientConn, series, elementsPerSeries, chunks, rps int, duration time.Duration) (int, error) {
	if series <= 0 {
		series = streamIdxDefaultSeries
	}
	if elementsPerSeries <= 0 {
		elementsPerSeries = streamIdxDefaultElements
	}
	if rps <= 0 {
		rps = 1000
	}
	perSweep := series * elementsPerSeries
	base := streamIdxBaseTime()
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
		if _, writeErr := streamIdxWriteFixture(ctx, conn, streamIdxLoadGroup, series, elementsPerSeries, chunks, base, versionBase); writeErr != nil {
			return total, writeErr
		}
		total += perSweep
		versionBase += perSweep
		elapsed := time.Since(sweepStart)
		budget := time.Duration(perSweep) * interval
		if elapsed < budget {
			time.Sleep(budget - elapsed)
		}
		fmt.Printf("[stream-index write-load] sweep done: %d elements total\n", total)
	}
	return total, nil
}
