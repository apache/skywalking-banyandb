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
	"math"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

func TestPercentileNearestRank(t *testing.T) {
	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	// Nearest-rank: sorted[ceil(p/100*n)-1]. For n=10 that makes p95 and p99
	// both the maximum, NOT the 9th value — the earlier floor((n-1)*p/100)
	// formula dropped the slowest observation at high percentiles.
	cases := []struct {
		p    float64
		want float64
	}{
		{0, 1},
		{50, 5},
		{90, 9},
		{95, 10},
		{99, 10},
		{100, 10},
	}
	for _, tc := range cases {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%v, %v) = %v, want %v", sorted, tc.p, got, tc.want)
		}
	}
	if got := percentile(nil, 99); got != 0 {
		t.Errorf("percentile of an empty slice = %v, want 0", got)
	}
}

// The tail must not be dropped: with a small sample the top percentile has to
// reach the slowest observation, which is the whole point of a soak probe.
func TestPercentileReachesTheTail(t *testing.T) {
	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	if got := percentile(sorted, 99); got != 8 {
		t.Errorf("percentile(p99) over 8 samples = %v, want the maximum 8", got)
	}
	if got := percentile(sorted, 50); got != 4 {
		t.Errorf("percentile(p50) over 8 samples = %v, want 4", got)
	}
	single := []float64{42}
	if got := percentile(single, 99); got != 42 {
		t.Errorf("percentile over a single sample = %v, want 42", got)
	}
}

func TestSummariseReportsRowsAndLatency(t *testing.T) {
	sample := summarize("q1", timeZero, []float64{10, 20, 30, 40}, []int{2, 0, 5, 2}, 0, "")
	if sample.QueryID != "q1" {
		t.Errorf("QueryID = %q, want q1", sample.QueryID)
	}
	if sample.Calls != 4 {
		t.Errorf("Calls = %d, want 4", sample.Calls)
	}
	if sample.RowsMin != 0 || sample.RowsMax != 5 {
		t.Errorf("row bounds = [%d, %d], want [0, 5]", sample.RowsMin, sample.RowsMax)
	}
	if sample.Rows != 2 {
		t.Errorf("Rows (mean) = %d, want 2", sample.Rows)
	}
	if sample.P50Ms != 20 || sample.MaxMs != 40 {
		t.Errorf("p50=%v max=%v, want 20/40", sample.P50Ms, sample.MaxMs)
	}
	if sample.MeanMs != 25 {
		t.Errorf("MeanMs = %v, want 25", sample.MeanMs)
	}
}

// A probe window where every query errored must not look healthy: summarize
// still reports the error count and first error so the gate can act on it.
func TestSummariseSurfacesErrors(t *testing.T) {
	sample := summarize("q2", timeZero, nil, nil, 3, "boom")
	if sample.Errors != 3 || sample.FirstErr != "boom" {
		t.Errorf("errors = %d/%q, want 3/boom", sample.Errors, sample.FirstErr)
	}
	if sample.Calls != 0 {
		t.Errorf("Calls = %d, want 0 when nothing succeeded", sample.Calls)
	}
}

// The log-shaped fixture must be deterministic: the same seed has to produce
// byte-identical tag values, because parity (replay-and-diff) compares element
// sets and any nondeterminism here shows up as a phantom divergence.
func TestStreamIdxFixtureIsDeterministic(t *testing.T) {
	first := collectStreamIdxTags(1000, 7)
	second := collectStreamIdxTags(1000, 7)
	if len(first) != 1000 {
		t.Fatalf("collected %d series, want 1000", len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("series %d differs between runs:\n %+v\n %+v", i, first[i], second[i])
		}
	}
}

// Different series must not collapse to the same service id, or the series
// index would hold far fewer series than the fixture claims.
func TestStreamIdxServiceIDsAreDistinct(t *testing.T) {
	seen := make(map[string]int, 200)
	for idx := 0; idx < 200; idx++ {
		id := streamIdxServiceID(idx)
		if prev, dup := seen[id]; dup {
			t.Fatalf("service id %q collides at series %d and %d", id, prev, idx)
		}
		seen[id] = idx
	}
}

// The level distribution must stay skewed: a uniform draw would produce only
// mid-sized postings and never exercise the posting-size crossover the
// filter_level_error / filter_level_info pair is meant to straddle.
func TestStreamIdxLevelDistributionIsSkewed(t *testing.T) {
	counts := make(map[string]int)
	collectStreamIdxLevels(20000, counts)
	info, err := counts["INFO"], counts["ERROR"]
	if info == 0 || err == 0 {
		t.Fatalf("level draw produced a degenerate distribution: %v", counts)
	}
	ratio := float64(info) / float64(err)
	if ratio < 5 || ratio > 40 {
		t.Errorf("INFO:ERROR ratio = %.1f, want roughly 16 (0.80 vs 0.05)", ratio)
	}
}

// Durations feed the prefix-coded numeric trie; a degenerate or negative
// distribution would make the range query in the catalog meaningless.
func TestStreamIdxDurationIsPositiveAndSpread(t *testing.T) {
	durations := collectStreamIdxDurations(5000)
	minD, maxD := int64(math.MaxInt64), int64(0)
	for _, d := range durations {
		if d < 1 {
			t.Fatalf("duration %d is not positive", d)
		}
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	if maxD-minD < 100 {
		t.Errorf("duration spread is only [%d, %d]; the log-normal tail did not materialize", minD, maxD)
	}
}

// Every catalog entry must pin an explicit Limit: BanyanDB defaults to 100, and
// a probe measuring a silently-truncated query reports the wrong latency.
func TestStreamIndexCatalogPinsLimit(t *testing.T) {
	entries, err := loadStreamCatalog(filepath.Join("catalog", "stream_index.json"))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("catalog is empty")
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if _, dup := seen[entry.ID]; dup {
			t.Errorf("duplicate catalog id %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		if entry.Request.GetLimit() == 0 {
			t.Errorf("catalog entry %q has no explicit limit; BanyanDB would default to 100", entry.ID)
		}
		if len(entry.Request.GetGroups()) == 0 {
			t.Errorf("catalog entry %q has no group", entry.ID)
		}
		if entry.Request.GetProjection() == nil {
			t.Errorf("catalog entry %q has no projection", entry.ID)
		}
	}
}

// The catalog is the whole point of the pilot, so assert it still covers both
// sides of the split: a series-index criterion on the entity tag, an
// element-index criterion on a non-entity tag, and at least one index-order
// sort.
//
// This inspects the DECODED criteria and orderBy, not a substring of the raw
// JSON. An earlier revision grepped the whole request, which also contains the
// projection — and the projection lists the same tag names — so deleting every
// criterion from the catalog still satisfied both assertions.
func TestStreamIndexCatalogCoversBothIndexes(t *testing.T) {
	entries, err := loadStreamCatalog(filepath.Join("catalog", "stream_index.json"))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	// collectTagNames walks a criteria tree and returns every tag it filters on.
	var collectTagNames func(c *modelv1.Criteria, into map[string]struct{})
	collectTagNames = func(c *modelv1.Criteria, into map[string]struct{}) {
		if c == nil {
			return
		}
		if cond := c.GetCondition(); cond != nil {
			into[cond.GetName()] = struct{}{}
		}
		if le := c.GetLe(); le != nil {
			collectTagNames(le.GetLeft(), into)
			collectTagNames(le.GetRight(), into)
		}
	}

	var hasSeries, hasElement, hasSort bool
	for _, entry := range entries {
		tags := make(map[string]struct{})
		collectTagNames(entry.Request.GetCriteria(), tags)
		if _, ok := tags[streamIdxTagServiceID]; ok {
			hasSeries = true
		}
		for _, tag := range []string{streamIdxTagLevel, streamIdxTagDuration, streamIdxTagMessage, streamIdxTagPod} {
			if _, ok := tags[tag]; ok {
				hasElement = true
			}
		}
		if entry.Request.GetOrderBy().GetIndexRuleName() != "" {
			hasSort = true
		}
	}
	if !hasSeries {
		t.Error("no catalog entry filters on the entity tag; the series index would go unobserved")
	}
	if !hasElement {
		t.Error("no catalog entry filters on a non-entity indexed tag; the element index would go unobserved")
	}
	if !hasSort {
		t.Error("no catalog entry pins an index rule for sorting")
	}
}

func TestDirOf(t *testing.T) {
	cases := map[string]string{
		"latency.json":      ".",
		"a/latency.json":    "a",
		"a/b/latency.json":  "a/b",
		"/abs/latency.json": "/abs",
		"/latency.json":     "/",
	}
	for path, want := range cases {
		if got := dirOf(path); got != want {
			t.Errorf("dirOf(%q) = %q, want %q", path, got, want)
		}
	}
}

// Go emits a summary header for debug=1; per-goroutine stacks are debug=2. This
// function used to count only the legacy per-goroutine shape, so it returned 0
// on every current runtime and silently disabled every goroutine-leak gate.
func TestCountGoroutinesParsesSummaryHeader(t *testing.T) {
	modern := "goroutine profile: total 167\n42 @ 0x48de0e 0x41ca2e\n13 @ 0x1 0x2\n"
	if got, err := countGoroutines([]byte(modern)); err != nil || got != 167 {
		t.Errorf("countGoroutines(modern) = %d, %v; want 167, nil", got, err)
	}
	legacy := "goroutine 1 [running]:\n\tx.go:1 +0x1\n\ngoroutine 2 [select]:\n\ty.go:2 +0x2\n"
	if got, err := countGoroutines([]byte(legacy)); err != nil || got != 2 {
		t.Errorf("countGoroutines(legacy) = %d, %v; want 2, nil", got, err)
	}
	// Trailing decoration on the header must not defeat the parse.
	decorated := "goroutine profile: total 12 @ 0x1\n1 @ 0x2 0x3\n"
	if got, err := countGoroutines([]byte(decorated)); err != nil || got != 12 {
		t.Errorf("countGoroutines(decorated) = %d, %v; want 12, nil", got, err)
	}
}

// Unreadable input must be an ERROR, not a zero. A gate that reads "0
// goroutines" from an HTML error page served with HTTP 200 would pass on
// evidence it never read.
func TestCountGoroutinesRejectsUnreadableBodies(t *testing.T) {
	for name, body := range map[string]string{
		"empty":         "",
		"html error":    "<html><body>503 Service Unavailable</body></html>",
		"json error":    `{"error":"pprof disabled"}`,
		"bad total":     "goroutine profile: total notanumber\n",
		"unrelated log": "starting server\nlistening on :17912\n",
	} {
		if got, err := countGoroutines([]byte(body)); err == nil {
			t.Errorf("countGoroutines(%s) = %d, nil; want an error", name, got)
		}
	}
}

// probeTestServer exercises the real concurrent reporting loop without a database.
type probeTestServer struct {
	streamv1.UnimplementedStreamServiceServer
}

func (*probeTestServer) Query(context.Context, *streamv1.QueryRequest) (*streamv1.QueryResponse, error) {
	return &streamv1.QueryResponse{}, nil
}

func TestLatencyProbeFirstWindowMinimum(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	streamv1.RegisterStreamServiceServer(server, &probeTestServer{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	conn, connErr := grpc.NewClient("passthrough:///probe-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, connErr)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	report := streamIdxLatencyProbe(context.Background(), conn, streamCatalogTestPath,
		1000, time.Hour, 3, probeDefaultMinCalls, time.Millisecond, 200*time.Millisecond)
	require.True(t, report.Pass)
	require.NotEmpty(t, report.Samples)
	require.GreaterOrEqual(t, report.Samples[0].Calls, int64(10), "first reporting window must accumulate at least 10 calls per query")
}

func TestProbeWindowEmissionThreshold(t *testing.T) {
	accums := map[string]*probeAccum{"q": {}}
	report := latencyReport{Pass: true}
	// Errors must neither count toward the threshold nor disappear on deferral.
	accums["q"].recordErr(errors.New("query failed"))
	for i := 1; i < probeDefaultMinCalls; i++ {
		accums["q"].record(float64(i), i)
		emitProbeWindow(&report, accums, timeZero, probeDefaultMinCalls, false)
		require.Empty(t, report.Samples, "must retain a window below the threshold")
	}
	require.False(t, report.Pass)
	accums["q"].record(10, 10)
	emitProbeWindow(&report, accums, timeZero, probeDefaultMinCalls, false)
	require.Len(t, report.Samples, 1)
	sample := report.Samples[0]
	require.Equal(t, int64(10), sample.Calls)
	require.Equal(t, int64(1), sample.Errors)
	require.Equal(t, "query failed", sample.FirstErr)
	require.Equal(t, 5.5, sample.MeanMs)
	require.Equal(t, int64(1), sample.RowsMin)
	require.Equal(t, int64(10), sample.RowsMax)
	require.False(t, sample.LowConfidence)
	emitProbeWindow(&report, accums, timeZero, probeDefaultMinCalls, true)
	require.Len(t, report.Samples, 1, "empty final flush must not duplicate observations")
}

func TestProbeWindowRetainsSparseQueriesAndTail(t *testing.T) {
	accums := map[string]*probeAccum{"full": {}, "sparse": {}, "failed": {}}
	report := latencyReport{Pass: true}
	// Concurrent workers use the same accumulators; emission follows their join.
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			accums["full"].record(5, 7)
		}()
	}
	workers.Wait()
	accums["sparse"].record(9, 2)
	accums["failed"].recordErr(errors.New("failed tail"))
	emitProbeWindow(&report, accums, timeZero, 10, false)
	require.Len(t, report.Samples, 1)
	require.Equal(t, "full", report.Samples[0].QueryID)
	require.Equal(t, int64(10), report.Samples[0].Calls)
	emitProbeWindow(&report, accums, timeZero, 10, true)
	require.Len(t, report.Samples, 3)
	require.False(t, report.Pass)
	require.Equal(t, "failed", report.Samples[1].QueryID)
	require.Equal(t, int64(1), report.Samples[1].Errors)
	require.True(t, report.Samples[1].LowConfidence)
	require.Equal(t, "sparse", report.Samples[2].QueryID)
	require.Equal(t, int64(1), report.Samples[2].Calls)
	require.True(t, report.Samples[2].LowConfidence)
}

func TestLatencyProbeRejectsInvalidMinimum(t *testing.T) {
	for _, minimum := range []string{"0", "-1"} {
		cmd := newLatencyProbeCmd()
		cmd.SetArgs([]string{"--catalog", streamCatalogTestPath, "--until", "1000", "--min-calls", minimum})
		require.ErrorContains(t, cmd.Execute(), "min-calls must be positive")
	}
}
