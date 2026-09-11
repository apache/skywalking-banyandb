// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"fmt"
	"math/bits"
	"runtime"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	benchmarkTimestampTag = "start_time"
	benchmarkServiceTag   = "service"
	benchmarkServiceValue = "benchmark"
)

// BenchmarkSIDXPartScanTimestampFilter measures the real SIDX part scan path.
// Forced cases clear only the request's timestamp selection hints. This is
// deliberately benchmark-local: the production TimeTagFilter still carries the
// exact range and must scan the same part. Auto cases retain the hints, allowing
// the production coverage-bypass decision to omit that matcher.
func BenchmarkSIDXPartScanTimestampFilter(b *testing.B) {
	previousMaxProcs := runtime.GOMAXPROCS(2)
	b.Cleanup(func() {
		runtime.GOMAXPROCS(previousMaxProcs)
	})

	fixtures := []struct {
		name   string
		rows   int
		reopen bool
		order  modelv1.Sort
	}{
		{name: "Memory/10K/ASC", rows: 10_000, order: modelv1.Sort_SORT_ASC},
		{name: "FlushedReopened/10K/DESC", rows: 10_000, reopen: true, order: modelv1.Sort_SORT_DESC},
		{name: "FlushedReopened/100K/ASC", rows: 100_000, reopen: true, order: modelv1.Sort_SORT_ASC},
	}
	for _, fixtureConfig := range fixtures {
		b.Run(fixtureConfig.name, func(b *testing.B) {
			fixture := newTimestampScanBenchmarkFixture(b, fixtureConfig.rows, fixtureConfig.reopen, false)
			fixture.runFullCoverage(b, fixtureConfig.order)
			partialFixture := newTimestampScanBenchmarkFixture(b, fixtureConfig.rows, fixtureConfig.reopen, true)
			partialFixture.runPartialOverlap(b, fixtureConfig.order)
		})
	}
}

type timestampScanBenchmarkFixture struct {
	instance sidx.SIDX
	rows     int
	minTime  int64
	maxTime  int64
}

func newTimestampScanBenchmarkFixture(b *testing.B, rows int, reopen, conservativeEnvelope bool) *timestampScanBenchmarkFixture {
	b.Helper()

	root := b.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	options, optionsErr := sidx.NewOptions(root, protector.Nop{})
	if optionsErr != nil {
		b.Fatalf("create SIDX options: %v", optionsErr)
	}
	instance, instanceErr := sidx.NewSIDX(fileSystem, options)
	if instanceErr != nil {
		b.Fatalf("create SIDX: %v", instanceErr)
	}

	const baseTimestamp = int64(1_700_000_000_000_000_000)
	minTime := baseTimestamp
	maxTime := baseTimestamp + int64(rows) - 1
	requests := make([]sidx.WriteRequest, 0, rows)
	for row := 0; row < rows; row++ {
		// The key deliberately has no timestamp semantics. Reversing its bits
		// gives a deterministic ordering independent of the timestamp column.
		key := int64(bits.Reverse64(uint64(row)))
		rowTimestamp := minTime + int64(row)
		requests = append(requests, sidx.WriteRequest{
			SeriesID: common.SeriesID(1),
			Key:      key,
			Data:     []byte(fmt.Sprintf("trace-%06d", row)),
			Tags: []sidx.Tag{
				{Name: benchmarkTimestampTag, Value: convert.Int64ToBytes(rowTimestamp), ValueType: pbv1.ValueTypeTimestamp},
				{Name: benchmarkServiceTag, Value: []byte(benchmarkServiceValue), ValueType: pbv1.ValueTypeStr},
			},
		})
	}
	partMinTime, partMaxTime := minTime, maxTime
	if conservativeEnvelope {
		partMinTime -= int64(rows) * 2
		partMaxTime += int64(rows) * 2
	}
	memPart, convertErr := instance.ConvertToMemPart(requests, 1, &partMinTime, &partMaxTime)
	if convertErr != nil {
		b.Fatalf("convert benchmark part: %v", convertErr)
	}
	instance.IntroduceMemPart(1, memPart)
	snapshot := instance.CurrentSnapshot()
	if snapshot == nil {
		b.Fatal("published benchmark part is not visible")
	}
	snapshot.DecRef()

	if reopen {
		flushIntroduction, flushErr := instance.Flush(map[uint64]struct{}{1: {}})
		if flushErr != nil {
			b.Fatalf("flush benchmark part: %v", flushErr)
		}
		if flushIntroduction == nil {
			b.Fatal("flush benchmark part: missing introduction")
		}
		instance.IntroduceFlushed(flushIntroduction)
		flushIntroduction.Release()
		if closeErr := instance.Close(); closeErr != nil {
			b.Fatalf("close flushed SIDX: %v", closeErr)
		}

		options.AvailablePartIDs = []uint64{1}
		instance, instanceErr = sidx.NewSIDX(fileSystem, options)
		if instanceErr != nil {
			b.Fatalf("reopen flushed SIDX: %v", instanceErr)
		}
	}
	b.Cleanup(func() {
		if closeErr := instance.Close(); closeErr != nil {
			b.Errorf("close benchmark SIDX: %v", closeErr)
		}
	})

	return &timestampScanBenchmarkFixture{instance: instance, rows: rows, minTime: minTime, maxTime: maxTime}
}

func (f *timestampScanBenchmarkFixture) runFullCoverage(b *testing.B, order modelv1.Sort) {
	b.Helper()

	for _, hasExistingPredicate := range []bool{false, true} {
		predicateName := "NoExistingPredicate"
		if hasExistingPredicate {
			predicateName = "ExistingServicePredicate"
		}
		for _, projectTimestamp := range []bool{false, true} {
			projectionName := "TimestampOmitted"
			if projectTimestamp {
				projectionName = "TimestampProjected"
			}
			baseRequest := f.request(order, f.minTime, f.maxTime, hasExistingPredicate, projectTimestamp)
			matcher := newTimestampTagFilterMatcher(
				timestamp.NewTimeRange(time.Unix(0, f.minTime), time.Unix(0, f.maxTime), true, true),
				benchmarkTimestampTag,
				baseRequest.TagFilter,
			)
			variants := []struct {
				name    string
				request sidx.QueryRequest
			}{
				{name: "NoTimeFilter", request: baseRequest},
				{name: "ForcedTimeFilter", request: f.forcedRequest(baseRequest, matcher)},
				{name: "AutoCoverageBypass", request: f.autoRequest(baseRequest, matcher)},
			}
			baseline := f.mustQueryRows(b, variants[0].request)
			if len(baseline) != f.rows {
				b.Fatalf("full-coverage baseline returned %d rows, want %d", len(baseline), f.rows)
			}
			for _, variant := range variants {
				rows := f.mustQueryRows(b, variant.request)
				assertBenchmarkRowsEqual(b, baseline, rows)
				f.runRequest(b, predicateName+"/"+projectionName+"/"+variant.name, variant.request, len(rows))
			}
		}
	}
}

func (f *timestampScanBenchmarkFixture) runPartialOverlap(b *testing.B, order modelv1.Sort) {
	b.Helper()

	for _, matchPercent := range []int{0, 10, 50, 100} {
		minTime, maxTime := f.partialRange(matchPercent)
		baseRequest := f.request(order, minTime, maxTime, false, false)
		matcher := newTimestampTagFilterMatcher(
			timestamp.NewTimeRange(time.Unix(0, minTime), time.Unix(0, maxTime), true, true),
			benchmarkTimestampTag,
			nil,
		)
		forcedRequest := f.forcedRequest(baseRequest, matcher)
		autoRequest := f.autoRequest(baseRequest, matcher)
		forcedRows := f.mustQueryRows(b, forcedRequest)
		autoRows := f.mustQueryRows(b, autoRequest)
		assertBenchmarkRowsEqual(b, forcedRows, autoRows)
		wantRows := f.rows * matchPercent / 100
		if len(forcedRows) != wantRows {
			b.Fatalf("partial %d%% returned %d rows, want %d", matchPercent, len(forcedRows), wantRows)
		}
		f.runRequest(b, fmt.Sprintf("Partial/%dPercent/ForcedTimeFilter", matchPercent), forcedRequest, len(forcedRows))
		f.runRequest(b, fmt.Sprintf("Partial/%dPercent/AutoSelectiveFilter", matchPercent), autoRequest, len(autoRows))
	}
}

func (f *timestampScanBenchmarkFixture) partialRange(matchPercent int) (int64, int64) {
	if matchPercent == 0 {
		// This intentionally overlaps the conservative envelope used by the
		// benchmark part selection fixture, while containing no row timestamp.
		return f.minTime - int64(f.rows), f.minTime - 1
	}
	matchingRows := f.rows * matchPercent / 100
	return f.minTime, f.minTime + int64(matchingRows) - 1
}

func (f *timestampScanBenchmarkFixture) request(order modelv1.Sort, minTime, maxTime int64, existingPredicate, projectTimestamp bool) sidx.QueryRequest {
	request := sidx.QueryRequest{
		SeriesIDs:        []common.SeriesID{1},
		Order:            &index.OrderBy{Sort: order},
		MinTimestamp:     &minTime,
		MaxTimestamp:     &maxTime,
		TimeIncludeStart: true,
		TimeIncludeEnd:   true,
		MaxBatchSize:     1024,
	}
	if existingPredicate {
		request.TagFilter = benchmarkServiceMatcher{}
		request.FilterTagNames = []string{benchmarkServiceTag}
	}
	// Keep this harmless non-timestamp column projected in every full-scan case.
	// Without it an empty projection instructs SIDX to load all tags, including
	// the timestamp column that TimestampOmitted is intended to avoid.
	projectedTags := []string{benchmarkServiceTag}
	if projectTimestamp {
		projectedTags = append(projectedTags, benchmarkTimestampTag)
	}
	if len(projectedTags) > 0 {
		request.TagProjection = []model.TagProjection{{Names: projectedTags}}
	}
	return request
}

func (f *timestampScanBenchmarkFixture) forcedRequest(request sidx.QueryRequest, matcher model.TagFilterMatcher) sidx.QueryRequest {
	// Clear only selection hints in this benchmark request. The production time
	// matcher retains the exact range, while partNeedsTimeFilter safely selects
	// the row predicate for this same immutable part.
	request.MinTimestamp = nil
	request.MaxTimestamp = nil
	request.TimeTagFilter = matcher
	request.TimeTagName = benchmarkTimestampTag
	return request
}

func (f *timestampScanBenchmarkFixture) autoRequest(request sidx.QueryRequest, matcher model.TagFilterMatcher) sidx.QueryRequest {
	request.TimeTagFilter = matcher
	request.TimeTagName = benchmarkTimestampTag
	return request
}

func (f *timestampScanBenchmarkFixture) mustQueryRows(b *testing.B, request sidx.QueryRequest) map[string]int64 {
	b.Helper()

	responses, queryErr := f.instance.QuerySync(context.Background(), request)
	if queryErr != nil {
		b.Fatalf("query benchmark part: %v", queryErr)
	}
	rows := make(map[string]int64, f.rows)
	for _, response := range responses {
		if response == nil {
			continue
		}
		if response.Error != nil {
			b.Fatalf("query response error: %v", response.Error)
		}
		for rowIndex, data := range response.Data {
			rows[string(data)] = response.Keys[rowIndex]
		}
	}
	return rows
}

func (f *timestampScanBenchmarkFixture) runRequest(b *testing.B, name string, request sidx.QueryRequest, outputRows int) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			responses, queryErr := f.instance.QuerySync(context.Background(), request)
			if queryErr != nil {
				b.Fatalf("query benchmark part: %v", queryErr)
			}
			rows := 0
			for _, response := range responses {
				if response != nil {
					rows += response.Len()
				}
			}
			if rows != outputRows {
				b.Fatalf("query returned %d rows, want %d", rows, outputRows)
			}
		}
		b.StopTimer()
		if elapsed := b.Elapsed(); elapsed > 0 {
			b.ReportMetric(float64(f.rows*b.N)/elapsed.Seconds(), "input_rows/s")
		}
		b.ReportMetric(float64(outputRows), "output_rows/op")
	})
}

func assertBenchmarkRowsEqual(b *testing.B, want, got map[string]int64) {
	b.Helper()
	if len(got) != len(want) {
		b.Fatalf("row count mismatch: got %d, want %d", len(got), len(want))
	}
	for traceID, wantKey := range want {
		gotKey, ok := got[traceID]
		if !ok || gotKey != wantKey {
			b.Fatalf("row mismatch for %q: got key %d present %t, want key %d", traceID, gotKey, ok, wantKey)
		}
	}
}

type benchmarkServiceMatcher struct{}

func (benchmarkServiceMatcher) Match(tags []*modelv1.Tag) (bool, error) {
	for _, tag := range tags {
		if tag.GetKey() == benchmarkServiceTag && tag.GetValue().GetStr().GetValue() == benchmarkServiceValue {
			return true, nil
		}
	}
	return false, nil
}

func (benchmarkServiceMatcher) GetDecoder() model.TagValueDecoder {
	return mustDecodeTagValueAndArray
}
