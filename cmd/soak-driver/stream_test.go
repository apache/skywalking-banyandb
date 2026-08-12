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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

const streamCatalogTestPath = "catalog/stream.json"

func TestLoadStreamCatalogParsesAllShapes(t *testing.T) {
	entries, loadErr := loadStreamCatalog(streamCatalogTestPath)
	require.NoError(t, loadErr)
	require.Len(t, entries, 4)
	wantIDs := []string{"stream_by_time_svc0", "stream_newest_svc0", "stream_by_duration_svc0", "stream_filter_state_svc0"}
	gotIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		gotIDs = append(gotIDs, entry.ID)
		assert.Equal(t, streamName, entry.Request.GetName(), "entry %s name", entry.ID)
		assert.Equal(t, []string{streamFixtureGroup}, entry.Request.GetGroups(), "entry %s groups", entry.ID)
		assert.NotEmpty(t, entry.Request.GetProjection().GetTagFamilies(), "entry %s projection", entry.ID)
		assert.NotZero(t, entry.Request.GetLimit(), "entry %s must pin a limit", entry.ID)
	}
	assert.ElementsMatch(t, wantIDs, gotIDs)
}

func TestStreamCatalogByDurationIsIndexOrdered(t *testing.T) {
	entries, loadErr := loadStreamCatalog(streamCatalogTestPath)
	require.NoError(t, loadErr)
	var entry streamCatalogEntry
	for _, e := range entries {
		if e.ID == "stream_by_duration_svc0" {
			entry = e
		}
	}
	require.NotEmpty(t, entry.ID)
	order := entry.Request.GetOrderBy()
	require.NotNil(t, order)
	assert.Equal(t, streamIndexDuration, order.GetIndexRuleName())
	assert.Equal(t, modelv1.Sort_SORT_DESC, order.GetSort())
	assert.True(t, streamCatalogEntryOrdered(entry), "index-order query must diff position-by-position")
}

func TestStreamCatalogFilterStateIsAnd(t *testing.T) {
	entries, loadErr := loadStreamCatalog(streamCatalogTestPath)
	require.NoError(t, loadErr)
	var entry streamCatalogEntry
	for _, e := range entries {
		if e.ID == "stream_filter_state_svc0" {
			entry = e
		}
	}
	require.NotEmpty(t, entry.ID)
	le := entry.Request.GetCriteria().GetLe()
	require.NotNil(t, le)
	assert.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_AND, le.GetOp())
	assert.Equal(t, streamTagServiceID, le.GetLeft().GetCondition().GetName())
	assert.Equal(t, "svc-0", le.GetLeft().GetCondition().GetValue().GetStr().GetValue())
	assert.Equal(t, streamTagState, le.GetRight().GetCondition().GetName())
	assert.Equal(t, int64(0), le.GetRight().GetCondition().GetValue().GetInt().GetValue())
	assert.False(t, streamCatalogEntryOrdered(entry))
}

func TestBuildStreamQueryRequestInjectsTimeRangeKeepsLimit(t *testing.T) {
	entries, loadErr := loadStreamCatalog(streamCatalogTestPath)
	require.NoError(t, loadErr)
	entry := entries[0]
	untilMs := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	window := 7 * 24 * time.Hour
	req := buildStreamQueryRequest(entry, untilMs, window)
	require.NotNil(t, req.GetTimeRange())
	assert.Equal(t, untilMs, req.GetTimeRange().GetEnd().AsTime().UnixMilli())
	assert.Equal(t, entry.Request.GetLimit(), req.GetLimit(), "pinned limit preserved")
	assert.Nil(t, entry.Request.GetTimeRange(), "clone semantics: original untouched")
}

func TestStreamServiceIDExactFraction(t *testing.T) {
	const series = 8
	svcZero := 0
	for idx := 0; idx < series; idx++ {
		if streamServiceIDForIndex(idx, series) == "svc-0" {
			svcZero++
		}
	}
	assert.Equal(t, int(float64(series)*streamSvcZeroFraction), svcZero)
}

func makeElement(id string, duration int64) *streamv1.Element {
	return &streamv1.Element{
		ElementId: id,
		TagFamilies: []*modelv1.TagFamily{{
			Name: streamTagFamily,
			Tags: []*modelv1.Tag{{Key: streamTagDuration, Value: streamIntTagValue(duration)}},
		}},
	}
}

func TestCompareStreamResultsUnorderedSetMatch(t *testing.T) {
	baseline := []*streamv1.Element{makeElement("a", 1), makeElement("b", 2)}
	replay := []*streamv1.Element{makeElement("b", 2), makeElement("a", 1)}
	_, matched := compareStreamResults("set", baseline, replay, false)
	assert.True(t, matched, "unordered comparison sorts by element_id")
}

func TestCompareStreamResultsOrderedPositionMatters(t *testing.T) {
	baseline := []*streamv1.Element{makeElement("a", 1), makeElement("b", 2)}
	replay := []*streamv1.Element{makeElement("b", 2), makeElement("a", 1)}
	_, matched := compareStreamResults("seq", baseline, replay, true)
	assert.False(t, matched, "ordered comparison preserves position")
}

func TestCompareStreamResultsChangedTagNoMatch(t *testing.T) {
	baseline := []*streamv1.Element{makeElement("a", 1)}
	replay := []*streamv1.Element{makeElement("a", 999)}
	div, matched := compareStreamResults("changed", baseline, replay, false)
	assert.False(t, matched)
	assert.NotEmpty(t, div.FirstDiffs)
}

func TestCompareStreamResultsLengthMismatch(t *testing.T) {
	baseline := []*streamv1.Element{makeElement("a", 1)}
	replay := []*streamv1.Element{makeElement("a", 1), makeElement("b", 2)}
	div, matched := compareStreamResults("len", baseline, replay, false)
	assert.False(t, matched)
	assert.Equal(t, 1, div.BaselineLen)
	assert.Equal(t, 2, div.ReplayLen)
}

func TestValidateEngineStream(t *testing.T) {
	assert.NoError(t, validateEngine(engineStream))
}
