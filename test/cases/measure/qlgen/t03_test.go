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

package qlgen

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func t03ExpectedPoints(t *testing.T, c Case) []map[string]any {
	t.Helper()
	var expected struct {
		Points []map[string]any `json:"dataPoints"`
	}
	require.NoError(t, json.Unmarshal(c.WantJSON, &expected))
	return expected.Points
}

func t03ExpectedRIDs(t *testing.T, c Case) []string {
	t.Helper()
	var ids []string
	for _, point := range t03ExpectedPoints(t, c) {
		families := point["tagFamilies"].([]any)
		tags := families[0].(map[string]any)["tags"].([]any)
		value := tags[0].(map[string]any)["value"].(map[string]any)["int"].(map[string]any)["value"].(string)
		ids = append(ids, value)
	}
	return ids
}

func TestT03OracleUsesExactLatticeAndFreshClock(t *testing.T) {
	for _, entry := range []struct {
		name  string
		where *T03Time
		clock *int64
		ids   []string
	}{
		{name: "strict-inclusive", where: t03And(t03Compare(">", 1000), t03Compare("<=", 3000)), ids: []string{"3", "4"}},
		{name: "fractional-lower", where: t03And(&T03Time{Op: ">=", Nanos: 2_000_000_001}, t03Compare("<=", 3000)), ids: []string{"4"}},
		{name: "fractional-upper", where: t03And(t03Compare(">=", 1000), &T03Time{Op: "<=", Nanos: 1_999_999_999}), ids: []string{"2"}},
		{name: "equality", where: t03Compare("=", 2000), ids: []string{"3"}},
		{name: "or", where: &T03Time{Op: "OR", Children: []*T03Time{t03Compare("=", 0), t03Compare("=", 5000)}}, ids: []string{"1", "6"}},
		{name: "contradiction", where: t03And(t03Compare(">", 3000), t03Compare("<", 3000))},
	} {
		t.Run(entry.name, func(t *testing.T) {
			c, err := BuildT03Case(entry.name, T03Query{Columns: []T03Column{t03RID()}, Where: entry.where, ClockMillis: entry.clock})
			require.NoError(t, err)
			require.Equal(t, entry.ids, t03ExpectedRIDs(t, c))
		})
	}
	var texts []string
	for _, millis := range []int64{3000, 6000} {
		clock := millis
		c, err := BuildT03Case("fresh", T03Query{
			Columns: []T03Column{t03RID()}, ClockMillis: &clock,
			Where: t03And(&T03Time{Op: ">=", Now: true, Nanos: -1_000_000_000}, &T03Time{Op: "<=", Now: true}),
		})
		require.NoError(t, err)
		if millis == 3000 {
			require.Equal(t, []string{"3", "4"}, t03ExpectedRIDs(t, c))
		} else {
			require.Equal(t, []string{"6", "7"}, t03ExpectedRIDs(t, c))
		}
		texts = append(texts, c.Query)
	}
	require.Equal(t, texts[0], texts[1], "resolved clock values must not enter reusable QL text")
}

func TestT03ProjectionKeepsIndependentNamesFamiliesAndNulls(t *testing.T) {
	c, err := BuildT03Case("aliases", T03Query{Columns: []T03Column{
		{Source: "host", Alias: "node"},
		{Source: "host", Alias: "copy"},
		{Source: "extra.location::TAG", Alias: "site"},
		{Source: "time", Alias: "observed"},
		{Source: "v::FIELD", Alias: "first"},
		{Source: "v::FIELD", Alias: "second"},
	}, Where: t03Compare("=", 3000)})
	require.NoError(t, err)
	points := t03ExpectedPoints(t, c)
	require.Len(t, points, 1)
	pointJSON, marshalErr := json.Marshal(points[0])
	require.NoError(t, marshalErr)
	require.JSONEq(t, `{
 "timestamp":"{{time:3000}}",
 "tagFamilies":[
 {"name":"default","tags":[{"key":"node","value":{"str":{"value":"b"}}},{"key":"copy","value":{"str":{"value":"b"}}}]},
 {"name":"extra","tags":[{"key":"site","value":{"str":{"value":"location-b"}}}]},
 {"name":"__ql","tags":[{"key":"observed","value":{"timestamp":"{{time:3000}}"}}]}
 ],
 "fields":[{"name":"first","value":{"null":0}},{"name":"second","value":{"null":0}}]
 }`, string(pointJSON))
}

func TestT03LimitPresenceAndCompleteInput(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, limit := range []*uint32{nil, uint32Pointer(0), uint32Pointer(125)} {
			c, err := BuildT03Case("paging", T03Query{Columns: []T03Column{t03RID()}, Legacy: legacy, Limit: limit})
			require.NoError(t, err)
			count := 129
			if limit != nil {
				count = int(*limit)
			}
			if legacy && (limit == nil || *limit == 0) {
				count = 100
			}
			require.Len(t, t03ExpectedPoints(t, c), count)
			require.Equal(t, !legacy && limit != nil && *limit == 0, c.NoScan)
		}
	}
}

func TestT03FixtureAndNanosecondRendering(t *testing.T) {
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	writes := T03FixtureWrites(base)
	require.Len(t, writes, 129)
	require.Len(t, T03FixtureSchema().GetTagFamilies(), 3)
	require.Equal(t, []string{"host"}, T03FixtureSchema().GetEntity().GetTagNames())
	require.Len(t, writes[0].GetDataPoint().GetTagFamilies(), 3)
	require.True(t, writes[128].GetDataPoint().GetTimestamp().AsTime().Equal(base.Add(128*time.Second)))
	c, err := BuildT03Case("fractional-params", T03Query{
		Columns: []T03Column{t03RID()}, Bind: true, TimestampParams: true,
		Where: &T03Time{Op: ">", Nanos: 2_000_000_001},
	})
	require.NoError(t, err)
	text, err := Render(c.Query, base)
	require.NoError(t, err)
	require.Contains(t, text, `"timestamp":"2026-10-16T00:00:02.000000001Z"`)
	require.NotContains(t, text, "{{")
	require.Equal(t, 1, strings.Count(text, "?"))
}
