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

package csvfmt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func md(name string) *commonv1.Metadata {
	return &commonv1.Metadata{Name: name, Group: "g"}
}

func family(name string, tags ...*databasev1.TagSpec) *databasev1.TagFamilySpec {
	return &databasev1.TagFamilySpec{Name: name, Tags: tags}
}

func tag(name string, t databasev1.TagType) *databasev1.TagSpec {
	return &databasev1.TagSpec{Name: name, Type: t}
}

func field(name string, t databasev1.FieldType) *databasev1.FieldSpec {
	return &databasev1.FieldSpec{Name: name, FieldType: t}
}

func TestStreamLayoutUnionOrder(t *testing.T) {
	streams := []*databasev1.Stream{
		{Metadata: md("sw_service_log"), TagFamilies: []*databasev1.TagFamilySpec{
			family("searchable", tag("trace_id", databasev1.TagType_TAG_TYPE_STRING), tag("service_id", databasev1.TagType_TAG_TYPE_DATA_BINARY),
				tag("state", databasev1.TagType_TAG_TYPE_INT)),
			family("data", tag("data_binary", databasev1.TagType_TAG_TYPE_DATA_BINARY)),
		}},
		{Metadata: md("sw_browser_log"), TagFamilies: []*databasev1.TagFamilySpec{
			family("searchable", tag("service_id", databasev1.TagType_TAG_TYPE_DATA_BINARY), tag("page", databasev1.TagType_TAG_TYPE_STRING_ARRAY)),
			family("data", tag("data_binary", databasev1.TagType_TAG_TYPE_DATA_BINARY)),
		}},
	}
	l, err := StreamLayout(streams)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"_name", "_element_id", "_timestamp",
		"searchable.service_id", "searchable.page", "data.data_binary", "searchable.trace_id", "searchable.state",
	}, l.Header(), "sw_browser_log comes first by name")
	assert.Equal(t, []string{"sw_browser_log", "sw_service_log"}, l.Columns[3].Resources)
	assert.Equal(t, []string{"sw_service_log"}, l.Columns[6].Resources)
	assert.Nil(t, l.Columns[0].Resources)
	assert.Equal(t, ValueStringArray, l.Columns[4].Type)
	assert.Equal(t, KindTag, l.Columns[4].Kind)
	assert.Equal(t, "searchable", l.Columns[4].Family)
	assert.Equal(t, "page", l.Columns[4].Name)

	idx, ok := l.TagIndex("sw_browser_log", "searchable", "service_id")
	require.True(t, ok)
	assert.Equal(t, 3, idx)
	_, ok = l.TagIndex("sw_browser_log", "searchable", "trace_id")
	assert.False(t, ok)
	_, ok = l.TagIndex("missing", "searchable", "trace_id")
	assert.False(t, ok)

	row := l.NewRow()
	require.Len(t, row, len(l.Columns))
	for _, c := range row {
		assert.Equal(t, NullCell, c)
	}
	assert.Equal(t, []string{"#datatype", "string", "string", "timestamp", "base64Binary", "stringArray", "base64Binary", "string", "long"},
		l.DatatypeLine())

	// Determinism: building twice yields identical layouts.
	again, err := StreamLayout(streams)
	require.NoError(t, err)
	assert.Equal(t, l, again)
}

func TestStreamLayoutRejectsTimestampTag(t *testing.T) {
	_, err := StreamLayout([]*databasev1.Stream{{Metadata: md("s"), TagFamilies: []*databasev1.TagFamilySpec{
		family("f", tag("ts", databasev1.TagType_TAG_TYPE_TIMESTAMP)),
	}}})
	require.Error(t, err)
	_, err = MeasureLayout([]*databasev1.Measure{{Metadata: md("m"), TagFamilies: []*databasev1.TagFamilySpec{
		family("f", tag("ts", databasev1.TagType_TAG_TYPE_TIMESTAMP)),
	}}})
	require.Error(t, err)
	_, err = StreamLayout([]*databasev1.Stream{{Metadata: md("s"), TagFamilies: []*databasev1.TagFamilySpec{
		family("f", tag("u", databasev1.TagType_TAG_TYPE_UNSPECIFIED)),
	}}})
	require.Error(t, err)
}

func TestLayoutRejectsDuplicates(t *testing.T) {
	_, err := StreamLayout([]*databasev1.Stream{{Metadata: md("s")}, {Metadata: md("s")}})
	require.Error(t, err)
	_, err = StreamLayout([]*databasev1.Stream{{Metadata: md("s"), TagFamilies: []*databasev1.TagFamilySpec{
		family("f", tag("a", databasev1.TagType_TAG_TYPE_STRING), tag("a", databasev1.TagType_TAG_TYPE_STRING)),
	}}})
	require.Error(t, err)
	_, err = StreamLayout([]*databasev1.Stream{{Metadata: &commonv1.Metadata{}}})
	require.Error(t, err)
}

func TestMeasureLayoutFieldsAndConflictSplit(t *testing.T) {
	measures := []*databasev1.Measure{
		{Metadata: md("service_cpm_minute"), TagFamilies: []*databasev1.TagFamilySpec{
			family("default", tag("entity_id", databasev1.TagType_TAG_TYPE_STRING), tag("foo", databasev1.TagType_TAG_TYPE_STRING)),
		}, Fields: []*databasev1.FieldSpec{field("total", databasev1.FieldType_FIELD_TYPE_INT), field("value", databasev1.FieldType_FIELD_TYPE_INT)}},
		{Metadata: md("service_traffic"), TagFamilies: []*databasev1.TagFamilySpec{
			family("default", tag("entity_id", databasev1.TagType_TAG_TYPE_STRING), tag("foo", databasev1.TagType_TAG_TYPE_INT)),
		}},
		{Metadata: md("service_resp_time"), TagFamilies: []*databasev1.TagFamilySpec{
			family("default", tag("foo", databasev1.TagType_TAG_TYPE_STRING)),
		}, Fields: []*databasev1.FieldSpec{
			field("value", databasev1.FieldType_FIELD_TYPE_FLOAT), field("percentile", databasev1.FieldType_FIELD_TYPE_DATA_BINARY),
			field("total", databasev1.FieldType_FIELD_TYPE_INT),
		}},
		// The internal Top-N measure has no rows in CSV, so it takes no column.
		{Metadata: md(TopNResultMeasure), TagFamilies: []*databasev1.TagFamilySpec{
			family("_topN", tag("name", databasev1.TagType_TAG_TYPE_STRING), tag("foo", databasev1.TagType_TAG_TYPE_DATA_BINARY)),
		}, Fields: []*databasev1.FieldSpec{field("value", databasev1.FieldType_FIELD_TYPE_DATA_BINARY)}},
	}
	l, err := MeasureLayout(measures)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"_name", "_timestamp", "_version",
		"default.entity_id", "service_cpm_minute::default.foo", "service_resp_time::default.foo", "service_traffic::default.foo",
		"_field.total", "service_cpm_minute::_field.value", "service_resp_time::_field.value", "_field.percentile",
	}, l.Header())

	idx, ok := l.TagIndex("service_traffic", "default", "foo")
	require.True(t, ok)
	assert.Equal(t, 6, idx)
	assert.Equal(t, ValueInt, l.Columns[idx].Type)
	assert.Equal(t, []string{"service_traffic"}, l.Columns[idx].Resources)

	idx, ok = l.FieldIndex("service_resp_time", "total")
	require.True(t, ok)
	assert.Equal(t, 7, idx)
	assert.Equal(t, []string{"service_cpm_minute", "service_resp_time"}, l.Columns[idx].Resources)
	assert.Equal(t, KindField, l.Columns[idx].Kind)

	idx, ok = l.FieldIndex("service_resp_time", "value")
	require.True(t, ok)
	assert.Equal(t, ValueFloat, l.Columns[idx].Type)

	// Index-mode measures (no fields) contribute no field columns.
	_, ok = l.FieldIndex("service_traffic", "value")
	assert.False(t, ok)
	// A tag and a field with the same name live in separate key spaces.
	_, ok = l.TagIndex("service_cpm_minute", "", "total")
	assert.False(t, ok)
}

func TestTraceLayoutExcludesIDTags(t *testing.T) {
	traces := []*databasev1.Trace{
		{
			Metadata: md("sw_segments"), TraceIdTagName: "trace_id", SpanIdTagName: "span_id",
			Tags: []*databasev1.TraceTagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "start_time", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			},
		},
		{
			Metadata: md("zipkin"), TraceIdTagName: "tid", SpanIdTagName: "sid",
			Tags: []*databasev1.TraceTagSpec{
				{Name: "tid", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "sid", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		},
	}
	l, err := TraceLayout(traces)
	require.NoError(t, err)
	// zipkin's own trace_id tag would repeat the fixed trace_id header: it gets a column of
	// its own name.
	assert.Equal(t, []string{"_name", "trace_id", "span_id", "_span", "service_id", "start_time", "zipkin::trace_id", "duration"}, l.Header())
	assert.Equal(t, ValueBinary, l.Columns[3].Type)
	assert.Equal(t, ValueTimestamp, l.Columns[5].Type)
	_, ok := l.TagIndex("sw_segments", "", "trace_id")
	assert.False(t, ok)
	idx, ok := l.TagIndex("zipkin", "", "trace_id")
	require.True(t, ok)
	assert.Equal(t, 6, idx)
	_, ok = l.TagIndex("zipkin", "", "tid")
	assert.False(t, ok)
}

func TestPropertyLayout(t *testing.T) {
	props := []*databasev1.Property{
		{Metadata: md("ui_template"), Tags: []*databasev1.TagSpec{tag("name", databasev1.TagType_TAG_TYPE_STRING), tag("value", databasev1.TagType_TAG_TYPE_STRING)}},
		{Metadata: md("other"), Tags: []*databasev1.TagSpec{tag("value", databasev1.TagType_TAG_TYPE_STRING), tag("at", databasev1.TagType_TAG_TYPE_TIMESTAMP)}},
	}
	l, err := PropertyLayout(props)
	require.NoError(t, err)
	assert.Equal(t, []string{"_name", "_id", "_mod_revision", "_create_revision", "_updated_at", "_delete_time", "value", "at", "name"}, l.Header())
	assert.Equal(t, []ColumnKind{KindName, KindPropertyID, KindModRevision, KindCreateRevision, KindUpdatedAt, KindDeleteTime, KindTag, KindTag, KindTag},
		[]ColumnKind{
			l.Columns[0].Kind, l.Columns[1].Kind, l.Columns[2].Kind, l.Columns[3].Kind, l.Columns[4].Kind, l.Columns[5].Kind,
			l.Columns[6].Kind, l.Columns[7].Kind, l.Columns[8].Kind,
		})
	idx, ok := l.TagIndex("other", "", "value")
	require.True(t, ok)
	assert.Equal(t, 6, idx)
}

func TestLayoutIgnoresInputOrder(t *testing.T) {
	str, num := databasev1.TagType_TAG_TYPE_STRING, databasev1.TagType_TAG_TYPE_INT
	streams := []*databasev1.Stream{
		{Metadata: md("c"), TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("x", str), tag("c", str))}},
		{Metadata: md("a"), TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("x", num), tag("a", str))}},
		{Metadata: md("b"), TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("b", str), tag("x", str))}},
	}
	measures := []*databasev1.Measure{
		{Metadata: md("c"), TagFamilies: streams[0].TagFamilies, Fields: []*databasev1.FieldSpec{field("v", databasev1.FieldType_FIELD_TYPE_INT)}},
		{Metadata: md("a"), TagFamilies: streams[1].TagFamilies, Fields: []*databasev1.FieldSpec{field("v", databasev1.FieldType_FIELD_TYPE_FLOAT)}},
		{Metadata: md("b"), TagFamilies: streams[2].TagFamilies},
	}
	traceOf := func(name string, tags ...*databasev1.TraceTagSpec) *databasev1.Trace {
		return &databasev1.Trace{Metadata: md(name), Tags: tags}
	}
	traces := []*databasev1.Trace{
		traceOf("c", &databasev1.TraceTagSpec{Name: "x", Type: str}),
		traceOf("a", &databasev1.TraceTagSpec{Name: "x", Type: num}),
		traceOf("b", &databasev1.TraceTagSpec{Name: "b", Type: str}),
	}
	props := []*databasev1.Property{
		{Metadata: md("c"), Tags: []*databasev1.TagSpec{tag("x", str), tag("c", str)}},
		{Metadata: md("a"), Tags: []*databasev1.TagSpec{tag("x", num)}},
		{Metadata: md("b"), Tags: []*databasev1.TagSpec{tag("b", str)}},
	}
	// Every permutation of three resources gives the same layout.
	perms := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	build := func(p []int) []*Layout {
		var out []*Layout
		l, err := StreamLayout([]*databasev1.Stream{streams[p[0]], streams[p[1]], streams[p[2]]})
		require.NoError(t, err)
		out = append(out, l)
		l, err = MeasureLayout([]*databasev1.Measure{measures[p[0]], measures[p[1]], measures[p[2]]})
		require.NoError(t, err)
		out = append(out, l)
		l, err = TraceLayout([]*databasev1.Trace{traces[p[0]], traces[p[1]], traces[p[2]]})
		require.NoError(t, err)
		out = append(out, l)
		l, err = PropertyLayout([]*databasev1.Property{props[p[0]], props[p[1]], props[p[2]]})
		require.NoError(t, err)
		return append(out, l)
	}
	want := build(perms[0])
	assert.Equal(t, []string{"_name", "_element_id", "_timestamp", "a::f.x", "f.a", "f.b", "b::f.x", "c::f.x", "f.c"}, want[0].Header())
	for _, p := range perms[1:] {
		assert.Equal(t, want, build(p), "permutation %v", p)
	}
}

func TestEmptyLayout(t *testing.T) {
	l, err := MeasureLayout(nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"_name", "_timestamp", "_version"}, l.Header())
}

func TestLayoutHeadersAreUnique(t *testing.T) {
	str := databasev1.TagType_TAG_TYPE_STRING
	streams, err := StreamLayout([]*databasev1.Stream{
		{Metadata: md("s1"), TagFamilies: []*databasev1.TagFamilySpec{{Name: "a", Tags: []*databasev1.TagSpec{{Name: "b.c", Type: str}}}}},
		{Metadata: md("s2"), TagFamilies: []*databasev1.TagFamilySpec{{Name: "a.b", Tags: []*databasev1.TagSpec{{Name: "c", Type: str}}}}},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"_name", "_element_id", "_timestamp", "s1::a.b.c", "s2::a.b.c"}, streams.Header())

	measures, err := MeasureLayout([]*databasev1.Measure{{
		Metadata:    md("m"),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "_field", Tags: []*databasev1.TagSpec{{Name: "v", Type: str}}}},
		Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}})
	require.NoError(t, err)
	assert.Equal(t, []string{"_name", "_timestamp", "_version", "m::_field.v", "_field.v"}, measures.Header(), "_field.<name> stays the field's")
	idx, ok := measures.FieldIndex("m", "v")
	require.True(t, ok)
	assert.Equal(t, 4, idx)

	props, err := PropertyLayout([]*databasev1.Property{{
		Metadata: md("p"), Tags: []*databasev1.TagSpec{{Name: "_name", Type: str}, {Name: "x", Type: str}},
	}})
	require.NoError(t, err)
	assert.Equal(t, []string{"_name", "_id", "_mod_revision", "_create_revision", "_updated_at", "_delete_time", "p::_name", "x"}, props.Header())

	// Splitting cannot separate two keys of one resource whose headers coincide, nor a
	// split tag and a split field of the same name: such groups are refused.
	_, err = StreamLayout([]*databasev1.Stream{{Metadata: md("s"), TagFamilies: []*databasev1.TagFamilySpec{
		{Name: "a", Tags: []*databasev1.TagSpec{{Name: "b.c", Type: str}}},
		{Name: "a.b", Tags: []*databasev1.TagSpec{{Name: "c", Type: str}}},
	}}})
	require.ErrorContains(t, err, `two columns share the header "s::a.b.c"`)
	_, err = MeasureLayout([]*databasev1.Measure{
		{
			Metadata:    md("m"),
			TagFamilies: []*databasev1.TagFamilySpec{{Name: "_field", Tags: []*databasev1.TagSpec{{Name: "v", Type: str}}}},
			Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		},
		{Metadata: md("n"), Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT}}},
	})
	require.ErrorContains(t, err, `two columns share the header "m::_field.v"`)
}
