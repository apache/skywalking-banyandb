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

package measure

import (
	"container/heap"
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Microbenchmarks (G9e) — paired row-path vs vectorized-path benchmarks for
// the G9 query shapes. The vec half is the *production* compute path: a
// vectorized.Pipeline composed of the same measure-level operators
// plan.Execute wires (source → [BatchAggregation | BatchTop] → BatchLimit →
// NewIteratorFromPipeline), drained through the public adapter. The row half
// is a faithful replica of pkg/query/logical/measure's reduce/top — it cannot
// be imported (cycle: logical/measure → this package), so it is replicated
// here exactly as diff_test.go documents for rowSerialize.
//
// The earlier harness benchmarked NewMIterator (the deprecated
// leaf-substitution path) which is scan/decode only and never runs
// Top/Agg/GroupBy — that did not measure the production vec compute the
// parity gate validates. This file replaces it.
//
// Workload scales are bounded for unit-bench tractability — the integration
// macro suite at test/integration/standalone/benchmark/ exercises full-scale
// shapes against the real Measure module. Acceptance gates are ratios
// (vec/row), so the relative comparison holds at any scale.
//
// Run via:
//
//	go test ./pkg/query/vectorized/measure -bench=. -benchmem -count=5 -benchtime=2s

// Repeated literals (goconst min-occurrences 4).
const (
	benchSvc       = "svc"
	benchValue     = "value"
	benchDefault   = "default"
	benchEnvID     = "env_id"
	benchVInt      = "v_int"
	benchVFloat    = "v_float"
	benchCriteria  = "criteria"
	defaultBatch   = 1024
	defaultMemMiB  = 64
	defaultTopN    = 50
	defaultLimitN  = 100
	groupKeyCardin = 16
)

// queryShape selects which operator pipeline a workload exercises. Each
// shape names one production query class the vec subsystem handles via
// plan.Dispatch (scan/top/scalarReduce/rawGroupBy/groupByAgg/hiddenTags/
// countFloat); the row half reduces the same input with the row path's
// algorithm so the gate ratio reflects the real compute trade-off.
type queryShape int

// queryShape values.
const (
	shapeScan queryShape = iota
	shapeTop
	shapeScalarReduce
	shapeRawGroupBy
	shapeGroupByAgg
	shapeHiddenTags
	shapeCountFloat
)

// workloadSpec parameterizes a benchmark workload and the query shape applied
// on top of its scan output.
//
// chunksPerSeries > 1 splits each series's rowsPer rows into that many
// *model.MeasureResult instances, simulating the multi-block storage path
// where queryResult.merge produces multiple Pull() results per series.
// Default 1 = single-block per series.
type workloadSpec struct {
	groupBy         *model.MeasureGroupBy
	agg             *model.MeasureAgg
	id              string
	topField        string
	criteriaTag     string
	tagFamilies     []tagSpec
	fields          []fieldSpec
	series          int
	rowsPer         int
	chunksPerSeries int
	topN            int
	limitN          int
	shape           queryShape
	topAsc          bool
}

type tagSpec struct {
	family string
	name   string
	col    databasev1.TagType
}

type fieldSpec struct {
	name string
	col  databasev1.FieldType
}

// Workload catalog: a scan baseline (W1/W2 for regression continuity), then
// one workload per G9 query shape. Total rows per workload is held near 100k
// so each iteration amortizes fixture build cost under -benchtime=2s.
var (
	// W1 — single-series scan baseline (regression continuity).
	w1 = workloadSpec{
		id:     "W1",
		shape:  shapeScan,
		series: 1, rowsPer: 10000,
		fields: []fieldSpec{{name: benchVInt, col: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	// W2 — multi-tag multi-series scan baseline (regression continuity).
	w2 = workloadSpec{
		id:     "W2",
		shape:  shapeScan,
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{
			{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING},
			{family: benchDefault, name: benchEnvID, col: databasev1.TagType_TAG_TYPE_INT},
		},
		fields: []fieldSpec{
			{name: benchVInt, col: databasev1.FieldType_FIELD_TYPE_INT},
			{name: benchVFloat, col: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}
	// W3 — Top-N by an int field over a scan (Scan → Top → Limit).
	w3 = workloadSpec{
		id:     "W3",
		shape:  shapeTop,
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: benchValue, col: databasev1.FieldType_FIELD_TYPE_INT}},
		topField:    benchValue, topN: defaultTopN, topAsc: false, limitN: defaultLimitN,
	}
	// W4 — scalar reduce: SUM over an int field, no GroupBy (Agg only).
	w4 = workloadSpec{
		id:     "W4",
		shape:  shapeScalarReduce,
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: benchValue, col: databasev1.FieldType_FIELD_TYPE_INT}},
		agg:         &model.MeasureAgg{FieldName: benchValue, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		limitN:      defaultLimitN,
	}
	// W5 — raw GroupBy by svc, no Agg: first-seen row per group.
	w5 = workloadSpec{
		id:     "W5",
		shape:  shapeRawGroupBy,
		series: groupKeyCardin, rowsPer: 6250,
		tagFamilies: []tagSpec{{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: benchValue, col: databasev1.FieldType_FIELD_TYPE_INT}},
		groupBy:     &model.MeasureGroupBy{TagFamily: benchDefault, TagNames: []string{benchSvc}},
		limitN:      defaultLimitN,
	}
	// W6 — GroupBy by svc + SUM over an int field.
	w6 = workloadSpec{
		id:     "W6",
		shape:  shapeGroupByAgg,
		series: groupKeyCardin, rowsPer: 6250,
		tagFamilies: []tagSpec{{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: benchValue, col: databasev1.FieldType_FIELD_TYPE_INT}},
		groupBy:     &model.MeasureGroupBy{TagFamily: benchDefault, TagNames: []string{benchSvc}},
		agg:         &model.MeasureAgg{FieldName: benchValue, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		limitN:      defaultLimitN,
	}
	// W7 — hidden-tags egress strip: a criteria tag materialized for
	// storage-side filtering but absent from the visible projection, then
	// stripped at egress (both paths pay the strip cost).
	w7 = workloadSpec{
		id:     "W7",
		shape:  shapeHiddenTags,
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{
			{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING},
			{family: benchDefault, name: benchCriteria, col: databasev1.TagType_TAG_TYPE_STRING},
		},
		fields:      []fieldSpec{{name: benchVInt, col: databasev1.FieldType_FIELD_TYPE_INT}},
		criteriaTag: benchCriteria,
	}
	// W8 — COUNT over a float field: exercises the float-typed COUNT slot
	// (output follows input type, so COUNT-on-float emits a float).
	w8 = workloadSpec{
		id:     "W8",
		shape:  shapeCountFloat,
		series: groupKeyCardin, rowsPer: 6250,
		tagFamilies: []tagSpec{{family: benchDefault, name: benchSvc, col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: benchVFloat, col: databasev1.FieldType_FIELD_TYPE_FLOAT}},
		groupBy:     &model.MeasureGroupBy{TagFamily: benchDefault, TagNames: []string{benchSvc}},
		agg:         &model.MeasureAgg{FieldName: benchVFloat, Func: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT},
		limitN:      defaultLimitN,
	}

	allWorkloads = []workloadSpec{w1, w2, w3, w4, w5, w6, w7, w8}
)

// buildResults materializes a deterministic []*model.MeasureResult for the
// workload. Each cell allocates a fresh *modelv1.TagValue / *modelv1.FieldValue
// — mirroring production storage's mustDecodeTagValue, which produces a new
// wrapper per cell from raw bytes (3 allocs/cell). An earlier version reused
// singleton wrappers across all rows; that biased the gates because the row
// path and passthrough column type both inherit a free pre-built pointer that
// production never has, while a native-typed column type pays the wrapper
// reconstruction cost at egress and looks artificially slower. With per-cell
// wrappers, all paths see the same storage-decode cost the production
// queryResult pays — the ratios reflect the real pipeline trade-off.
//
// To exercise GroupBy/Top with realistic cardinality, the svc tag and the
// numeric field vary per series/row: svc = "svc-<sid mod groupKeyCardin>"
// so the number of distinct groups is bounded, and the numeric value is
// (row index) so Top has a non-degenerate ordering.
//
// chunksPerSeries > 1 splits each series's rows across multiple
// *model.MeasureResult entries, simulating multi-block heap-merge output
// from queryResult.merge.
func buildResults(spec workloadSpec) []*model.MeasureResult {
	chunks := max(spec.chunksPerSeries, 1)
	results := make([]*model.MeasureResult, 0, spec.series*chunks)
	for s := range spec.series {
		sid := common.SeriesID(s + 1)
		group := s % groupKeyCardin
		for c := range chunks {
			start := (spec.rowsPer * c) / chunks
			end := (spec.rowsPer * (c + 1)) / chunks
			n := end - start
			if n <= 0 {
				continue
			}
			r := &model.MeasureResult{SID: sid}
			r.Timestamps = make([]int64, n)
			r.Versions = make([]int64, n)
			r.ShardIDs = make([]common.ShardID, n)
			for i := range n {
				r.Timestamps[i] = int64(start + i)
				r.Versions[i] = 1
			}
			if len(spec.tagFamilies) > 0 {
				tags := make([]model.Tag, 0, len(spec.tagFamilies))
				for _, ts := range spec.tagFamilies {
					values := make([]*modelv1.TagValue, n)
					for i := range values {
						values[i] = freshTagValueFor(ts, group, start+i)
					}
					tags = append(tags, model.Tag{Name: ts.name, Values: values})
				}
				r.TagFamilies = []model.TagFamily{{Name: spec.tagFamilies[0].family, Tags: tags}}
			}
			if len(spec.fields) > 0 {
				r.Fields = make([]model.Field, 0, len(spec.fields))
				for _, f := range spec.fields {
					values := make([]*modelv1.FieldValue, n)
					for i := range values {
						values[i] = freshFieldValueFor(f, start+i)
					}
					r.Fields = append(r.Fields, model.Field{Name: f.name, Values: values})
				}
			}
			results = append(results, r)
		}
	}
	return results
}

// freshTagValueFor builds a brand-new *modelv1.TagValue per call. The svc
// tag varies by group so GroupBy has bounded cardinality; everything else
// follows freshTagValue's storage-decode-shaped wrappers.
func freshTagValueFor(ts tagSpec, group, row int) *modelv1.TagValue {
	if ts.col == databasev1.TagType_TAG_TYPE_STRING && ts.name == benchSvc {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: "svc-" + string(rune('a'+group%26))},
		}}
	}
	if ts.col == databasev1.TagType_TAG_TYPE_STRING {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: "c-" + string(rune('a'+(row+group)%26))},
		}}
	}
	return freshTagValue(ts.col)
}

// freshFieldValueFor varies the numeric value by row so Top/Agg see a
// non-degenerate distribution; non-numeric fields fall back to the static
// storage-decode-shaped wrapper.
func freshFieldValueFor(f fieldSpec, row int) *modelv1.FieldValue {
	switch f.col {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(row)}}}
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(row)}}}
	case databasev1.FieldType_FIELD_TYPE_STRING, databasev1.FieldType_FIELD_TYPE_DATA_BINARY,
		databasev1.FieldType_FIELD_TYPE_UNSPECIFIED:
		return freshFieldValue(f.col)
	}
	return freshFieldValue(f.col)
}

// freshTagValue builds a brand-new *modelv1.TagValue per call. The shape and
// inner alloc count mirror mustDecodeTagValue (wrapper + oneof + inner).
func freshTagValue(t databasev1.TagType) *modelv1.TagValue {
	switch t {
	case databasev1.TagType_TAG_TYPE_INT:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 7}}}
	case databasev1.TagType_TAG_TYPE_STRING:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{0xfe, 0xed}}}
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}}}
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"x", "y"}}}}
	case databasev1.TagType_TAG_TYPE_UNSPECIFIED, databasev1.TagType_TAG_TYPE_TIMESTAMP:
		// Bench fixtures never use these variants.
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}}
}

// freshFieldValue is the field-side counterpart to freshTagValue. Each call
// allocates a new wrapper (mirroring mustDecodeFieldValue).
func freshFieldValue(t databasev1.FieldType) *modelv1.FieldValue {
	switch t {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.14}}}
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "ok"}}}
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{0xab, 0xcd}}}
	case databasev1.FieldType_FIELD_TYPE_UNSPECIFIED:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
	}
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
}

// buildSchema reconstructs the *databasev1.Measure schema corresponding to
// the workload, only declaring the tags and fields that the workload
// projects.
func buildSchema(spec workloadSpec) *databasev1.Measure {
	m := &databasev1.Measure{}
	if len(spec.tagFamilies) > 0 {
		fam := &databasev1.TagFamilySpec{Name: spec.tagFamilies[0].family}
		for _, t := range spec.tagFamilies {
			fam.Tags = append(fam.Tags, &databasev1.TagSpec{Name: t.name, Type: t.col})
		}
		m.TagFamilies = []*databasev1.TagFamilySpec{fam}
	}
	for _, f := range spec.fields {
		m.Fields = append(m.Fields, &databasev1.FieldSpec{Name: f.name, FieldType: f.col})
	}
	return m
}

// buildOpts derives MeasureQueryOptions matching the workload's projection,
// including GroupBy/Agg so BuildBatchSchema promotes the referenced columns
// to native typed columns exactly as the production planner does. For the
// hidden-tags shape the criteria tag IS materialized (storage-side filter
// input) but is removed from the *visible* projection by buildVisibleOpts.
func buildOpts(spec workloadSpec) model.MeasureQueryOptions {
	opts := model.MeasureQueryOptions{GroupBy: spec.groupBy, Agg: spec.agg}
	if len(spec.tagFamilies) > 0 {
		names := make([]string, 0, len(spec.tagFamilies))
		for _, t := range spec.tagFamilies {
			names = append(names, t.name)
		}
		opts.TagProjection = []model.TagProjection{{Family: spec.tagFamilies[0].family, Names: names}}
	}
	if len(spec.fields) > 0 {
		opts.FieldProjection = make([]string, 0, len(spec.fields))
		for _, f := range spec.fields {
			opts.FieldProjection = append(opts.FieldProjection, f.name)
		}
	}
	return opts
}

// hiddenSet returns the criteria tags that were projected only for
// storage-side filtering and must be stripped at egress, or an empty set.
func hiddenSet(spec workloadSpec) logical.HiddenTagSet {
	h := logical.NewHiddenTagSet()
	if spec.criteriaTag != "" {
		h.Add(spec.criteriaTag)
	}
	return h
}

// benchSink prevents the compiler from eliding the work inside benchmark
// loops — every drained row is summed into it.
var benchSink int

// Row-path baseline.
//
// rowSerialize (diff_test.go) replays the scan/serialize the row path's
// resultMIterator runs. The Agg/Top/GroupBy reduces below replicate
// pkg/query/logical/measure faithfully enough for representative cost — they
// cannot be imported (cycle: logical/measure → this package), exactly as
// diff_test.go:96-99 documents for rowSerialize. aggregation.* IS imported
// (cycle-free, the same package the row path uses; see
// measure_plan_aggregation.go:59,63,84,186,197).

// runRowPath drains the row baseline once for the workload's shape over a
// fresh cursor backed by cloned results, accumulating into benchSink.
func runRowPath(spec workloadSpec, results []*model.MeasureResult, opts model.MeasureQueryOptions) {
	qr := &fakeMeasureQueryResult{seq: cloneResults(results)}
	dps := rowSerialize(qr, opts)
	switch spec.shape {
	case shapeScan:
		benchSink += len(dps)
	case shapeHiddenTags:
		hidden := hiddenSet(spec)
		for _, idp := range dps {
			idp.DataPoint.TagFamilies = hidden.StripHiddenTags(idp.DataPoint.TagFamilies)
		}
		benchSink += len(dps)
	case shapeTop:
		benchSink += len(rowTopReduce(dps, spec))
	case shapeScalarReduce, shapeGroupByAgg, shapeCountFloat:
		benchSink += len(rowAggReduce(dps, spec))
	case shapeRawGroupBy:
		benchSink += len(rowGroupByFirst(dps, spec))
	}
}

// rowFieldIndex finds the projected index of name in opts.FieldProjection,
// matching the row path's logical.FieldRef.Spec.FieldIdx semantics
// (DataPoint.Fields are appended in FieldProjection order by rowSerialize).
func rowFieldIndex(opts model.MeasureQueryOptions, name string) int {
	for i, f := range opts.FieldProjection {
		if f == name {
			return i
		}
	}
	return -1
}

// rowTopReduce mirrors pkg/query/logical/measure.topOp.Execute: every data
// point is inserted into one bounded TopQueue keyed on the field value, then
// drained in sorted order. asc=false → top-N (largest); asc=true → bottom-N.
func rowTopReduce(dps []*measurev1.InternalDataPoint, spec workloadSpec) []*measurev1.InternalDataPoint {
	if spec.topN <= 0 {
		return dps
	}
	opts := buildOpts(spec)
	fieldIdx := rowFieldIndex(opts, spec.topField)
	h := &rowTopHeap{asc: spec.topAsc}
	for _, idp := range dps {
		v := rowNumericFieldValue(idp.GetDataPoint().GetFields(), fieldIdx)
		el := rowTopEl{idp: idp, val: v, seq: h.seq}
		h.seq++
		if h.Len() < spec.topN {
			heap.Push(h, el)
			continue
		}
		root := h.rows[0]
		if (spec.topAsc && el.val < root.val) || (!spec.topAsc && el.val > root.val) {
			h.rows[0] = el
			heap.Fix(h, 0)
		}
	}
	out := make([]*measurev1.InternalDataPoint, 0, h.Len())
	for h.Len() > 0 {
		out = append(out, heap.Pop(h).(rowTopEl).idp)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return rowApplyLimit(out, spec.limitN)
}

func rowNumericFieldValue(fields []*measurev1.DataPoint_Field, idx int) float64 {
	if idx < 0 || idx >= len(fields) {
		return 0
	}
	switch v := fields[idx].GetValue().GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return float64(v.Int.GetValue())
	case *modelv1.FieldValue_Float:
		return v.Float.GetValue()
	}
	return 0
}

type rowTopEl struct {
	idp *measurev1.InternalDataPoint
	val float64
	seq int
}

type rowTopHeap struct {
	rows []rowTopEl
	seq  int
	asc  bool
}

func (h *rowTopHeap) Len() int { return len(h.rows) }

func (h *rowTopHeap) Less(i, j int) bool {
	if h.rows[i].val != h.rows[j].val {
		if h.asc {
			return h.rows[i].val > h.rows[j].val
		}
		return h.rows[i].val < h.rows[j].val
	}
	return h.rows[i].seq > h.rows[j].seq
}

func (h *rowTopHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *rowTopHeap) Push(x any) { h.rows = append(h.rows, x.(rowTopEl)) }

func (h *rowTopHeap) Pop() any {
	n := len(h.rows)
	x := h.rows[n-1]
	h.rows = h.rows[:n-1]
	return x
}

// rowAggReduce mirrors pkg/query/logical/measure.aggGroupIterator /
// aggAllIterator: rows are partitioned by the GroupBy key (the whole result
// when GroupBy is unset → scalar reduce), each group folded through an
// aggregation.Map dispatched on the field's declared type (FIELD_TYPE_INT →
// int64, FIELD_TYPE_FLOAT → float64), then emitted one row per group.
func rowAggReduce(dps []*measurev1.InternalDataPoint, spec workloadSpec) []*measurev1.InternalDataPoint {
	opts := buildOpts(spec)
	fieldIdx := rowFieldIndex(opts, spec.agg.FieldName)
	isFloat := false
	for _, f := range spec.fields {
		if f.name == spec.agg.FieldName && f.col == databasev1.FieldType_FIELD_TYPE_FLOAT {
			isFloat = true
		}
	}
	keyOf := rowGroupKeyFunc(spec)
	order := make([]string, 0)
	seen := make(map[string]struct{})
	intMaps := make(map[string]aggregation.Map[int64])
	floatMaps := make(map[string]aggregation.Map[float64])
	tagsOf := make(map[string][]*modelv1.TagFamily)
	for _, idp := range dps {
		dp := idp.GetDataPoint()
		key := keyOf(dp)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			order = append(order, key)
			tagsOf[key] = dp.GetTagFamilies()
			if isFloat {
				m, _ := aggregation.NewMap[float64](spec.agg.Func)
				floatMaps[key] = m
			} else {
				m, _ := aggregation.NewMap[int64](spec.agg.Func)
				intMaps[key] = m
			}
		}
		fv := dp.GetFields()[fieldIdx].GetValue()
		if isFloat {
			n, _ := aggregation.FromFieldValue[float64](fv)
			floatMaps[key].In(n)
		} else {
			n, _ := aggregation.FromFieldValue[int64](fv)
			intMaps[key].In(n)
		}
	}
	out := make([]*measurev1.InternalDataPoint, 0, len(order))
	for _, key := range order {
		var val *modelv1.FieldValue
		if isFloat {
			val, _ = aggregation.ToFieldValue(floatMaps[key].Val())
		} else {
			val, _ = aggregation.ToFieldValue(intMaps[key].Val())
		}
		out = append(out, &measurev1.InternalDataPoint{DataPoint: &measurev1.DataPoint{
			TagFamilies: tagsOf[key],
			Fields:      []*measurev1.DataPoint_Field{{Name: spec.agg.FieldName, Value: val}},
		}})
	}
	return rowApplyLimit(out, spec.limitN)
}

// rowGroupByFirst mirrors the raw-GroupBy row path: groupIterator yields the
// whole group, processor.go keeps only current[0], so exactly the first-seen
// row of each group surfaces in group-insertion order.
func rowGroupByFirst(dps []*measurev1.InternalDataPoint, spec workloadSpec) []*measurev1.InternalDataPoint {
	keyOf := rowGroupKeyFunc(spec)
	seen := make(map[string]struct{})
	out := make([]*measurev1.InternalDataPoint, 0)
	for _, idp := range dps {
		key := keyOf(idp.GetDataPoint())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, idp)
	}
	return rowApplyLimit(out, spec.limitN)
}

// rowGroupKeyFunc returns a function computing the GroupBy key string from a
// DataPoint's projected tag families. When GroupBy is unset every row maps to
// the same key (scalar reduce).
func rowGroupKeyFunc(spec workloadSpec) func(*measurev1.DataPoint) string {
	if spec.groupBy == nil || len(spec.groupBy.TagNames) == 0 {
		return func(*measurev1.DataPoint) string { return "" }
	}
	names := spec.groupBy.TagNames
	family := spec.groupBy.TagFamily
	return func(dp *measurev1.DataPoint) string {
		var b []byte
		for _, tf := range dp.GetTagFamilies() {
			if tf.GetName() != family {
				continue
			}
			for _, want := range names {
				for _, tag := range tf.GetTags() {
					if tag.GetKey() == want {
						b = append(b, tag.GetValue().GetStr().GetValue()...)
						b = append(b, 0)
					}
				}
			}
		}
		return string(b)
	}
}

func rowApplyLimit(in []*measurev1.InternalDataPoint, limitN int) []*measurev1.InternalDataPoint {
	if limitN > 0 && len(in) > limitN {
		return in[:limitN]
	}
	return in
}

// Vectorized path — the *production* compute pipeline.
//
// Constructed the same way plan.Execute does (executor.go): a
// vectorized.PipelineBuilder with a shared MemoryTracker, source →
// [BatchAggregation | BatchTop] → BatchLimit, built, Init'd, then wrapped via
// NewIteratorFromPipeline and drained through the public adapter. package
// plan cannot be imported (cycle: plan → measure), so the measure-level
// operators are wired directly — this is byte-for-byte the operator graph
// plan.Execute produces for these shapes.

// runVectorizedPath drains the production vec pipeline once over a fresh
// cursor backed by cloned results.
func runVectorizedPath(spec workloadSpec, results []*model.MeasureResult,
	schema *databasev1.Measure, opts model.MeasureQueryOptions, cfg VectorizedConfig,
) {
	qr := &fakeMeasureQueryResult{seq: cloneResults(results)}
	batchSchema, schemaErr := BuildBatchSchema(schema, opts)
	if schemaErr != nil {
		panic(schemaErr)
	}
	pool := vectorized.NewBatchPool(batchSchema, cfg.BatchSize)
	source := NewBatchScan(qr, batchSchema, pool, cfg.BatchSize)

	tracker := vectorized.NewMemoryTracker(int64(cfg.QueryMemoryMiB) * 1024 * 1024)
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source)

	terminalSchema := batchSchema
	switch spec.shape {
	case shapeScan, shapeHiddenTags:
		// Schema-preserving scan; no breaker.
	case shapeTop:
		fieldIdx, ok := batchSchema.FieldIndex(spec.topField)
		if !ok {
			panic("bench: top field not in schema")
		}
		builder.Break(NewBatchTop(batchSchema, fieldIdx, spec.topN, spec.topAsc, cfg.BatchSize))
	case shapeScalarReduce, shapeRawGroupBy, shapeGroupByAgg, shapeCountFloat:
		ops, opsErr := BuildOperators(opts, batchSchema, tracker, cfg.BatchSize)
		if opsErr != nil {
			panic(opsErr)
		}
		if len(ops) != 1 {
			panic("bench: expected exactly one breaker operator")
		}
		builder.Break(ops[0])
		terminalSchema = ops[0].OutputSchema()
	}
	if spec.limitN > 0 {
		builder.Apply(NewBatchLimit(terminalSchema, 0, uint32(spec.limitN)))
	}

	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		panic(buildErr)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		panic(initErr)
	}
	egressPool := vectorized.NewBatchPool(terminalSchema, cfg.BatchSize)
	it := NewIteratorFromPipeline(context.Background(), pipeline, egressPool)
	if spec.shape == shapeHiddenTags {
		hidden := hiddenSet(spec)
		for it.Next() {
			for _, dp := range it.Current() {
				dp.DataPoint.TagFamilies = hidden.StripHiddenTags(dp.DataPoint.TagFamilies)
			}
			benchSink++
		}
	} else {
		for it.Next() {
			benchSink++
		}
	}
	if closeErr := it.Close(); closeErr != nil {
		panic(closeErr)
	}
}

// cfgFor returns the standard bench VectorizedConfig.
func cfgFor() VectorizedConfig {
	return VectorizedConfig{Enabled: true, BatchSize: defaultBatch, QueryMemoryMiB: defaultMemMiB}
}

func benchmarkRow(b *testing.B, spec workloadSpec) {
	results := buildResults(spec)
	opts := buildOpts(spec)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runRowPath(spec, results, opts)
	}
}

func benchmarkVectorized(b *testing.B, spec workloadSpec) {
	results := buildResults(spec)
	schema := buildSchema(spec)
	opts := buildOpts(spec)
	cfg := cfgFor()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runVectorizedPath(spec, results, schema, opts, cfg)
	}
}

// Paired benchmarks per workload.

func BenchmarkRowPath_W1(b *testing.B)        { benchmarkRow(b, w1) }
func BenchmarkVectorizedPath_W1(b *testing.B) { benchmarkVectorized(b, w1) }

func BenchmarkRowPath_W2(b *testing.B)        { benchmarkRow(b, w2) }
func BenchmarkVectorizedPath_W2(b *testing.B) { benchmarkVectorized(b, w2) }

func BenchmarkRowPath_W3(b *testing.B)        { benchmarkRow(b, w3) }
func BenchmarkVectorizedPath_W3(b *testing.B) { benchmarkVectorized(b, w3) }

func BenchmarkRowPath_W4(b *testing.B)        { benchmarkRow(b, w4) }
func BenchmarkVectorizedPath_W4(b *testing.B) { benchmarkVectorized(b, w4) }

func BenchmarkRowPath_W5(b *testing.B)        { benchmarkRow(b, w5) }
func BenchmarkVectorizedPath_W5(b *testing.B) { benchmarkVectorized(b, w5) }

func BenchmarkRowPath_W6(b *testing.B)        { benchmarkRow(b, w6) }
func BenchmarkVectorizedPath_W6(b *testing.B) { benchmarkVectorized(b, w6) }

func BenchmarkRowPath_W7(b *testing.B)        { benchmarkRow(b, w7) }
func BenchmarkVectorizedPath_W7(b *testing.B) { benchmarkVectorized(b, w7) }

func BenchmarkRowPath_W8(b *testing.B)        { benchmarkRow(b, w8) }
func BenchmarkVectorizedPath_W8(b *testing.B) { benchmarkVectorized(b, w8) }
