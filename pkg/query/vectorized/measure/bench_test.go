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
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// Microbenchmarks (G5a) — paired row-path vs vectorized-path benchmarks for
// W1..W5 per the spec's Performance Evaluation Plan. Both paths consume the
// same fake MeasureQueryResult; only the serialization implementation
// differs. ns/op, B/op, allocs/op are reported via testing.B.
//
// Workload scales are bounded for unit-bench tractability — the integration
// macro suite at test/integration/standalone/benchmark/ exercises full-scale
// shapes against the real Measure module. Acceptance gates are ratios
// (vec/row), so the relative comparison holds at any scale; absolute
// throughput is not the gate.
//
// Run via:
//
//	go test ./pkg/query/vectorized/measure -bench=. -benchmem -count=5 -benchtime=2s

// workloadSpec parameterizes a benchmark workload.
//
// chunksPerSeries > 1 splits each series's rowsPer rows into that many
// *model.MeasureResult instances, simulating the multi-block storage path
// where queryResult.merge produces multiple Pull() results per series. The
// vec adapter must accumulate rows across PullBatch calls; the row adapter
// just sees more Pull invocations. Default 1 = single-block per series.
type workloadSpec struct {
	id              string
	tagFamilies     []tagSpec
	fields          []fieldSpec
	series          int
	rowsPer         int
	chunksPerSeries int
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

// w1..w5 mirror the spec's catalog, scaled for unit-bench memory budgets.
// Total rows per workload is held near 100k so each benchmark iteration
// completes quickly enough for -benchtime=2s to amortize fixture build cost.
var (
	w1 = workloadSpec{
		id:     "W1",
		series: 1, rowsPer: 10000,
		fields: []fieldSpec{{name: "v_int", col: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	w2 = workloadSpec{
		id:     "W2",
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{
			{family: "default", name: "svc", col: databasev1.TagType_TAG_TYPE_STRING},
			{family: "default", name: "env_id", col: databasev1.TagType_TAG_TYPE_INT},
		},
		fields: []fieldSpec{
			{name: "v_int", col: databasev1.FieldType_FIELD_TYPE_INT},
			{name: "v_float", col: databasev1.FieldType_FIELD_TYPE_FLOAT},
		},
	}
	w3 = workloadSpec{
		id:     "W3",
		series: 1000, rowsPer: 100,
		tagFamilies: []tagSpec{{family: "default", name: "svc", col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: "v_int", col: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	w4 = workloadSpec{
		id:     "W4",
		series: 100, rowsPer: 1000,
		tagFamilies: []tagSpec{{family: "default", name: "svc", col: databasev1.TagType_TAG_TYPE_STRING}},
		fields:      []fieldSpec{{name: "v_int", col: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	w5 = workloadSpec{
		id:     "W5",
		series: 1000, rowsPer: 100,
		tagFamilies: []tagSpec{
			{family: "default", name: "svc", col: databasev1.TagType_TAG_TYPE_STRING},
			{family: "default", name: "env_id", col: databasev1.TagType_TAG_TYPE_INT},
			{family: "default", name: "blob", col: databasev1.TagType_TAG_TYPE_DATA_BINARY},
			{family: "default", name: "ports", col: databasev1.TagType_TAG_TYPE_INT_ARRAY},
			{family: "default", name: "labels", col: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
		},
		fields: []fieldSpec{
			{name: "v_int", col: databasev1.FieldType_FIELD_TYPE_INT},
			{name: "v_float", col: databasev1.FieldType_FIELD_TYPE_FLOAT},
			{name: "v_str", col: databasev1.FieldType_FIELD_TYPE_STRING},
			{name: "v_bytes", col: databasev1.FieldType_FIELD_TYPE_DATA_BINARY},
		},
	}

	// Multi-block variants: same shape as their single-block counterpart but
	// with chunksPerSeries=4, so each series produces 4 *MeasureResult entries
	// instead of 1. Exercises BatchSourceFromBatchResult's cross-Pull
	// accumulation path that copies rows across PullBatch calls into a single
	// RecordBatch.
	w2mb = withChunks(w2, "W2-MB", 4)
	w4mb = withChunks(w4, "W4-MB", 4)
	w5mb = withChunks(w5, "W5-MB", 4)

	allWorkloads = []workloadSpec{w1, w2, w3, w4, w5, w2mb, w4mb, w5mb}
)

func withChunks(base workloadSpec, id string, chunks int) workloadSpec {
	c := base
	c.id = id
	c.chunksPerSeries = chunks
	return c
}

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
// chunksPerSeries > 1 splits each series's rows across multiple
// *model.MeasureResult entries, simulating multi-block heap-merge output
// from queryResult.merge.
func buildResults(spec workloadSpec) []*model.MeasureResult {
	chunks := max(spec.chunksPerSeries, 1)
	results := make([]*model.MeasureResult, 0, spec.series*chunks)
	for s := range spec.series {
		sid := common.SeriesID(s + 1)
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
						values[i] = freshTagValue(ts.col)
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
						values[i] = freshFieldValue(f.col)
					}
					r.Fields = append(r.Fields, model.Field{Name: f.name, Values: values})
				}
			}
			results = append(results, r)
		}
	}
	return results
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

// buildOpts derives MeasureQueryOptions matching the workload's projection.
func buildOpts(spec workloadSpec) model.MeasureQueryOptions {
	opts := model.MeasureQueryOptions{}
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

// benchSink prevents the compiler from eliding the work inside benchmark
// loops — every drained row is summed into it.
var benchSink int

// runRowPath drains the row-path serializer once over a fresh cursor backed
// by the supplied results, accumulating the row count into benchSink.
func runRowPath(results []*model.MeasureResult, opts model.MeasureQueryOptions) {
	qr := &fakeMeasureQueryResult{seq: results}
	benchSink += len(rowSerialize(qr, opts))
}

// runVectorizedPath drains the vectorized adapter once over a fresh cursor.
func runVectorizedPath(results []*model.MeasureResult, schema *databasev1.Measure,
	opts model.MeasureQueryOptions, cfg VectorizedConfig,
) {
	qr := &fakeMeasureQueryResult{seq: results}
	it, err := NewMIterator(context.Background(), qr, schema, opts, cfg)
	if err != nil {
		panic(err)
	}
	defer it.Close()
	for it.Next() {
		benchSink++
	}
}

func benchmarkRow(b *testing.B, spec workloadSpec) {
	results := buildResults(spec)
	opts := buildOpts(spec)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runRowPath(results, opts)
	}
}

func benchmarkVectorized(b *testing.B, spec workloadSpec) {
	results := buildResults(spec)
	schema := buildSchema(spec)
	opts := buildOpts(spec)
	cfg := VectorizedConfig{Enabled: true, BatchSize: 1024, QueryMemoryMiB: 64}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runVectorizedPath(results, schema, opts, cfg)
	}
}

// Paired benchmarks per spec.

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

func BenchmarkRowPath_W2MB(b *testing.B)        { benchmarkRow(b, w2mb) }
func BenchmarkVectorizedPath_W2MB(b *testing.B) { benchmarkVectorized(b, w2mb) }

func BenchmarkRowPath_W4MB(b *testing.B)        { benchmarkRow(b, w4mb) }
func BenchmarkVectorizedPath_W4MB(b *testing.B) { benchmarkVectorized(b, w4mb) }

func BenchmarkRowPath_W5MB(b *testing.B)        { benchmarkRow(b, w5mb) }
func BenchmarkVectorizedPath_W5MB(b *testing.B) { benchmarkVectorized(b, w5mb) }
