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

package plan

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logicalmeasure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// runMultiGroupAgg runs req over the groups the way a standalone node does: every group is dispatched in partial
// mode with the merge's group request, then the merge reduces them. It returns the rendered rows in order, or the
// error the merge failed with.
func runMultiGroupAgg(t *testing.T, req *measurev1.QueryRequest, groups []*mgBatchEC, forbidTags ...string) ([]string, error) {
	t.Helper()
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16}
	var schemas []logical.Schema
	var measureSchemas []*databasev1.Measure
	for _, g := range groups {
		// A region criteria needs an index rule on it, for the groups that declare the tag.
		var rules []*databasev1.IndexRule
		for _, tag := range g.schema.GetTagFamilies()[0].GetTags() {
			if tag.GetName() == "region" {
				rules = append(rules, &databasev1.IndexRule{
					Metadata: &commonv1.Metadata{Name: "region-rule", Group: g.schema.GetMetadata().GetGroup()},
					Tags:     []string{"region"}, Type: databasev1.IndexRule_TYPE_INVERTED,
				})
			}
		}
		schema, schemaErr := logicalmeasure.BuildSchema(g.schema, rules)
		if schemaErr != nil {
			t.Fatalf("BuildSchema: %v", schemaErr)
		}
		schemas = append(schemas, schema)
		measureSchemas = append(measureSchemas, g.schema)
	}
	merge, mergeErr := NewMultiGroupMerge(context.Background(), req, schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	collected := make([]*GroupBatches, 0, len(groups))
	for i, g := range groups {
		result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
			&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, true, true)
		var group *GroupBatches
		if dispatchErr == nil {
			group, dispatchErr = merge.Collect(context.Background(), result, planStr)
		}
		if dispatchErr != nil {
			for _, obtained := range collected {
				obtained.Release()
			}
			t.Fatalf("DispatchBatches: %v", dispatchErr)
		}
		collected = append(collected, group)
	}
	iter, iterErr := merge.Merge(context.Background(), collected)
	if iterErr != nil {
		if used := merge.Tracker().Used(); used != 0 {
			t.Fatalf("a failed merge must give back its reservations, %d bytes still held", used)
		}
		return nil, iterErr
	}
	var rows []string
	for iter.Next() {
		dp := iter.Current()[0].GetDataPoint()
		key := ""
		for _, family := range dp.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if slices.Contains(forbidTags, tag.GetKey()) {
					t.Fatalf("the response carries the hidden tag %q", tag.GetKey())
				}
			}
			if len(family.GetTags()) > 0 && key == "" {
				key = family.GetTags()[0].GetValue().GetStr().GetValue()
			}
		}
		if req.GetGroupBy().GetTimeBucket() != nil {
			key = fmt.Sprintf("t%d", dp.GetTimestamp().AsTime().Unix())
		}
		rows = append(rows, fmt.Sprintf("%s=%v", key, mgAggNumber(dp)))
	}
	if used := merge.Tracker().Used(); len(rows) > 0 && used == 0 {
		t.Fatal("the reduced rows must stay charged to the memory tracker until the result is closed")
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if used := merge.Tracker().Used(); used != 0 {
		t.Fatalf("the memory tracker still holds %d bytes after the result is closed", used)
	}
	return rows, nil
}

func mgAggNumber(dp *measurev1.DataPoint) float64 {
	value := dp.GetFields()[0].GetValue()
	if value.GetFloat() != nil {
		return value.GetFloat().GetValue()
	}
	return float64(value.GetInt().GetValue())
}

func mgAggRequest(function modelv1.AggregationFunction) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:   "demo",
		Groups: []string{"a", "b"},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: defaultName, Tags: []string{tagSvc}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		TimeRange:       &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(600, 0))},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc}}}},
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{Function: function, FieldName: fieldValue},
	}
}

func mgAggRow(tag string, sid, value int64) mgRowSpec {
	return mgRowSpec{tag: tag, ts: sid, sid: sid, version: 1, value: value}
}

func mgAggGroups(a, b []mgRowSpec) (*mgBatchEC, *mgBatchEC) {
	return &mgBatchEC{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{a}},
		&mgBatchEC{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{b}}
}

// TestMultiGroupAggReduce pins that an aggregation over several groups is reduced across them, as the distributed
// liaison does, instead of concatenating each group's own result. Every expected value is hand-computed.
func TestMultiGroupAggReduce(t *testing.T) {
	const second = int64(time.Second)
	sum := modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
	function := func(fn modelv1.AggregationFunction) func(*measurev1.QueryRequest) {
		return func(req *measurev1.QueryRequest) { req.Agg.Function = fn }
	}
	// 120 keys in both groups: 240 rows reduce to 120 keys of 2, of which the default limit keeps 100.
	var manyA, manyB []mgRowSpec
	var wantMany []string
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("k%03d", i)
		manyA = append(manyA, mgAggRow(key, int64(i+1), 1))
		manyB = append(manyB, mgAggRow(key, int64(i+1), 1))
		if i < 100 {
			wantMany = append(wantMany, key+"=2")
		}
	}
	oneAndTwo := func() (*mgBatchEC, *mgBatchEC) {
		return mgAggGroups([]mgRowSpec{mgAggRow("m", 1, 1), mgAggRow("m", 2, 2), mgAggRow("m", 3, 3)}, []mgRowSpec{mgAggRow("m", 4, 10)})
	}
	xy := func() (*mgBatchEC, *mgBatchEC) {
		return mgAggGroups([]mgRowSpec{mgAggRow("x", 1, 1), mgAggRow("y", 2, 2)}, []mgRowSpec{mgAggRow("x", 3, 3), mgAggRow("y", 4, 4)})
	}
	topSpec := func(number int32, field string) func(*measurev1.QueryRequest) {
		return func(req *measurev1.QueryRequest) {
			req.Top = &measurev1.QueryRequest_Top{Number: number, FieldName: field, FieldValueSort: modelv1.Sort_SORT_DESC}
		}
	}
	cases := []struct {
		groups  func() (*mgBatchEC, *mgBatchEC)
		mutate  func(*measurev1.QueryRequest)
		name    string
		wantErr string
		want    []string
	}{
		{
			// (1+2+3+10)/4: averaging the group means would give (2+10)/2 = 6.
			name: "mean over rows of both groups", groups: oneAndTwo,
			mutate: function(modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN), want: []string{"m=4"},
		},
		{
			name: "count adds the groups' counts", groups: oneAndTwo,
			mutate: function(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT), want: []string{"m=4"},
		},
		{
			name: "min takes the smallest of both groups", groups: oneAndTwo,
			mutate: function(modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN), want: []string{"m=1"},
		},
		{
			name: "max takes the largest of both groups", groups: oneAndTwo,
			mutate: function(modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX), want: []string{"m=10"},
		},
		{
			// y is 9 in both groups, so its total 18 beats x=10; a per-group Top 1 would drop y from group a.
			name: "top 1 applies to the reduced sums",
			groups: func() (*mgBatchEC, *mgBatchEC) {
				return mgAggGroups([]mgRowSpec{mgAggRow("x", 1, 10), mgAggRow("y", 2, 9)}, []mgRowSpec{mgAggRow("y", 3, 9), mgAggRow("z", 4, 8)})
			},
			mutate: topSpec(1, fieldValue), want: []string{"y=18"},
		},
		{name: "top 0 keeps no row", groups: xy, mutate: topSpec(0, fieldValue), want: nil},
		{name: "a negative top keeps no row", groups: xy, mutate: topSpec(-1, fieldValue), want: nil},
		{
			name: "top on an unknown field fails even without rows",
			groups: func() (*mgBatchEC, *mgBatchEC) {
				return mgAggGroups(nil, nil)
			},
			mutate: topSpec(1, "absent"), wantErr: `field "absent" not present`,
		},
		{
			// x=1+3; a per-group limit would return x=1 from a and x=3 from b.
			name: "limit 1 is applied once after the reduce", groups: xy,
			mutate: func(req *measurev1.QueryRequest) { req.Limit = 1 }, want: []string{"x=4"},
		},
		{
			// The reduced rows are x=4 and y=6; a per-group offset would skip a row of each group.
			name: "offset is applied once after the reduce", groups: xy,
			mutate: func(req *measurev1.QueryRequest) { req.Offset, req.Limit = 1, 1 }, want: []string{"y=6"},
		},
		{
			name: "the default limit caps the reduced rows", groups: func() (*mgBatchEC, *mgBatchEC) { return mgAggGroups(manyA, manyB) },
			want: wantMany,
		},
		{
			// Both rows fall into the 1m bucket starting at 60s: 5+18.
			name: "one time bucket spans both groups",
			groups: func() (*mgBatchEC, *mgBatchEC) {
				return mgAggGroups([]mgRowSpec{{tag: "m", ts: 61 * second, sid: 1, version: 1, value: 5}},
					[]mgRowSpec{{tag: "m", ts: 62 * second, sid: 2, version: 1, value: 18}})
			},
			mutate: func(req *measurev1.QueryRequest) {
				req.TagProjection = nil
				req.GroupBy = &measurev1.QueryRequest_GroupBy{TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "1m"}}
			},
			want: []string{"t60=23"},
		},
		{
			// Group b declares zone instead of region, so only group a is asked for the projected region: 5+18.
			name: "a projected tag is declared by one group only",
			groups: func() (*mgBatchEC, *mgBatchEC) {
				a, _ := mgAggGroups([]mgRowSpec{mgAggRow("m", 1, 5)}, nil)
				b := &mgBatchEC{schema: evolvedGroupSchema("b", []string{tagSvc, tagZone}), batches: [][]mgRowSpec{{mgAggRow("m", 2, 18)}}}
				return a, b
			},
			mutate: func(req *measurev1.QueryRequest) {
				req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: defaultName, Tags: []string{tagSvc, "region"}},
				}}
			},
			want: []string{"m=23"},
		},
		{
			// The key region is only a criteria tag, not projected: it must survive until the groups are reduced.
			name: "a grouping key that only the criteria names is not hidden",
			groups: func() (*mgBatchEC, *mgBatchEC) {
				return mgAggGroups([]mgRowSpec{mgAggRow("m", 1, 5)}, []mgRowSpec{mgAggRow("m", 2, 18)})
			},
			mutate: func(req *measurev1.QueryRequest) {
				req.GroupBy.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: defaultName, Tags: []string{"region"}},
				}}
				req.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
					Name: "region", Op: modelv1.Condition_BINARY_OP_EQ, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "m"}}},
				}}}
			},
			want: []string{"m=23"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mgAggRequest(sum)
			if tc.mutate != nil {
				tc.mutate(req)
			}
			a, b := tc.groups()
			var forbid []string
			if req.GetCriteria() != nil {
				forbid = []string{"region"}
			}
			got, err := runMultiGroupAgg(t, req, []*mgBatchEC{a, b}, forbid...)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Merge: %v", err)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("rows = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMultiGroupAggReduce_ReleasesOnFailure pins that a reduce that fails gives back its reservations and the groups.
func TestMultiGroupAggReduce_ReleasesOnFailure(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16}
	groups := []*mgBatchEC{
		{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{{mgAggRow("m", 1, 5)}}},
		{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{{mgAggRow("m", 2, 18)}}},
	}
	schemas, measureSchemas := mgTestSetup(t, groups)
	merge, mergeErr := NewMultiGroupMerge(context.Background(), mgAggRequest(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM),
		schemas, measureSchemas, cfg)
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	collected := make([]*GroupBatches, 0, len(groups))
	for i, g := range groups {
		result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
			&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, true, true)
		var group *GroupBatches
		if dispatchErr == nil {
			group, dispatchErr = merge.Collect(context.Background(), result, planStr)
		}
		if dispatchErr != nil {
			t.Fatalf("DispatchBatches: %v", dispatchErr)
		}
		collected = append(collected, group)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, mergeFailure := merge.MergeBatches(canceled, collected); mergeFailure == nil {
		t.Fatal("MergeBatches must fail on a canceled context")
	}
	if used := merge.Tracker().Used(); used != 0 {
		t.Fatalf("a failed reduce must give back its reservations, %d bytes still held", used)
	}
	for i, g := range groups {
		if g.result.released == 0 {
			t.Fatalf("group %d was not released after the failed reduce", i)
		}
	}
}

// TestAnalyzeDistributed_AggPartialsAreNotLimited pins that a data node returns every partial of an aggregation,
// like the standalone groups do: a key cut off by one node's limit would lose its contribution to the total.
func TestAnalyzeDistributed_AggPartialsAreNotLimited(t *testing.T) {
	req := mgAggRequest(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM)
	req.Limit, req.Offset = 1, 2
	p, err := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchemaForGroup("a"), testMeasureSchemaForGroup("b")}, nil,
		measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16})
	if err != nil {
		t.Fatalf("AnalyzeDistributed: %v", err)
	}
	if got := p.nodeTemplate.GetLimit(); got != math.MaxUint32 {
		t.Fatalf("node limit = %d, want unbounded", got)
	}
	if got := p.queryTemplate.GetLimit(); got != 1 {
		t.Fatalf("the liaison keeps the requested limit, got %d", got)
	}
}

// cancelAfterCtx reports context.Canceled from its n-th Err call on.
type cancelAfterCtx struct {
	context.Context
	calls, after int
}

func (c *cancelAfterCtx) Err() error {
	c.calls++
	if c.calls > c.after {
		return context.Canceled
	}
	return nil
}

// TestMultiGroupAggReduce_CancelAtEveryCheckpoint cancels the reduce at each of its context checks, including the
// last one before the result is built, and expects the tracker to end at zero: a reservation handed to the result
// is released once, by the result.
func TestMultiGroupAggReduce_CancelAtEveryCheckpoint(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16}
	for after := 0; after < 40; after++ {
		groups := []*mgBatchEC{
			{schema: testMeasureSchemaForGroup("a"), batches: [][]mgRowSpec{{mgAggRow("x", 1, 1), mgAggRow("y", 2, 2)}}},
			{schema: testMeasureSchemaForGroup("b"), batches: [][]mgRowSpec{{mgAggRow("x", 3, 3)}}},
		}
		schemas, measureSchemas := mgTestSetup(t, groups)
		req := mgAggRequest(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM)
		req.Top = &measurev1.QueryRequest_Top{Number: 1, FieldName: fieldValue, FieldValueSort: modelv1.Sort_SORT_DESC}
		merge, mergeErr := NewMultiGroupMerge(context.Background(), req, schemas, measureSchemas, cfg)
		if mergeErr != nil {
			t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
		}
		collected := make([]*GroupBatches, 0, len(groups))
		for i, g := range groups {
			result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
				&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, true, true)
			var group *GroupBatches
			if dispatchErr == nil {
				group, dispatchErr = merge.Collect(context.Background(), result, planStr)
			}
			if dispatchErr != nil {
				t.Fatalf("DispatchBatches: %v", dispatchErr)
			}
			collected = append(collected, group)
		}
		result, err := merge.MergeBatches(&cancelAfterCtx{Context: context.Background(), after: after}, collected)
		if err == nil {
			if closeErr := result.Close(); closeErr != nil {
				t.Fatalf("after %d checks: Close: %v", after, closeErr)
			}
		}
		if used := merge.Tracker().Used(); used != 0 {
			t.Fatalf("canceled after %d checks (err=%v): the tracker holds %d bytes", after, err, used)
		}
	}
}

// TestMultiGroupAggConcat_DataNodeKeepsPartials pins the data-node side: a node that emits partials asks for
// ConcatAggregates, so its groups keep the request's Top and limit and their partials are handed over as they are,
// one row per group for a key both groups hold, for the liaison to reduce.
func TestMultiGroupAggConcat_DataNodeKeepsPartials(t *testing.T) {
	cfg := measure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16}
	a, b := mgAggGroups([]mgRowSpec{mgAggRow("m", 1, 5)}, []mgRowSpec{mgAggRow("m", 2, 18)})
	groups := []*mgBatchEC{a, b}
	schemas, measureSchemas := mgTestSetup(t, groups)
	req := mgAggRequest(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM)
	req.Limit = 7
	req.Top = &measurev1.QueryRequest_Top{Number: 3, FieldName: fieldValue, FieldValueSort: modelv1.Sort_SORT_DESC}
	merge, mergeErr := NewMultiGroupMerge(context.Background(), req, schemas, measureSchemas, cfg, ConcatAggregates())
	if mergeErr != nil {
		t.Fatalf("NewMultiGroupMerge: %v", mergeErr)
	}
	if merge.ReducesAggregates() {
		t.Fatal("a data node must not reduce its groups' partials")
	}
	if merge.GroupRequest() != req {
		t.Fatal("a data node dispatches every group with the request it received")
	}
	collected := make([]*GroupBatches, 0, len(groups))
	for i, g := range groups {
		result, planStr, dispatchErr := DispatchBatches(context.Background(), merge.GroupRequest(),
			&commonv1.Metadata{Name: "demo", Group: g.schema.GetMetadata().GetGroup()}, g.schema, schemas[i], g, cfg, true, true)
		var group *GroupBatches
		if dispatchErr == nil {
			group, dispatchErr = merge.Collect(context.Background(), result, planStr)
		}
		if dispatchErr != nil {
			t.Fatalf("DispatchBatches: %v", dispatchErr)
		}
		collected = append(collected, group)
	}
	result, err := merge.MergeBatches(context.Background(), collected)
	if err != nil {
		t.Fatalf("MergeBatches: %v", err)
	}
	rows := 0
	for {
		batch, nextErr := result.NextBatch(context.Background())
		if nextErr != nil {
			t.Fatalf("NextBatch: %v", nextErr)
		}
		if batch == nil {
			break
		}
		rows += batch.ActiveLen()
	}
	if closeErr := result.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if rows != 2 {
		t.Fatalf("got %d partial rows, want one per group (2)", rows)
	}
}

// reducedRows builds a reduced batch of (svc, value) rows.
func reducedRows(t *testing.T, svcs []string, values []int64) *vectorized.RecordBatch {
	t.Helper()
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: defaultName, Name: tagSvc, Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
	})
	batch := vectorized.NewRecordBatch(schema, len(svcs))
	for i := range svcs {
		if appendErr := appendCell(batch.Columns[0], svcs[i]); appendErr != nil {
			t.Fatal(appendErr)
		}
		if appendErr := appendCell(batch.Columns[1], values[i]); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	batch.Len = len(svcs)
	return batch
}

// TestTopPeakBytes pins the bound Top's reservation uses: the cells of the n largest rows, retained and emitted,
// plus a header per retained row and cell.
func TestTopPeakBytes(t *testing.T) {
	batches := []*vectorized.RecordBatch{
		reducedRows(t, []string{"a", strings.Repeat("b", 100)}, []int64{1, 2}),
		reducedRows(t, []string{strings.Repeat("c", 10)}, []int64{3}),
	}
	// Row cells: a=16+1+8, b=16+100+8, c=16+10+8.
	rowHeader := int64(topRowOverheadBytes + 2*topCellOverheadBytes)
	if got, want := topPeakBytes(batches, 1), 2*int64(124)+rowHeader; got != want {
		t.Fatalf("Top 1 = %d, want %d: the largest row, retained and emitted", got, want)
	}
	if got, want := topPeakBytes(batches, 10), 2*int64(25+124+34)+3*rowHeader; got != want {
		t.Fatalf("Top 10 over 3 rows = %d, want %d: every row", got, want)
	}
}

// TestMultiGroupAggReduce_TopCopiesAreCharged pins that Top's copies of the rows it keeps are reserved before it runs:
// a budget that holds the reduced rows but not those copies fails the query and gives everything back.
func TestMultiGroupAggReduce_TopCopiesAreCharged(t *testing.T) {
	var svcs []string
	var values []int64
	for i := 0; i < 64; i++ {
		svcs = append(svcs, fmt.Sprintf("%s%03d", strings.Repeat("s", 200), i))
		values = append(values, int64(i))
	}
	batches := []*vectorized.RecordBatch{reducedRows(t, svcs, values)}
	reduced := reduceRowsBytes(batches)
	top := measure.ReduceTopSpec{FieldName: fieldValue, N: 64}
	tracker := vectorized.NewMemoryTracker(reduced + topPeakBytes(batches, top.N) - 1)
	merge := &MultiGroupMerge{tracker: tracker, cfg: measure.VectorizedConfig{BatchSize: 16}}
	hold := &reduceHold{tracker: tracker}
	if reserveErr := hold.reserve(reduced); reserveErr != nil {
		t.Fatalf("the reduced rows fit the budget: %v", reserveErr)
	}
	if _, topErr := merge.topReduced(context.Background(), top, batches, hold); !errors.Is(topErr, vectorized.ErrMemoryBudgetExceeded) {
		t.Fatalf("topReduced = %v, want the memory budget error", topErr)
	}
	hold.release(hold.held)
	if used := tracker.Used(); used != 0 {
		t.Fatalf("%d bytes still held after the failed Top", used)
	}
}

// TestMultiGroupAggReduce_RejectsTagDeclaredAsAnotherType pins that a standalone node, like the liaison, rejects an
// aggregation whose groups declare the same projected tag with different types (apache/skywalking#14069).
func TestMultiGroupAggReduce_RejectsTagDeclaredAsAnotherType(t *testing.T) {
	groups := []*mgBatchEC{
		{schema: typedTagMeasure("a", databasev1.TagType_TAG_TYPE_STRING)},
		{schema: typedTagMeasure("b", databasev1.TagType_TAG_TYPE_INT)},
	}
	req := sumBySvcRequest("a")
	req.Groups = []string{"a", "b"}
	req.TimeRange = &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(600, 0))}
	if _, err := runMultiGroupAgg(t, req, groups); err == nil || !strings.Contains(err.Error(), "zone") {
		t.Fatalf("error = %v, want one naming zone", err)
	}
}
