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

package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

const (
	mgGroupA    = "group-a"
	mgGroupB    = "group-b"
	mgFamily    = "default"
	mgTagSvc    = "svc"
	mgTagRegion = "region"
	mgTagExtra  = "extra"
	mgField     = "value"
	mgIndexRule = "svc-rule"
	mgTagLabels = "labels"
)

// mgRow is one stored row: the (ts, SID, version, value) tuple of the issue's oracle plus its tags.
type mgRow struct {
	tags    map[string]string
	arrays  map[string][]string
	ts      int64
	sid     uint64
	version int64
	value   int64
}

// mgGroup is a fake storage group: it serves its rows in the given storage batches.
type mgGroup struct {
	onEOF     func()
	queryErr  error
	schema    *databasev1.Measure
	result    *mgResult
	batches   [][]mgRow
	pullErrAt int
}

func (g *mgGroup) Query(_ context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	if g.queryErr != nil {
		return nil, g.queryErr
	}
	schema, schemaErr := vmeasure.BuildBatchSchema(g.schema, opts)
	if schemaErr != nil {
		return nil, schemaErr
	}
	g.result = &mgResult{pullErrAt: g.pullErrAt, onEOF: g.onEOF}
	for _, rows := range g.batches {
		g.result.batches = append(g.result.batches, buildMgBatch(schema, rows))
	}
	return g.result, nil
}

func (g *mgGroup) VectorizedConfig() vmeasure.VectorizedConfig {
	// One row per batch, so a group's rows reach the merge as several batches.
	return vmeasure.VectorizedConfig{BatchSize: 1, QueryMemoryMiB: 16}
}

func (g *mgGroup) GetSchema() *databasev1.Measure { return g.schema }

var _ vecExecutionContext = (*mgGroup)(nil)

// mgResult is a MeasureBatchResult over prebuilt storage batches that counts Release calls.
type mgResult struct {
	onEOF     func()
	batches   []*model.MeasureBatch
	pullErrAt int
	pulled    int
	released  int
}

func (r *mgResult) Pull() *model.MeasureResult { return nil }

func (r *mgResult) PullBatch(context.Context) (*model.MeasureBatch, error) {
	if r.pullErrAt > 0 && r.pulled+1 == r.pullErrAt {
		return nil, errors.New("storage read failed")
	}
	if r.pulled >= len(r.batches) {
		if r.onEOF != nil {
			r.onEOF()
			r.onEOF = nil
		}
		return nil, nil
	}
	batch := r.batches[r.pulled]
	r.pulled++
	return batch, nil
}

func (r *mgResult) Release() { r.released++ }

func buildMgBatch(schema *vectorized.BatchSchema, rows []mgRow) *model.MeasureBatch {
	batch := &model.MeasureBatch{Schema: schema}
	for _, row := range rows {
		batch.Timestamps = append(batch.Timestamps, row.ts)
		batch.Versions = append(batch.Versions, row.version)
		batch.ShardIDs = append(batch.ShardIDs, 0)
		batch.SeriesIDs = append(batch.SeriesIDs, common.SeriesID(row.sid))
	}
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := vectorized.NewColumnForType(def.Type, len(rows))
			for _, row := range rows {
				if arrCol, isArr := col.(*vectorized.TypedColumn[[]string]); isArr {
					if values, has := row.arrays[def.Name]; has {
						arrCol.Append(values)
					} else {
						col.AppendNull()
					}
					continue
				}
				value, ok := row.tags[def.Name]
				strCol, isStr := col.(*vectorized.TypedColumn[string])
				if !ok || !isStr {
					col.AppendNull()
					continue
				}
				strCol.Append(value)
			}
			batch.Tags = append(batch.Tags, col)
		case vectorized.RoleField:
			col := vectorized.NewColumnForType(def.Type, len(rows))
			for _, row := range rows {
				col.(*vectorized.TypedColumn[int64]).Append(row.value)
			}
			batch.Fields = append(batch.Fields, col)
		case vectorized.RoleTimestamp, vectorized.RoleVersion, vectorized.RoleSeriesID,
			vectorized.RoleShardID, vectorized.RoleElementID, vectorized.RoleOrderKey:
		}
	}
	return batch
}

func mgMeasure(group string, tags ...string) *databasev1.Measure {
	specs := make([]*databasev1.TagSpec, 0, len(tags))
	for _, tag := range tags {
		tagType := databasev1.TagType_TAG_TYPE_STRING
		if tag == mgTagLabels {
			tagType = databasev1.TagType_TAG_TYPE_STRING_ARRAY
		}
		specs = append(specs, &databasev1.TagSpec{Name: tag, Type: tagType})
	}
	return &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "m", Group: group},
		Entity:      &databasev1.Entity{TagNames: []string{mgTagSvc}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: mgFamily, Tags: specs}},
		Fields:      []*databasev1.FieldSpec{{Name: mgField, FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

func mgContext(t *testing.T, groups ...*mgGroup) *measureExecutionContext {
	t.Helper()
	rules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: mgIndexRule, Group: mgGroupA},
			Tags:     []string{mgTagSvc},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
		{
			Metadata: &commonv1.Metadata{Name: "labels-rule", Group: mgGroupA},
			Tags:     []string{mgTagLabels},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
		{
			Metadata: &commonv1.Metadata{Name: "region-rule", Group: mgGroupA},
			Tags:     []string{mgTagRegion},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	}
	mctx := &measureExecutionContext{}
	for _, g := range groups {
		schema, schemaErr := logical_measure.BuildSchema(g.schema, rules)
		if schemaErr != nil {
			t.Fatalf("BuildSchema: %v", schemaErr)
		}
		mctx.ecc = append(mctx.ecc, g)
		mctx.schemas = append(mctx.schemas, schema)
		mctx.metadata = append(mctx.metadata, g.schema.GetMetadata())
	}
	return mctx
}

// mgRequest projects svc and extra and filters on region, a criteria tag outside the projection.
func mgRequest(sort modelv1.Sort, indexRule string, offset, limit uint32) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:   "m",
		Groups: []string{mgGroupA, mgGroupB},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Unix(0, 0)),
			End:   timestamppb.New(time.Unix(1, 0)),
		},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: mgFamily, Tags: []string{mgTagSvc, mgTagExtra}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{mgField}},
		Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  mgTagRegion,
			Op:    modelv1.Condition_BINARY_OP_EQ,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "r"}}},
		}}},
		OrderBy: &modelv1.QueryOrder{Sort: sort, IndexRuleName: indexRule},
		Offset:  offset,
		Limit:   limit,
	}
}

// mgOracleGroups builds the issue's oracle: duplicate versions split across batches and groups.
// Group B's second batch holds the lower version of the same (ts, SID), which must lose to version 3.
func mgOracleGroups() (*mgGroup, *mgGroup) {
	groupA := &mgGroup{
		schema: mgMeasure(mgGroupA, mgTagSvc, mgTagRegion),
		batches: [][]mgRow{
			{{ts: 10, sid: 7, version: 1, value: 100, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
			{{ts: 20, sid: 8, version: 1, value: 200, tags: map[string]string{mgTagSvc: "y", mgTagRegion: "r"}}},
		},
	}
	groupB := &mgGroup{
		schema: mgMeasure(mgGroupB, mgTagSvc, mgTagRegion, mgTagExtra),
		batches: [][]mgRow{
			{{ts: 10, sid: 7, version: 3, value: 300, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r", mgTagExtra: "e"}}},
			{{ts: 10, sid: 7, version: 2, value: 250, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r", mgTagExtra: "e"}}},
		},
	}
	return groupA, groupB
}

// mgDispatch runs dispatchMeasure and turns a panic inside it into a test failure, so a
// regression reads as a failed assertion rather than aborting the whole test binary.
func mgDispatch(ctx context.Context, t *testing.T, req *measurev1.QueryRequest, mctx *measureExecutionContext,
) (iter executor.MIterator, planStr string, err error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("dispatchMeasure panicked: %v", recovered)
		}
	}()
	return dispatchMeasure(ctx, req, mctx, false)
}

func drainMgRows(t *testing.T, iter executor.MIterator) ([]string, []*measurev1.DataPoint) {
	t.Helper()
	var rows []string
	var points []*measurev1.DataPoint
	for iter.Next() {
		for _, idp := range iter.Current() {
			dp := idp.GetDataPoint()
			rows = append(rows, fmt.Sprintf("(%d,%d,%d,%d)", dp.GetTimestamp().AsTime().UnixNano(), dp.GetSid(), dp.GetVersion(),
				dp.GetFields()[0].GetValue().GetInt().GetValue()))
			points = append(points, dp)
		}
	}
	return rows, points
}

func TestDispatchMeasureMultiGroupBatchMerge(t *testing.T) {
	oracleAsc := []string{"(10,7,3,300)", "(20,8,1,200)"}
	oracleDesc := []string{"(20,8,1,200)", "(10,7,3,300)"}
	tests := []struct {
		name      string
		indexRule string
		want      []string
		sort      modelv1.Sort
		offset    uint32
		limit     uint32
	}{
		{name: "time ascending", sort: modelv1.Sort_SORT_ASC, limit: 10, want: oracleAsc},
		{name: "time descending", sort: modelv1.Sort_SORT_DESC, limit: 10, want: oracleDesc},
		{name: "time ascending offset 1 limit 1", sort: modelv1.Sort_SORT_ASC, offset: 1, limit: 1, want: []string{"(20,8,1,200)"}},
		{name: "index order ascending", indexRule: mgIndexRule, sort: modelv1.Sort_SORT_ASC, limit: 10, want: oracleAsc},
		{name: "index order descending", indexRule: mgIndexRule, sort: modelv1.Sort_SORT_DESC, limit: 10, want: oracleDesc},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupA, groupB := mgOracleGroups()
			mctx := mgContext(t, groupA, groupB)
			iter, planStr, dispatchErr := mgDispatch(context.Background(), t, mgRequest(tt.sort, tt.indexRule, tt.offset, tt.limit), mctx)
			if dispatchErr != nil {
				t.Fatalf("dispatchMeasure: %v", dispatchErr)
			}
			if !strings.HasPrefix(planStr, "vec-multi-measure-batch[") {
				t.Fatalf("plan %q must name the batch merge, vec-multi-measure-batch[...]", planStr)
			}
			rows, points := drainMgRows(t, iter)
			if closeErr := iter.Close(); closeErr != nil {
				t.Fatalf("Close: %v", closeErr)
			}
			if got, want := strings.Join(rows, " "), strings.Join(tt.want, " "); got != want {
				t.Fatalf("rows = %s, want %s", got, want)
			}
			for _, dp := range points {
				for _, family := range dp.GetTagFamilies() {
					for _, tag := range family.GetTags() {
						if tag.GetKey() == mgTagRegion {
							t.Fatalf("hidden criteria tag %q leaked into %v", mgTagRegion, dp)
						}
					}
				}
			}
			if groupA.result.released == 0 || groupB.result.released == 0 {
				t.Fatalf("closing the merged iterator must release both groups, released a=%d b=%d", groupA.result.released, groupB.result.released)
			}
			// (20,8) comes from group A alone, which does not define the extra tag.
			for _, dp := range points {
				if dp.GetSid() != 8 {
					continue
				}
				for _, family := range dp.GetTagFamilies() {
					for _, tag := range family.GetTags() {
						if tag.GetKey() == mgTagExtra && tag.GetValue().GetStr() != nil {
							t.Fatalf("tag %q only group B defines must be null on group A rows, got %v", mgTagExtra, tag.GetValue())
						}
					}
				}
			}
		})
	}

	t.Run("tag only group B defines is returned for its rows", testMultiGroupExtraTag)
	t.Run("a group whose dispatch fails releases the batches already read", testMultiGroupDispatchFailure)
	t.Run("a batch read failure releases every group", testMultiGroupReadFailure)
	t.Run("a canceled context releases every group", testMultiGroupCanceled)
	t.Run("a context canceled after the groups were read stops the merge", testMultiGroupCanceledDuringMerge)
	t.Run("Top keeps cross-group order and version dedup", testMultiGroupTop)
	t.Run("null index values form their own sort window", testMultiGroupNullSortKey)
	t.Run("array index values order the merge", testMultiGroupArraySortKey)
	t.Run("null and empty index values stay distinct windows", testMultiGroupNullAndEmptySortKey)
	t.Run("an empty passthrough value sorts after a null one", testMultiGroupEmptyVersusNullPassthrough)
	t.Run("raw GroupBy keeps cross-group version dedup", testMultiGroupRawGroupBy)
	t.Run("Agg results are concatenated per group", testMultiGroupAgg)
	t.Run("time-bucketed Agg results are merged by time", testMultiGroupBucketedAgg)
	t.Run("the merged result emits a raw frame of the merged batches", testMultiGroupFrame)
}

func testMultiGroupExtraTag(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	iter, _, dispatchErr := mgDispatch(context.Background(), t, mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10), mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	_, points := drainMgRows(t, iter)
	if len(points) == 0 {
		t.Fatal("no rows")
	}
	var extra string
	for _, family := range points[0].GetTagFamilies() {
		for _, tag := range family.GetTags() {
			if tag.GetKey() == mgTagExtra {
				extra = tag.GetValue().GetStr().GetValue()
			}
		}
	}
	if extra != "e" {
		t.Fatalf("row (10,7,3,300) comes from group B and must carry extra=e, got %q", extra)
	}
}

func testMultiGroupDispatchFailure(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	groupB.queryErr = errors.New("group b unavailable")
	iter, _, dispatchErr := mgDispatch(context.Background(), t, mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10), mgContext(t, groupA, groupB))
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "group b unavailable") {
		t.Fatalf("want the group b error, got iter=%v err=%v", iter, dispatchErr)
	}
	if groupA.result.released == 0 {
		t.Fatal("group a was read before group b failed and must be released")
	}
}

func testMultiGroupReadFailure(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	groupB.pullErrAt = 2
	iter, _, dispatchErr := mgDispatch(context.Background(), t, mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10), mgContext(t, groupA, groupB))
	if dispatchErr == nil || !strings.Contains(dispatchErr.Error(), "storage read failed") {
		t.Fatalf("want the storage read error, got iter=%v err=%v", iter, dispatchErr)
	}
	if groupA.result.released == 0 || groupB.result.released == 0 {
		t.Fatalf("every group must be released, released a=%d b=%d", groupA.result.released, groupB.result.released)
	}
}

func testMultiGroupCanceled(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	iter, _, dispatchErr := mgDispatch(ctx, t, mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10), mgContext(t, groupA, groupB))
	if !errors.Is(dispatchErr, context.Canceled) {
		t.Fatalf("want context.Canceled, got iter=%v err=%v", iter, dispatchErr)
	}
	if groupA.result.released == 0 {
		t.Fatal("group a must be released")
	}
}

func testMultiGroupCanceledDuringMerge(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel once the last group has been read, so only the merge phase can notice it.
	groupB.onEOF = cancel
	iter, _, dispatchErr := mgDispatch(ctx, t, mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10), mgContext(t, groupA, groupB))
	if !errors.Is(dispatchErr, context.Canceled) {
		t.Fatalf("want context.Canceled from the merge phase, got iter=%v err=%v", iter, dispatchErr)
	}
	if groupA.result.released == 0 || groupB.result.released == 0 {
		t.Fatalf("every group must be released, released a=%d b=%d", groupA.result.released, groupB.result.released)
	}
}

func testMultiGroupTop(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10)
	req.Top = &measurev1.QueryRequest_Top{Number: 10, FieldName: mgField, FieldValueSort: modelv1.Sort_SORT_DESC}
	iter, planStr, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	if !strings.HasPrefix(planStr, "vec-multi-measure-batch[") {
		t.Fatalf("plan %q must name the batch merge", planStr)
	}
	rows, _ := drainMgRows(t, iter)
	// Group B's Top returns both versions of (10,7); the merge keeps version 3 and orders by time.
	if got, want := strings.Join(rows, " "), "(10,7,3,300) (20,8,1,200)"; got != want {
		t.Fatalf("rows = %s, want %s", got, want)
	}
}

// mgNullKeyGroups adds a row without an svc value, which sorts before every non-null value.
func mgNullKeyGroups() (*mgGroup, *mgGroup) {
	groupA, groupB := mgOracleGroups()
	groupB.batches = append(groupB.batches, []mgRow{
		{ts: 30, sid: 9, version: 1, value: 900, tags: map[string]string{mgTagRegion: "r"}},
	})
	return groupA, groupB
}

func testMultiGroupNullSortKey(t *testing.T) {
	groupA, groupB := mgNullKeyGroups()
	iter, _, dispatchErr := mgDispatch(context.Background(), t, mgRequest(modelv1.Sort_SORT_ASC, mgIndexRule, 0, 10), mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	rows, _ := drainMgRows(t, iter)
	// The null svc row (30,9) is its own window and comes first in ascending order.
	if got, want := strings.Join(rows, " "), "(30,9,1,900) (10,7,3,300) (20,8,1,200)"; got != want {
		t.Fatalf("rows = %s, want %s", got, want)
	}
}

func testMultiGroupArraySortKey(t *testing.T) {
	newGroup := func(group string, rows ...mgRow) *mgGroup {
		batches := make([][]mgRow, 0, len(rows))
		for _, row := range rows {
			batches = append(batches, []mgRow{row})
		}
		return &mgGroup{schema: mgMeasure(group, mgTagSvc, mgTagRegion, mgTagLabels), batches: batches}
	}
	groupA := newGroup(mgGroupA,
		mgRow{ts: 10, sid: 7, version: 1, value: 100, tags: map[string]string{mgTagRegion: "r"}, arrays: map[string][]string{mgTagLabels: {"b"}}},
		mgRow{ts: 20, sid: 8, version: 1, value: 200, tags: map[string]string{mgTagRegion: "r"}, arrays: map[string][]string{mgTagLabels: {"a"}}})
	groupB := newGroup(mgGroupB,
		mgRow{ts: 30, sid: 9, version: 1, value: 300, tags: map[string]string{mgTagRegion: "r"}, arrays: map[string][]string{mgTagLabels: {"c"}}})
	req := mgRequest(modelv1.Sort_SORT_ASC, "labels-rule", 0, 10)
	// labels is not projected, so it is a hidden order-by column.
	req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: mgFamily, Tags: []string{mgTagSvc}}}}
	iter, _, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	rows, _ := drainMgRows(t, iter)
	if got, want := strings.Join(rows, " "), "(20,8,1,200) (10,7,1,100) (30,9,1,300)"; got != want {
		t.Fatalf("rows = %s, want %s", got, want)
	}
}

func testMultiGroupNullAndEmptySortKey(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	groupA.batches = [][]mgRow{
		{{ts: 10, sid: 7, version: 1, value: 100, tags: map[string]string{mgTagRegion: "r"}}},
		{{ts: 20, sid: 9, version: 1, value: 200, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
	}
	groupB.batches = [][]mgRow{
		{{ts: 10, sid: 7, version: 3, value: 300, tags: map[string]string{mgTagRegion: "r"}}},
		{{ts: 30, sid: 8, version: 1, value: 800, tags: map[string]string{mgTagSvc: "", mgTagRegion: "r"}}},
	}
	iter, _, dispatchErr := mgDispatch(context.Background(), t, mgRequest(modelv1.Sort_SORT_ASC, mgIndexRule, 0, 10), mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	rows, _ := drainMgRows(t, iter)
	// null < empty < "x"; the two null rows are one window, so version 3 replaces version 1.
	if got, want := strings.Join(rows, " "), "(10,7,3,300) (30,8,1,800) (20,9,1,200)"; got != want {
		t.Fatalf("rows = %s, want %s", got, want)
	}
}

func testMultiGroupRawGroupBy(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10)
	req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: mgFamily, Tags: []string{mgTagSvc}},
	}}, FieldName: mgField}
	iter, planStr, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	if !strings.HasPrefix(planStr, "vec-multi-measure-batch[") {
		t.Fatalf("plan %q must name the batch merge", planStr)
	}
	rows, _ := drainMgRows(t, iter)
	// Each group keeps its first row per svc; the merge then keeps version 3 of (10,7).
	if got, want := strings.Join(rows, " "), "(10,7,3,300) (20,8,1,200)"; got != want {
		t.Fatalf("rows = %s, want %s", got, want)
	}
}

func mgFieldNumber(dp *measurev1.DataPoint) float64 {
	value := dp.GetFields()[0].GetValue()
	if value.GetFloat() != nil {
		return value.GetFloat().GetValue()
	}
	return float64(value.GetInt().GetValue())
}

func testMultiGroupAgg(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10)
	req.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: mgField}
	iter, _, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	var sums []float64
	for iter.Next() {
		sums = append(sums, mgFieldNumber(iter.Current()[0].GetDataPoint()))
	}
	// One partial sum per group, in group order: A = 100+200, B = 300+250. Reducing them across
	// groups is #14069.
	if len(sums) != 2 || sums[0] != 300 || sums[1] != 550 {
		t.Fatalf("sums = %v, want [300 550]", sums)
	}
}

func testMultiGroupBucketedAgg(t *testing.T) {
	const second = int64(time.Second)
	groupA, groupB := mgOracleGroups()
	groupA.batches = [][]mgRow{
		{{ts: 2 * second, sid: 7, version: 1, value: 20, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
		{{ts: 4 * second, sid: 7, version: 1, value: 40, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
	}
	groupB.batches = [][]mgRow{
		{{ts: 1 * second, sid: 7, version: 1, value: 10, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
		{{ts: 3 * second, sid: 7, version: 1, value: 30, tags: map[string]string{mgTagSvc: "x", mgTagRegion: "r"}}},
	}
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10)
	req.Criteria = nil
	req.TimeRange = &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(10, 0))}
	req.GroupBy = &measurev1.QueryRequest_GroupBy{TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "1s"}}
	req.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: mgField}
	iter, _, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	var buckets []string
	for iter.Next() {
		dp := iter.Current()[0].GetDataPoint()
		buckets = append(buckets, fmt.Sprintf("%d:%v", dp.GetTimestamp().AsTime().Unix(), mgFieldNumber(dp)))
	}
	if got, want := strings.Join(buckets, " "), "1:10 2:20 3:30 4:40"; got != want {
		t.Fatalf("buckets = %s, want %s (ordered by time across groups)", got, want)
	}
}

func testMultiGroupFrame(t *testing.T) {
	groupA, groupB := mgOracleGroups()
	req := mgRequest(modelv1.Sort_SORT_ASC, "", 0, 10)
	req.Criteria = nil
	iter, _, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	// The batch merge ends in a vectorized pipeline over the merged batches; a result built from
	// rows would have neither capability.
	if _, ok := iter.(vmeasure.RawFrameSource); !ok {
		t.Fatalf("%T must expose the merged pipeline as a RawFrameSource", iter)
	}
	emitter, ok := iter.(vmeasure.FrameEmitter)
	if !ok {
		t.Fatalf("%T must emit a raw frame", iter)
	}
	body, emitErr := emitter.EmitFrame(context.Background())
	if emitErr != nil {
		t.Fatalf("EmitFrame: %v", emitErr)
	}
	batch, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		t.Fatalf("Decode: %v", decodeErr)
	}
	defer batch.ReleaseOwners()
	if batch.ActiveLen() != 2 {
		t.Fatalf("frame rows = %d, want 2", batch.ActiveLen())
	}
	timestamps := batch.Columns[batch.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()
	if timestamps[0] != 10 || timestamps[1] != 20 {
		t.Fatalf("frame timestamps = %v, want [10 20]", timestamps[:2])
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if groupA.result.released == 0 || groupB.result.released == 0 {
		t.Fatalf("closing the frame source must release both groups, released a=%d b=%d", groupA.result.released, groupB.result.released)
	}
}

// testMultiGroupEmptyVersusNullPassthrough makes svc a string in group A and an int in group B, so
// the merged column is a passthrough TagValue whose encoder returns no bytes for an empty string.
// Group A's empty string must still sort after group B's null.
func testMultiGroupEmptyVersusNullPassthrough(t *testing.T) {
	groupA := &mgGroup{
		schema: mgMeasure(mgGroupA, mgTagSvc, mgTagRegion),
		batches: [][]mgRow{
			{{ts: 10, sid: 1, version: 1, value: 100, tags: map[string]string{mgTagSvc: "", mgTagRegion: "r"}}},
		},
	}
	schemaB := mgMeasure(mgGroupB, mgTagSvc, mgTagRegion)
	schemaB.TagFamilies[0].Tags[0].Type = databasev1.TagType_TAG_TYPE_INT
	groupB := &mgGroup{
		schema: schemaB,
		batches: [][]mgRow{
			{{ts: 20, sid: 2, version: 1, value: 200, tags: map[string]string{mgTagRegion: "r"}}},
		},
	}
	req := mgRequest(modelv1.Sort_SORT_ASC, mgIndexRule, 0, 1)
	req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: mgFamily, Tags: []string{mgTagSvc}}}}
	iter, _, dispatchErr := mgDispatch(context.Background(), t, req, mgContext(t, groupA, groupB))
	if dispatchErr != nil {
		t.Fatalf("dispatchMeasure: %v", dispatchErr)
	}
	defer iter.Close()
	rows, _ := drainMgRows(t, iter)
	if got, want := strings.Join(rows, " "), "(20,2,1,200)"; got != want {
		t.Fatalf("rows = %s, want %s (the null svc row first)", got, want)
	}
}
