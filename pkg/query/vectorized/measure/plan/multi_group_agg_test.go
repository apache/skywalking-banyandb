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
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const (
	tagZone   = "zone"
	tagRegion = "region"
)

// evolvedNodeRow is one stored data point. tags only holds the tags the
// producing group's schema defines.
type evolvedNodeRow struct {
	tags  map[string]string
	value int64
}

func nodeRow(value int64, tags ...string) evolvedNodeRow {
	r := evolvedNodeRow{tags: map[string]string{}, value: value}
	for i := 0; i+1 < len(tags); i += 2 {
		r.tags[tags[i]] = tags[i+1]
	}
	return r
}

// schemaEvolvedNodes plays the data nodes of a multi-group query. Each
// broadcast is answered with the AggModeMap partial a node would emit for
// the group named in the broadcast request, built from the request it
// actually received.
type schemaEvolvedNodes struct {
	schemas     map[string]*databasev1.Measure
	rows        map[string][]evolvedNodeRow
	projections map[string][]string
}

// Scatter implements executor.DistributedExecutionContext; the measure plan only broadcasts.
func (n *schemaEvolvedNodes) Scatter(context.Context, bus.Topic, bus.Message, bus.ScatterOptions) ([]bus.NodeStream, error) {
	return nil, errors.New("scatter is not used by the measure plan")
}

func (n *schemaEvolvedNodes) Broadcast(_ time.Duration, _ bus.Topic, message bus.Message) ([]bus.Future, error) {
	req := message.Data().(*measurev1.InternalQueryRequest).GetRequest()
	group := req.GetGroups()[0]
	var projected []string
	for _, family := range req.GetTagProjection().GetTagFamilies() {
		projected = append(projected, family.GetTags()...)
	}
	n.projections[group] = projected
	body, encodeErr := n.encodePartial(req, group)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return []bus.Future{rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{RawFrameBody: body})}}, nil
}

// encodePartial runs the data node's own AggModeMap plan for req over the
// group's rows, with the storage scan swapped for an in-memory source.
func (n *schemaEvolvedNodes) encodePartial(req *measurev1.QueryRequest, group string) ([]byte, error) {
	nodePlan, analyzeErr := Analyze(req, n.schemas[group], vmeasure.AggModeMap, nil)
	if analyzeErr != nil {
		return nil, analyzeErr
	}
	scan := findScan(nodePlan)
	if scan == nil {
		return nil, fmt.Errorf("group %s: node plan has no scan: %s", group, nodePlan)
	}
	raw, rawErr := scanBatch(scan.BatchSchema, n.rows[group])
	if rawErr != nil {
		return nil, fmt.Errorf("group %s: %w", group, rawErr)
	}
	scan.Source = &fakePullSource{schema: scan.BatchSchema, batches: []*vectorized.RecordBatch{raw}}
	ctx := context.Background()
	tracker := vectorized.NewMemoryTracker(1 << 30)
	bc := &BuildContext{
		Builder: vectorized.NewPipelineBuilder().WithMemoryTracker(tracker),
		Tracker: tracker,
		Config:  vmeasure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16},
	}
	if buildErr := nodePlan.Build(ctx, bc); buildErr != nil {
		return nil, buildErr
	}
	pipeline, pipelineErr := bc.Builder.Build()
	if pipelineErr != nil {
		return nil, pipelineErr
	}
	defer pipeline.Close()
	if initErr := pipeline.Init(ctx); initErr != nil {
		return nil, initErr
	}
	return vmeasure.DrainPipelineToFrame(ctx, pipeline, nodePlan.Schema())
}

func findScan(p VecPlan) *Scan {
	if s, ok := p.(*Scan); ok {
		return s
	}
	for _, child := range p.Children() {
		if s := findScan(child); s != nil {
			return s
		}
	}
	return nil
}

// scanBatch lays rows out the way storage fills the scan schema: a tag the
// row doesn't carry is null. Every row sits on shard 0, so all groups
// report the same shard id.
func scanBatch(schema *vectorized.BatchSchema, rows []evolvedNodeRow) (*vectorized.RecordBatch, error) {
	raw := vectorized.NewRecordBatch(schema, len(rows))
	for i, def := range schema.Columns {
		for r, row := range rows {
			var cell any = int64(0)
			switch def.Role {
			case vectorized.RoleTag:
				cell = nil
				if v, ok := row.tags[def.Name]; ok {
					cell = v
				}
			case vectorized.RoleField:
				cell = row.value
			case vectorized.RoleSeriesID:
				cell = int64(r + 1)
			default:
				// Timestamp, version and shard id stay 0.
			}
			if appendErr := appendCell(raw.Columns[i], cell); appendErr != nil {
				return nil, fmt.Errorf("column %s: %w", def.Name, appendErr)
			}
		}
	}
	raw.Len = len(rows)
	return raw, nil
}

func appendCell(col vectorized.Column, cell any) error {
	if cell == nil {
		col.AppendNull()
		return nil
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		c.Append(cell.(int64))
	case *vectorized.TypedColumn[string]:
		c.Append(cell.(string))
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		c.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: cell.(string)}}})
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		c.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: cell.(int64)}}})
	default:
		return fmt.Errorf("unexpected column %T", col)
	}
	return nil
}

func (n *schemaEvolvedNodes) TimeRange() *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(3600, 0))}
}

func (n *schemaEvolvedNodes) NodeSelectors() map[string][]string { return nil }

func evolvedGroupSchema(group string, tagNames []string) *databasev1.Measure {
	tags := make([]*databasev1.TagSpec, 0, len(tagNames))
	for _, name := range tagNames {
		tags = append(tags, &databasev1.TagSpec{Name: name, Type: databasev1.TagType_TAG_TYPE_STRING})
	}
	return &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "demo", Group: group},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: defaultName, Tags: tags}},
		Fields:      []*databasev1.FieldSpec{{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

// TestDistributedPlan_SchemaEvolvedMultiGroupAgg runs GROUP BY svc,
// SUM(value) through DistributedPlan.Execute over groups with different
// schemas, in every group order. Each row renders as its projected tags (NULL
// when absent) followed by the sum.
func TestDistributedPlan_SchemaEvolvedMultiGroupAgg(t *testing.T) {
	prevRaw := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(prevRaw) })

	cases := []struct {
		groupTags  map[string][]string
		rows       map[string][]evolvedNodeRow
		want       map[string]string
		name       string
		projection []string
	}{
		{
			// The apache/skywalking#14069 oracle.
			name:       "old group lacks the projected tag",
			groupTags:  map[string][]string{"old": {tagSvc}, "new": {tagSvc, tagZone}},
			projection: []string{tagSvc, tagZone},
			rows: map[string][]evolvedNodeRow{
				"old": {nodeRow(2, tagSvc, "old"), nodeRow(3, tagSvc, "old")},
				"new": {nodeRow(7, tagSvc, "new", tagZone, "west"), nodeRow(11, tagSvc, "new", tagZone, "west")},
			},
			want: map[string]string{"old": "old,NULL,5", "new": "new,west,18"},
		},
		{
			name:       "groups add disjoint tags",
			groupTags:  map[string][]string{"a": {tagSvc, tagZone}, "b": {tagSvc, tagRegion}},
			projection: []string{tagSvc, tagZone, tagRegion},
			rows: map[string][]evolvedNodeRow{
				"a": {nodeRow(1, tagSvc, "a", tagZone, "west")},
				"b": {nodeRow(2, tagSvc, "b", tagRegion, "eu")},
			},
			want: map[string]string{"a": "a,west,NULL,1", "b": "b,NULL,eu,2"},
		},
		{
			name:       "only group defining the tag has no rows",
			groupTags:  map[string][]string{"old": {tagSvc}, "new": {tagSvc, tagZone}},
			projection: []string{tagSvc, tagZone},
			rows: map[string][]evolvedNodeRow{
				"old": {nodeRow(2, tagSvc, "old"), nodeRow(3, tagSvc, "old")},
			},
			want: map[string]string{"old": "old,NULL,5"},
		},
		{
			// a's node sends zone as string, b's as TagValue (all null).
			name:       "the tag is null in one group's rows",
			groupTags:  map[string][]string{"a": {tagSvc, tagZone}, "b": {tagSvc, tagZone}},
			projection: []string{tagSvc, tagZone},
			rows: map[string][]evolvedNodeRow{
				"a": {nodeRow(1, tagSvc, "a", tagZone, "west")},
				"b": {nodeRow(2, tagSvc, "b")},
			},
			want: map[string]string{"a": "a,west,1", "b": "b,NULL,2"},
		},
	}
	for _, tc := range cases {
		groups := make([]string, 0, len(tc.groupTags))
		for g := range tc.groupTags {
			groups = append(groups, g)
		}
		sort.Strings(groups)
		for _, order := range [][]string{groups, {groups[1], groups[0]}} {
			t.Run(fmt.Sprintf("%s/%s-first", tc.name, order[0]), func(t *testing.T) {
				runSchemaEvolvedCase(t, order, tc.groupTags, tc.rows, tc.projection, tc.want)
			})
		}
	}
}

func runSchemaEvolvedCase(t *testing.T, groups []string, groupTags map[string][]string,
	rows map[string][]evolvedNodeRow, projection []string, want map[string]string,
) {
	t.Helper()
	byGroup := map[string]*databasev1.Measure{}
	schemas := make([]*databasev1.Measure, 0, len(groups))
	for _, g := range groups {
		byGroup[g] = evolvedGroupSchema(g, groupTags[g])
		schemas = append(schemas, byGroup[g])
	}
	nodes := &schemaEvolvedNodes{schemas: byGroup, rows: rows, projections: map[string][]string{}}
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          groups,
		TagProjection:   &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: projection}}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc}}}},
			FieldName:     fieldValue,
		},
		Agg: &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
	}
	p, analyzeErr := AnalyzeDistributed(req, schemas, nil, vmeasure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	iter, execErr := p.Execute(executor.WithDistributedExecutionContext(context.Background(), nodes))
	if execErr != nil {
		t.Fatalf("Execute: %v", execErr)
	}
	defer iter.Close()

	// Each group is only asked for the projected tags its own schema defines.
	for _, g := range groups {
		var wantAsked []string
		for _, tag := range projection {
			if slices.Contains(groupTags[g], tag) {
				wantAsked = append(wantAsked, tag)
			}
		}
		if !slices.Equal(nodes.projections[g], wantAsked) {
			t.Fatalf("group %s was asked for %v, want %v", g, nodes.projections[g], wantAsked)
		}
	}
	got := map[string]string{}
	for iter.Next() {
		for _, idp := range iter.Current() {
			dp := idp.GetDataPoint()
			var tagOrder, cells []string
			for _, family := range dp.GetTagFamilies() {
				for _, tag := range family.GetTags() {
					tagOrder = append(tagOrder, tag.GetKey())
					cell := "NULL"
					if s := tag.GetValue().GetStr(); s != nil {
						cell = s.GetValue()
					}
					cells = append(cells, cell)
				}
			}
			if !slices.Equal(tagOrder, projection) {
				t.Fatalf("tags = %v, want projection order %v", tagOrder, projection)
			}
			if len(dp.GetFields()) != 1 {
				t.Fatalf("fields = %v, want one value field", dp.GetFields())
			}
			cells = append(cells, fmt.Sprint(dp.GetFields()[0].GetValue().GetInt().GetValue()))
			got[cells[0]] = strings.Join(cells, ",")
		}
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for svc, w := range want {
		if got[svc] != w {
			t.Fatalf("rows = %v, want %v", got, want)
		}
	}
}
