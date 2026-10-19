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
	"fmt"
	"slices"
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

const tagZone = "zone"

// evolvedNodeRow is one stored data point. zone is only stored by groups whose schema defines it.
type evolvedNodeRow struct {
	svc   string
	zone  string
	value int64
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
	nodePlan, analyzeErr := Analyze(req, n.schemas[group], vmeasure.AggModeMap)
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

// scanBatch lays rows out the way storage fills the scan schema. Every row
// sits on shard 0, so both groups report the same shard id.
func scanBatch(schema *vectorized.BatchSchema, rows []evolvedNodeRow) (*vectorized.RecordBatch, error) {
	raw := vectorized.NewRecordBatch(schema, len(rows))
	for i, def := range schema.Columns {
		for r, row := range rows {
			var cell any = int64(0)
			switch {
			case def.Role == vectorized.RoleTag && def.Name == tagSvc:
				cell = row.svc
			case def.Role == vectorized.RoleTag && def.Name == tagZone:
				cell = row.zone
			case def.Role == vectorized.RoleField:
				cell = row.value
			case def.Role == vectorized.RoleSeriesID:
				cell = int64(r + 1)
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

func evolvedGroupSchema(group string, withZone bool) *databasev1.Measure {
	tags := []*databasev1.TagSpec{{Name: tagSvc, Type: databasev1.TagType_TAG_TYPE_STRING}}
	if withZone {
		tags = append(tags, &databasev1.TagSpec{Name: tagZone, Type: databasev1.TagType_TAG_TYPE_STRING})
	}
	return &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: "demo", Group: group},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: defaultName, Tags: tags}},
		Fields:      []*databasev1.FieldSpec{{Name: fieldValue, FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

// TestDistributedPlan_SchemaEvolvedMultiGroupAgg runs the apache/skywalking#14069
// oracle through DistributedPlan.Execute: old schema (svc,value), new schema
// (svc,zone,value), GROUP BY svc, SUM(value), projecting svc and zone.
func TestDistributedPlan_SchemaEvolvedMultiGroupAgg(t *testing.T) {
	prevRaw := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(prevRaw) })

	for _, groups := range [][]string{{"old", "new"}, {"new", "old"}} {
		t.Run(fmt.Sprintf("%s-first", groups[0]), func(t *testing.T) {
			byGroup := map[string]*databasev1.Measure{
				"old": evolvedGroupSchema("old", false),
				"new": evolvedGroupSchema("new", true),
			}
			nodes := &schemaEvolvedNodes{
				schemas: byGroup,
				rows: map[string][]evolvedNodeRow{
					"old": {{svc: "old", value: 2}, {svc: "old", value: 3}},
					"new": {{svc: "new", zone: "west", value: 7}, {svc: "new", zone: "west", value: 11}},
				},
				projections: map[string][]string{},
			}
			req := &measurev1.QueryRequest{
				Name:            "demo",
				Groups:          groups,
				TagProjection:   &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc, tagZone}}}},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
				GroupBy: &measurev1.QueryRequest_GroupBy{
					TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: defaultName, Tags: []string{tagSvc}}}},
					FieldName:     fieldValue,
				},
				Agg: &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
			}
			schemas := []*databasev1.Measure{byGroup[groups[0]], byGroup[groups[1]]}
			p, analyzeErr := AnalyzeDistributed(req, schemas, nil, vmeasure.VectorizedConfig{BatchSize: 1024, QueryMemoryMiB: 16})
			if analyzeErr != nil {
				t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
			}
			iter, execErr := p.Execute(executor.WithDistributedExecutionContext(context.Background(), nodes))
			if execErr != nil {
				t.Fatalf("Execute: %v", execErr)
			}
			defer iter.Close()

			if !slices.Equal(nodes.projections["old"], []string{tagSvc}) {
				t.Fatalf("old group was asked for %v, want only svc", nodes.projections["old"])
			}
			got := map[string]string{}
			for iter.Next() {
				for _, idp := range iter.Current() {
					dp := idp.GetDataPoint()
					tags := map[string]string{}
					var tagOrder []string
					for _, family := range dp.GetTagFamilies() {
						for _, tag := range family.GetTags() {
							tagOrder = append(tagOrder, tag.GetKey())
							tags[tag.GetKey()] = "NULL"
							if s := tag.GetValue().GetStr(); s != nil {
								tags[tag.GetKey()] = s.GetValue()
							}
						}
					}
					if !slices.Equal(tagOrder, []string{tagSvc, tagZone}) {
						t.Fatalf("tags = %v, want projection order [svc zone]", tagOrder)
					}
					if len(dp.GetFields()) != 1 {
						t.Fatalf("fields = %v, want one value field", dp.GetFields())
					}
					got[tags[tagSvc]] = fmt.Sprintf("%s,%s,%d", tags[tagSvc], tags[tagZone], dp.GetFields()[0].GetValue().GetInt().GetValue())
				}
			}
			want := map[string]string{"old": "old,NULL,5", "new": "new,west,18"}
			if len(got) != len(want) || got["old"] != want["old"] || got["new"] != want["new"] {
				t.Fatalf("rows = %v, want %v", got, want)
			}
		})
	}
}
