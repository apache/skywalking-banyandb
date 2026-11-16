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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestToTopNResponseEmptyAggregateHasNoList(t *testing.T) {
	assert.Empty(t, toTopNResponse(nil, true).GetLists())
}

func TestToTopNResponseEmptyWithoutAggregateKeepsOneList(t *testing.T) {
	resp := toTopNResponse(nil, false)
	require.Len(t, resp.GetLists(), 1)
	assert.Empty(t, resp.GetLists()[0].GetItems())
}

func TestToTopNResponseAggregateStampsTheList(t *testing.T) {
	dp := &measurev1.DataPoint{
		TagFamilies: []*modelv1.TagFamily{{Tags: []*modelv1.Tag{{Key: "k"}}}},
		Fields:      []*measurev1.DataPoint_Field{{Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 3}}}}},
	}
	resp := toTopNResponse([]*measurev1.InternalDataPoint{{DataPoint: dp}}, true)
	require.Len(t, resp.GetLists(), 1)
	assert.NotNil(t, resp.GetLists()[0].GetTimestamp())
	require.Len(t, resp.GetLists()[0].GetItems(), 1)
	assert.EqualValues(t, 3, resp.GetLists()[0].GetItems()[0].GetValue().GetInt().GetValue())
}

// TestTopNProcessorRejectsAnInvalidInternalRequest checks that an internal request reads one group, because shard IDs
// of different groups cannot be told apart, and carries an aggregation function, because a partial needs one.
func TestTopNProcessorRejectsAnInvalidInternalRequest(t *testing.T) {
	p := &topNQueryProcessor{queryService: &queryService{log: logger.GetLogger("topn-test")}}
	for name, request := range map[string]*measurev1.TopNRequest{
		"several groups":          {Groups: []string{"g", "h"}, Name: "n", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC, Agg: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		"no aggregation function": {Groups: []string{"g"}, Name: "n", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC},
	} {
		t.Run(name, func(t *testing.T) {
			resp := p.Rev(context.Background(), bus.NewMessage(1, &measurev1.InternalTopNRequest{Request: request, PartialLimit: 3}))
			_, isErr := resp.Data().(*common.Error)
			assert.True(t, isErr, "%v", resp.Data())
		})
	}
}

// failingTopNMeasures serves the source measure of a TopN and a TopN result measure whose scan fails.
type failingTopNMeasures struct {
	measure.Service
	source *databasev1.Measure
}

func (f *failingTopNMeasures) Measure(md *commonv1.Metadata) (measure.Measure, error) {
	if md.GetName() == f.source.GetMetadata().GetName() {
		return &failingMeasure{schema: f.source}, nil
	}
	return &failingMeasure{}, nil
}

type failingMeasure struct {
	schema *databasev1.Measure
}

func (f *failingMeasure) Query(context.Context, model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	return nil, errors.New("scan failed")
}

func (f *failingMeasure) GetSchema() *databasev1.Measure { return f.schema }

func (f *failingMeasure) GetIndexRules() []*databasev1.IndexRule { return nil }

// TestTopNProcessorFailsATracedInternalRequest checks that a data node whose internal TopN query fails answers
// with an error even when the query is traced: an answer without shards would let the liaison merge the other
// nodes' partials and silently return an incomplete top N.
func TestTopNProcessorFailsATracedInternalRequest(t *testing.T) {
	source := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "service_cpm", Group: "g"},
		Entity:   &databasev1.Entity{TagNames: []string{"service_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	ctrl := gomock.NewController(t)
	registry := schema.NewMockTopNAggregation(ctrl)
	registry.EXPECT().GetTopNAggregation(gomock.Any(), gomock.Any()).Return(&databasev1.TopNAggregation{
		Metadata: &commonv1.Metadata{Name: "n", Group: "g"}, SourceMeasure: source.GetMetadata(), FieldName: "value",
	}, nil).AnyTimes()
	repo := metadata.NewMockRepo(ctrl)
	repo.EXPECT().TopNAggregationRegistry().Return(registry).AnyTimes()
	p := &topNQueryProcessor{
		measureService: &failingTopNMeasures{source: source},
		queryService:   &queryService{log: logger.GetLogger("topn-test"), metaService: repo},
	}
	request := &measurev1.TopNRequest{
		Groups: []string{"g"}, Name: "n", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC,
		Agg:       modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(3600, 0))},
		Trace:     true,
	}
	resp := p.Rev(context.Background(), bus.NewMessage(1, &measurev1.InternalTopNRequest{Request: request, PartialLimit: 3}))
	failure, isErr := resp.Data().(*common.Error)
	require.True(t, isErr, "a failed internal query must answer an error, got %T", resp.Data())
	assert.Contains(t, failure.Error(), "scan failed")
}
