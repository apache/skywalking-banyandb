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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

func TestTopNProcessorRejectsAnInternalRequestOverSeveralGroups(t *testing.T) {
	p := &topNQueryProcessor{queryService: &queryService{log: logger.GetLogger("topn-test")}}
	for _, request := range []*measurev1.TopNRequest{
		{Groups: []string{"g", "h"}, Name: "n", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC, Agg: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM},
		{Groups: []string{"g"}, Name: "n", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC},
	} {
		resp := p.Rev(context.Background(), bus.NewMessage(1, &measurev1.InternalTopNRequest{Request: request, PartialLimit: 3}))
		_, isErr := resp.Data().(*common.Error)
		assert.True(t, isErr, "shard IDs of different groups cannot be told apart, and a partial needs a function: %v", resp.Data())
	}
}
