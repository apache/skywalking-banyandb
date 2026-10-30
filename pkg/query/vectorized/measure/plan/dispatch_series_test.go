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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func TestDispatchSeriesBatchesCompleteRawInput(t *testing.T) {
	schema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)
	req := bareReq()
	req.Limit = math.MaxUint32
	before := proto.Clone(req)
	result, _, err := DispatchSeriesBatches(context.Background(), req, metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
	require.NoError(t, err)
	require.True(t, ec.called)
	require.NotNil(t, ec.lastOpts.Order)
	require.Equal(t, index.OrderByTypeSeries, ec.lastOpts.Order.Type)
	require.Equal(t, modelv1.Sort_SORT_ASC, ec.lastOpts.Order.Sort)
	require.True(t, proto.Equal(before, req), "dispatch must not mutate the bound request")
	require.NoError(t, result.Close())
	result, _, err = DispatchBatches(context.Background(), req, metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
	require.NoError(t, err)
	require.Nil(t, ec.lastOpts.Order)
	require.NoError(t, result.Close())
}

func TestDispatchSeriesBatchesRejectsOrderSensitiveInput(t *testing.T) {
	cases := []struct {
		mutate func(*measurev1.QueryRequest)
		name   string
	}{
		{name: "limited", mutate: func(r *measurev1.QueryRequest) { r.Limit = 10 }},
		{name: "offset", mutate: func(r *measurev1.QueryRequest) { r.Offset = 1 }},
		{name: "ordered", mutate: func(r *measurev1.QueryRequest) { r.OrderBy = &modelv1.QueryOrder{} }},
		{name: "grouped", mutate: func(r *measurev1.QueryRequest) { r.GroupBy = &measurev1.QueryRequest_GroupBy{} }},
		{name: "aggregate", mutate: func(r *measurev1.QueryRequest) { r.Agg = &measurev1.QueryRequest_Aggregation{} }},
		{name: "top", mutate: func(r *measurev1.QueryRequest) { r.Top = &measurev1.QueryRequest_Top{} }},
		{name: "multiple_groups", mutate: func(r *measurev1.QueryRequest) { r.Groups = append(r.Groups, "other") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)
			req := bareReq()
			req.Limit = math.MaxUint32
			tc.mutate(req)
			_, _, err := DispatchSeriesBatches(context.Background(), req, metadata, schema, logicalSchema, ec, dispatchCfg(), false, false)
			require.Error(t, err)
			require.False(t, ec.called)
		})
	}
}

func TestDispatchSeriesBatchesRejectsNonlocalModes(t *testing.T) {
	for _, partial := range []bool{false, true} {
		schema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)
		schema.IndexMode = !partial
		req := bareReq()
		req.Limit = math.MaxUint32
		_, _, err := DispatchSeriesBatches(context.Background(), req, metadata, schema, logicalSchema, ec, dispatchCfg(), partial, false)
		require.Error(t, err)
		require.False(t, ec.called)
	}
}
