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

package logical_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v2"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/logical"
)

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(20).
		Offset(0).
		Metadata("default", "sw").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical.Limit(
		logical.Offset(
			logical.TableScan(sT, eT, metadata, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			0),
		20).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv2.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "http.method", "service_id", "duration").
		FieldsInTagFamily("searchable", "service_id", "=", "my_app", "http.method", "=", "GET", "mq.topic", "=", "event_topic").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical.Limit(
		logical.Offset(
			logical.IndexScan(sT, eT, metadata,
				[]logical.Expr{
					logical.Eq(logical.NewSearchableFieldRef("mq.topic"), logical.Str("event_topic")),
					logical.Eq(logical.NewSearchableFieldRef("http.method"), logical.Str("GET")),
				}, tsdb.Entity{tsdb.Entry("my_app"), tsdb.AnyEntry, tsdb.AnyEntry},
				logical.OrderBy("duration", modelv2.QueryOrder_SORT_DESC),
				logical.NewTags("searchable", "http.method", "service_id", "duration")),
			10),
		5).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestAnalyzer_TraceIDQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "sw").
		FieldsInTagFamily("searchable", "trace_id", "=", "123").
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical.TraceIDFetch("123", metadata).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestAnalyzer_OrderBy_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", modelv2.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "trace_id", "service_id").
		FieldsInTagFamily("searchable", "duration", ">", 500).
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}

func TestAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration2", modelv2.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "trace_id", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}

func TestAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv2.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "duration", "service_id", "unknown").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrFieldNotDefined)
}

func TestAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "sw").
		Projection("duration", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		FieldsInTagFamily("searchable", "start_time", ">", 10000).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}
