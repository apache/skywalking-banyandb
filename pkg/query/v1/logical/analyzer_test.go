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

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/series"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	logical2 "github.com/apache/skywalking-banyandb/pkg/query/v1/logical"
)

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(0).
		Offset(0).
		Metadata("default", "trace").
		TimeRange(sT, eT).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical2.Limit(
		logical2.Offset(
			logical2.TableScan(sT.UnixNano(), eT.UnixNano(), metadata, series.TraceStateDefault, false),
			0),
		20).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", modelv1.QueryOrder_SORT_DESC).
		Metadata("default", "trace").
		Projection("http.method", "service_id", "service_instance_id").
		Fields("service_id", "=", "my_app", "http.method", "=", "GET").
		TimeRange(sT, eT).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical2.Limit(
		logical2.Offset(
			logical2.OrderBy(logical2.IndexScan(sT.UnixNano(), eT.UnixNano(), metadata,
				[]logical2.Expr{
					logical2.Eq(logical2.NewFieldRef("service_instance_id"), logical2.Str("my_app")),
					logical2.Eq(logical2.NewFieldRef("http.method"), logical2.Str("GET")),
				},
				series.TraceStateDefault, false),
				"service_instance_id", modelv1.QueryOrder_SORT_DESC),
			10),
		5).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_TraceIDQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "sw").
		Fields("trace_id", "=", "123").
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical2.TraceIDFetch("123", metadata, false).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_Fields_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", modelv1.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("trace_id", "service_id").
		Fields("duration", ">", 500).
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.ErrFieldNotDefined)
}

func TestAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("trace_id", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.ErrFieldNotDefined)
}

func TestAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.QueryOrder_SORT_DESC).
		Metadata("default", "sw").
		Projection("duration", "service_id", "unknown").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.ErrFieldNotDefined)
}

func TestAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "sw").
		Projection("duration", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Fields("status_code", "=", "200").
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.ErrIndexNotDefined)
}
