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
	"github.com/apache/skywalking-banyandb/pkg/pb"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

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
	correctPlan, err := logical.Limit(
		logical.Offset(
			logical.TableScan(sT.UnixNano(), eT.UnixNano(), metadata, series.TraceStateDefault),
			0),
		20).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

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

	correctPlan, err := logical.Limit(
		logical.Offset(
			logical.OrderBy(logical.IndexScan(sT.UnixNano(), eT.UnixNano(), metadata,
				[]logical.Expr{
					logical.Eq(logical.NewFieldRef("service_instance_id"), logical.Str("my_app")),
					logical.Eq(logical.NewFieldRef("http.method"), logical.Str("GET")),
				},
				series.TraceStateDefault),
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

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "trace").
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

	correctPlan, err := logical.TraceIDFetch("123", metadata).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_Fields_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

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
	assert.ErrorIs(err, logical.ErrFieldNotDefined)
}

func TestAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.QueryOrder_SORT_DESC).
		Metadata("default", "trace").
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
	assert.ErrorIs(err, logical.ErrFieldNotDefined)
}

func TestAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

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
	assert.ErrorIs(err, logical.ErrFieldNotDefined)
}

func TestAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "trace").
		Projection("duration", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Fields("service_name", "=", "app").
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}
