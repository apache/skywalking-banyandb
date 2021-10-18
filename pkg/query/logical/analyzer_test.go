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
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// setUpAnalyzer creates a default analyzer for testing.
// You have to close the underlying metadata after test
func setUpAnalyzer() (*logical.Analyzer, func(), error) {
	metadataService, err := metadata.NewService(context.TODO())
	if err != nil {
		return nil, func() {
		}, err
	}

	lc, lp := test.RandomUnixDomainListener()
	rootDir := test.RandomTempDir()
	err = metadataService.FlagSet().Parse([]string{"--listener-client-url=" + lc, "--listener-peer-url=" + lp, "--metadata-root-path=" + rootDir})

	if err != nil {
		return nil, func() {
		}, err
	}

	err = metadataService.PreRun()
	if err != nil {
		return nil, func() {
		}, err
	}

	ana, err := logical.CreateAnalyzerFromMetaService(metadataService)
	if err != nil {
		return nil, func() {
		}, err
	}
	return ana, func() {
		metadataService.GracefulStop()
		os.RemoveAll(rootDir)
	}, nil
}

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

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
			logical.IndexScan(sT, eT, metadata, nil, tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry}, nil),
			0),
		20).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.QueryOrder_SORT_DESC).
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
				logical.OrderBy("duration", modelv1.QueryOrder_SORT_DESC),
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

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewQueryRequestBuilder().
		Limit(100).
		Offset(0).
		Metadata("default", "sw").
		FieldsInTagFamily("searchable", "trace_id", "=", "123").
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical.Limit(
		logical.Offset(logical.IndexScan(time.Now(), time.Now(), metadata, []logical.Expr{
			logical.Eq(logical.NewSearchableFieldRef("trace_id"), logical.Str("123")),
		}, nil, nil),
			0),
		100).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestAnalyzer_OrderBy_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", modelv1.QueryOrder_SORT_DESC).
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

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration2", modelv1.QueryOrder_SORT_DESC).
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

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.QueryOrder_SORT_DESC).
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

	ana, stopFunc, err := setUpAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

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
