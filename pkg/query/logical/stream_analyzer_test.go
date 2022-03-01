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
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

// setUpStreamAnalyzer creates a default analyzer for testing.
// You have to close the underlying metadata after teststream
func setUpStreamAnalyzer() (*logical.StreamAnalyzer, func(), error) {
	metadataService, err := metadata.NewService(context.TODO())
	if err != nil {
		return nil, func() {
		}, err
	}

	rootDir := teststream.RandomTempDir()
	err = metadataService.FlagSet().Parse([]string{"--metadata-root-path=" + rootDir})

	if err != nil {
		return nil, func() {
		}, err
	}

	err = metadataService.PreRun()
	if err != nil {
		return nil, func() {
		}, err
	}

	err = teststream.PreloadSchema(metadataService.SchemaRegistry())
	if err != nil {
		return nil, func() {
		}, err
	}

	ana, err := logical.CreateStreamAnalyzerFromMetaService(metadataService)
	if err != nil {
		return nil, func() {
		}, err
	}
	return ana, func() {
		metadataService.GracefulStop()
		os.RemoveAll(rootDir)
	}, nil
}

func TestStreamAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewStreamQueryRequestBuilder().
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

func TestStreamAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.Sort_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "http.method", "service_id", "duration").
		TagsInTagFamily("searchable", "service_id", "=", "my_app", "http.method", "=", "GET", "mq.topic", "=", "event_topic").
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
				logical.OrderBy("duration", modelv1.Sort_SORT_DESC),
				logical.NewTags("searchable", "http.method", "service_id", "duration")),
			10),
		5).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestStreamAnalyzer_TraceIDQuery(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(100).
		Offset(0).
		Metadata("default", "sw").
		TagsInTagFamily("searchable", "trace_id", "=", "123").
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

func TestStreamAnalyzer_OrderBy_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", modelv1.Sort_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "trace_id", "service_id").
		TagsInTagFamily("searchable", "duration", ">", 500).
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}

func TestStreamAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration2", modelv1.Sort_SORT_DESC).
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

func TestStreamAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("duration", modelv1.Sort_SORT_DESC).
		Metadata("default", "sw").
		Projection("searchable", "duration", "service_id", "unknown").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrTagNotDefined)
}

func TestStreamAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpStreamAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewStreamQueryRequestBuilder().
		Limit(5).
		Offset(10).
		Metadata("default", "sw").
		Projection("duration", "service_id").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		TagsInTagFamily("searchable", "start_time", ">", 10000).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildStreamSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}
