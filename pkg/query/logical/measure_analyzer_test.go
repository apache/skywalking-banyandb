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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

// setUpMeasureAnalyzer creates a default analyzer for testing.
// You have to close the underlying metadata after testmeasure
func setUpMeasureAnalyzer() (*logical.MeasureAnalyzer, func(), error) {
	err := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	})
	if err != nil {
		return nil, func() {
		}, err
	}
	metadataService, err := metadata.NewService(context.TODO())
	if err != nil {
		return nil, func() {
		}, err
	}
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	if err != nil {
		return nil, func() {
		}, err
	}

	rootDir := testmeasure.RandomTempDir()
	err = metadataService.FlagSet().Parse([]string{
		"--metadata-root-path=" + rootDir,
		"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
	})

	if err != nil {
		return nil, func() {
		}, err
	}

	err = metadataService.PreRun()
	if err != nil {
		return nil, func() {
		}, err
	}

	err = testmeasure.PreloadSchema(metadataService.SchemaRegistry())
	if err != nil {
		return nil, func() {
		}, err
	}

	ana, err := logical.CreateMeasureAnalyzerFromMetaService(metadataService)
	if err != nil {
		return nil, func() {
		}, err
	}
	return ana, func() {
		metadataService.GracefulStop()
		os.RemoveAll(rootDir)
	}, nil
}

func TestMeasureAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical.MeasureLimit(
		logical.MeasureIndexScan(sT, eT, metadata, nil,
			tsdb.Entity{tsdb.AnyEntry},
			nil, nil, false, nil),
		0, logical.DefaultLimit).
		Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestMeasureAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TagProjection("default", "entity_id", "id").
		TagsInTagFamily("default", "entity_id", "=", "abc", "id", "=", pb.TagTypeID("aadxxx")).
		FieldProjection("total", "value").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical.MeasureLimit(
		logical.MeasureIndexScan(sT, eT, metadata,
			[]logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("aadxxx")),
			}, tsdb.Entity{tsdb.Entry("abc")},
			[][]*logical.Tag{logical.NewTags("default", "entity_id", "id")},
			[]*logical.Field{logical.NewField("total"), logical.NewField("value")},
			false,
			nil,
		), 0, logical.DefaultLimit).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestMeasureAnalyzer_GroupByAndAggregation(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TagProjection("default", "entity_id", "id").
		TagsInTagFamily("default", "entity_id", "=", "abc", "id", "=", pb.TagTypeID("abdxx")).
		FieldProjection("total", "value").
		GroupBy("default", "entity_id").Max("value").
		Top(10, "value", modelv1.Sort_SORT_DESC).
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical.MeasureLimit(
		logical.Top(
			logical.Aggregation(
				logical.GroupBy(
					logical.MeasureIndexScan(sT, eT, metadata,
						[]logical.Expr{
							logical.Eq(logical.NewTagRef("default", "id"), logical.ID("abdxx")),
						}, tsdb.Entity{tsdb.Entry("abc")},
						[][]*logical.Tag{logical.NewTags("default", "entity_id", "id")},
						[]*logical.Field{logical.NewField("total"), logical.NewField("value")},
						true,
						nil,
					),
					[][]*logical.Tag{logical.NewTags("default", "entity_id")},
					true,
				),
				logical.NewField("value"),
				modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
				true,
			), &measurev1.QueryRequest_Top{
				Number:         10,
				FieldName:      "value",
				FieldValueSort: modelv1.Sort_SORT_DESC,
			}), 0, logical.DefaultLimit).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestMeasureAnalyzer_Paging(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TagProjection("default", "entity_id", "id").
		TagsInTagFamily("default", "entity_id", "=", "abc", "id", "=", pb.TagTypeID("abdxx")).
		FieldProjection("total", "value").
		OrderBy("id", modelv1.Sort_SORT_DESC).
		Limit(100, 10).
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical.MeasureLimit(
		logical.MeasureIndexScan(sT, eT, metadata,
			[]logical.Expr{
				logical.Eq(logical.NewTagRef("default", "id"), logical.ID("abdxx")),
			}, tsdb.Entity{tsdb.Entry("abc")},
			[][]*logical.Tag{logical.NewTags("default", "entity_id", "id")},
			[]*logical.Field{logical.NewField("total"), logical.NewField("value")},
			false,
			logical.OrderBy("id", modelv1.Sort_SORT_DESC),
		),
		100, 10).Analyze(schema)
	assert.NoError(err)
	assert.NotNil(correctPlan)
	assert.True(cmp.Equal(plan, correctPlan), "plan is not equal to correct plan")
}

func TestMeasureAnalyzer_Projection_TagNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TagProjection("default", "scope", "entity_id", "unknown").
		FieldProjection("summation", "count", "value").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrTagNotDefined)
}

func TestMeasureAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TagProjection("default", "id", "entity_id").
		FieldProjection("total", "value", "unknown").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrFieldNotDefined)
}

func TestMeasureAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana, stopFunc, err := setUpMeasureAnalyzer()
	assert.NoError(err)
	assert.NotNil(ana)
	defer stopFunc()

	criteria := pb.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_traffic").
		TagProjection("default", "id", "name").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		TagsInTagFamily("default", "name", "=", "abc").
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}
