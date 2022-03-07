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

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

// setUpMeasureAnalyzer creates a default analyzer for testing.
// You have to close the underlying metadata after testmeasure
func setUpMeasureAnalyzer() (*logical.MeasureAnalyzer, func(), error) {
	metadataService, err := metadata.NewService(context.TODO())
	if err != nil {
		return nil, func() {
		}, err
	}

	rootDir := testmeasure.RandomTempDir()
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
		Metadata("default", "cpm").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical.MeasureIndexScan(sT, eT, metadata, nil,
		tsdb.Entity{tsdb.AnyEntry},
		nil, nil).
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
		Metadata("default", "cpm").
		TagProjection("default", "entity_id", "scope").
		TagsInTagFamily("default", "entity_id", "=", "abc", "scope", "=", "endpoint").
		FieldProjection("summation", "count", "value").
		TimeRange(sT, eT).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical.MeasureIndexScan(sT, eT, metadata,
		[]logical.Expr{
			logical.Eq(logical.NewTagRef("default", "scope"), logical.Str("endpoint")),
		}, tsdb.Entity{tsdb.Entry("abc")},
		[][]*logical.Tag{logical.NewTags("default", "entity_id", "scope")},
		[]*logical.Field{logical.NewField("summation"), logical.NewField("count"), logical.NewField("value")},
	).Analyze(schema)
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
		Metadata("default", "cpm").
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
		Metadata("default", "cpm").
		TagProjection("default", "scope", "entity_id").
		FieldProjection("summation", "count", "value", "unknown").
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
		Metadata("default", "cpm").
		TagProjection("default", "scope", "entity_id").
		FieldProjection("summation", "count", "value").
		TimeRange(time.Now().Add(-3*time.Hour), time.Now()).
		TagsInTagFamily("searchable", "unindexed-tag", ">", 10000).
		Build()

	metadata := criteria.GetMetadata()

	schema, err := ana.BuildMeasureSchema(context.TODO(), metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.ErrIndexNotDefined)
}
