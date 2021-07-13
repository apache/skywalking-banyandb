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
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/fb"
	logical2 "github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(0),
		fb.AddOffset(0),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(sT, eT),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	correctPlan, err := logical2.Limit(
		logical2.Offset(
			logical2.TableScan(uint64(sT.UnixNano()), uint64(eT.UnixNano()), metadata, series.TraceStateDefault),
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

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(sT, eT),
		builder.BuildFields("service_id", "=", "my_app", "http.method", "=", "GET"),
		builder.BuildOrderBy("service_instance_id", apiv1.SortDESC),
		builder.BuildProjection("http.method", "service_id", "service_instance_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan, err := logical2.Limit(
		logical2.Offset(
			logical2.OrderBy(logical2.IndexScan(uint64(sT.UnixNano()), uint64(eT.UnixNano()), metadata,
				[]logical2.Expr{
					logical2.Eq(logical2.NewFieldRef("service_instance_id"), logical2.Str("my_app")),
					logical2.Eq(logical2.NewFieldRef("http.method"), logical2.Str("GET")),
				},
				series.TraceStateDefault),
				"service_instance_id", apiv1.SortDESC),
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

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildFields("trace_id", "=", "123"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)

	correctPlan := logical2.TraceIDFetch("123", metadata, schema)
	assert.NotNil(correctPlan)
	cmp.Equal(plan, correctPlan)
}

func TestAnalyzer_Fields_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "sw"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500),
		builder.BuildOrderBy("service_instance_id", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.FieldNotDefinedErr)
}

func TestAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildOrderBy("duration", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.FieldNotDefinedErr)
}

func TestAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "sw"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildProjection("duration", "service_id", "unknown"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.FieldNotDefinedErr)
}

func TestAnalyzer_Fields_IndexNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical2.DefaultAnalyzer()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(5),
		fb.AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildFields("service_name", "=", "app"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildProjection("duration", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical2.IndexNotDefinedErr)
}
