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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

func TestAnalyzer_SimpleTimeScan(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	criteria := builder.Build(
		AddLimit(0),
		AddOffset(0),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	fmt.Print(logical.Format(plan))
}

func TestAnalyzer_ComplexQuery(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	criteria := builder.Build(
		AddLimit(5),
		AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("service_name", "=", "my_app", "http.method", "=", "GET"),
		builder.BuildOrderBy("service_instance_id", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	fmt.Print(logical.Format(plan))
}

func TestAnalyzer_Fields_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	criteria := builder.Build(
		AddLimit(5),
		AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500),
		builder.BuildOrderBy("service_instance_id", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.FieldNotDefinedErr)
}

func TestAnalyzer_OrderBy_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	criteria := builder.Build(
		AddLimit(5),
		AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildOrderBy("duration", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.FieldNotDefinedErr)
}

func TestAnalyzer_Projection_FieldNotDefined(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	criteria := builder.Build(
		AddLimit(5),
		AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildProjection("duration", "service_id"),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)

	_, err = ana.Analyze(context.TODO(), criteria, metadata, schema)
	assert.ErrorIs(err, logical.FieldNotDefinedErr)
}
