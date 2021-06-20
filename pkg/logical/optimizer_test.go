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

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

func TestOptimizer_ProjectionPushDown(t *testing.T) {
	assert := require.New(t)

	ana := logical.DefaultAnalyzer()

	builder := NewCriteriaBuilder()
	sT, eT := time.Now().Add(-3*time.Hour), time.Now()
	criteria := builder.Build(
		AddLimit(5),
		AddOffset(10),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(sT, eT),
		builder.BuildFields("service_name", "=", "my_app", "http.method", "=", "GET"),
		builder.BuildOrderBy("service_instance_id", apiv1.SortDESC),
		builder.BuildProjection("trace_id", "service_id"),
	)

	traceMetadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *traceMetadata)
	assert.NoError(err)

	plan, err := ana.Analyze(context.TODO(), criteria, traceMetadata, schema)
	assert.NoError(err)
	assert.NotNil(plan)
	fmt.Print(logical.Format(plan))

	plan, err = logical.NewProjectionPushDown().Apply(plan)
	assert.NoError(err)
	assert.NotNil(plan)
	fmt.Print(logical.Format(plan))

	correctPlan, err := logical.Projection(logical.Limit(
		logical.Offset(logical.OrderBy(
			logical.Selection(
				logical.Scan(uint64(sT.UnixNano()), uint64(eT.UnixNano()), traceMetadata, "service_id", "trace_id", "service_instance_id", "http.method", "service_name"),
				logical.Eq(logical.NewFieldRef("http.method"), logical.Str("GET")),
				logical.Eq(logical.NewFieldRef("service_name"), logical.Str("my_app")),
			), "service_instance_id", apiv1.SortDESC,
		), 10), 5,
	), []string{"service_id", "trace_id"}).Analyze(schema)
	assert.NoError(err)
	assert.True(cmp.Equal(plan, correctPlan))
}
