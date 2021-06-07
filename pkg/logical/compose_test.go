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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/clientutil"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

func TestTableScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}

func TestIndexScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500, "duration", "<=", 1000),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}

func TestMultiIndexesScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500, "duration", "<=", 1000, "component", "=", "mysql"),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}

func TestTraceIDSearch(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa"),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}

func TestTraceIDSearchAndIndexSearch(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa", "duration", "<=", 1000),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}

func TestProjection(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa", "duration", "<=", 1000),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
		builder.BuildProjection("startTime", "traceID"),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}
