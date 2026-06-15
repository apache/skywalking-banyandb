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

package trace

import (
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Phase1Plan holds the executable Phase-1 pipeline and its carry-forward operator.
type Phase1Plan struct {
	Pipeline *vectorized.Pipeline
	Carry    *DistinctTraceID
}

// Phase2Plan holds the executable Phase-2 pipeline and its grouping operator.
type Phase2Plan struct {
	Pipeline *vectorized.Pipeline
	Group    *GroupByTraceID
}

// BuildMergePhase1 builds the ordered sidx Phase-1 pipeline.
func BuildMergePhase1(iters []itersort.Iterator[*MergeItem], desc bool, maxTraceSize uint32, batchSize int) (*Phase1Plan, error) {
	source := NewSortedMerge(iters, desc, batchSize)
	return buildPhase1(source, true, maxTraceSize)
}

// BuildStaticPhase1 builds the traceID lookup Phase-1 pipeline.
func BuildStaticPhase1(traceIDs []string, keys map[string]int64, maxTraceSize uint32, batchSize int) (*Phase1Plan, error) {
	source := NewStaticTraceIDSource(traceIDs, keys, batchSize)
	return buildPhase1(source, false, maxTraceSize)
}

// BuildPhase2 builds the span-materialization Phase-2 pipeline.
// source must emit Phase-2 schema batches. traceIDsOrder is the Phase-1 arrival order.
func BuildPhase2(source vectorized.PullOperator, traceIDsOrder []string) (*Phase2Plan, error) {
	schema := source.OutputSchema()
	group := NewGroupByTraceID(schema, traceIDsOrder)
	pipeline, buildErr := vectorized.NewPipelineBuilder().
		From(source).
		Apply(NewProject(schema)).
		Break(group).
		Build()
	if buildErr != nil {
		return nil, buildErr
	}
	return &Phase2Plan{Pipeline: pipeline, Group: group}, nil
}

func buildPhase1(source vectorized.PullOperator, decode bool, maxTraceSize uint32) (*Phase1Plan, error) {
	schema := source.OutputSchema()
	limitCarry := NewLimitedDistinctTraceID(schema, maxTraceSize)
	builder := vectorized.NewPipelineBuilder().From(source)
	if decode {
		builder.Apply(NewIDDecode(schema))
	}
	builder.Apply(limitCarry)
	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		return nil, buildErr
	}
	return &Phase1Plan{Pipeline: pipeline, Carry: limitCarry.Carry()}, nil
}
