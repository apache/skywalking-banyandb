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

package plan

import (
	"context"
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ScanParams holds everything the executor needs to materialize a batch
// source from a Measure. The analyzer populates these from the proto
// QueryRequest at plan-build time; the executor (G8c) consults the
// MeasureExecutionContext and constructs the MeasureBatchResult right
// before calling Scan.Build.
type ScanParams struct {
	Measure         *databasev1.Measure
	TimeRange       *timestamp.TimeRange
	Query           index.Query
	Entities        [][]*modelv1.TagValue
	TagProjection   []model.TagProjection
	FieldProjection []string
}

// Scan is the leaf node of every vec plan. It carries the schema for
// downstream nodes to consult and the parameters the executor needs to
// build a source. Source is set by the executor immediately before
// Build is called; tests can populate it directly to drive Build with a
// fake source.
type Scan struct {
	BatchSchema *vectorized.BatchSchema
	Source      vectorized.PullOperator
	Params      ScanParams
}

// NewScan constructs a Scan node with an analyzed schema and params.
// Source is unset; the executor or test populates it before Build.
func NewScan(schema *vectorized.BatchSchema, params ScanParams) *Scan {
	return &Scan{BatchSchema: schema, Params: params}
}

// Schema returns the BatchSchema of rows this node emits.
func (s *Scan) Schema() *vectorized.BatchSchema { return s.BatchSchema }

// Children returns no children — Scan is the leaf.
func (s *Scan) Children() []VecPlan { return nil }

// Build attaches Source as the pipeline source. The executor must set
// Source before invoking Build; an unset Source is treated as a
// programming error (the executor missed a step).
func (s *Scan) Build(_ context.Context, bc *BuildContext) error {
	if s.Source == nil {
		return fmt.Errorf("plan.Scan.Build: Source not set; the executor must populate it before Build")
	}
	bc.Builder.From(s.Source)
	return nil
}

// String returns a single-line debug description.
func (s *Scan) String() string {
	name := ""
	if s.Params.Measure != nil {
		name = s.Params.Measure.GetMetadata().GetName()
	}
	return fmt.Sprintf("Scan(measure=%s)", name)
}
