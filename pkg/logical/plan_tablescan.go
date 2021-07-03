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

package logical

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/executor"
)

var _ Plan = (*tableScan)(nil)
var _ UnresolvedPlan = (*unresolvedTableScan)(nil)

type unresolvedTableScan struct {
	startTime        uint64
	endTime          uint64
	projectionFields []string
	traceMetadata    *common.Metadata
	traceState       series.TraceState
}

func (u *unresolvedTableScan) Type() PlanType {
	return PlanTableScan
}

func (u *unresolvedTableScan) Analyze(schema Schema) (Plan, error) {
	if u.projectionFields == nil || len(u.projectionFields) == 0 {
		return &tableScan{
			startTime:           u.startTime,
			endTime:             u.endTime,
			projectionFieldRefs: nil,
			schema:              schema,
			traceMetadata:       u.traceMetadata,
		}, nil
	}

	if schema == nil {
		return nil, errors.Wrap(InvalidSchemaErr, "nil")
	}

	fieldRefs, err := schema.CreateRef(u.projectionFields...)
	if err != nil {
		return nil, err
	}
	return &tableScan{
		startTime:           u.startTime,
		endTime:             u.endTime,
		projectionFields:    u.projectionFields,
		projectionFieldRefs: fieldRefs,
		schema:              schema,
		traceMetadata:       u.traceMetadata,
		traceState:          u.traceState,
	}, nil
}

type tableScan struct {
	startTime           uint64
	endTime             uint64
	traceState          series.TraceState
	projectionFields    []string
	projectionFieldRefs []*fieldRef
	schema              Schema
	traceMetadata       *common.Metadata
}

func (s *tableScan) Execute(ec executor.ExecutionContext) ([]data.Entity, error) {
	return ec.ScanEntity(*s.traceMetadata, s.startTime, s.endTime, series.ScanOptions{
		Projection: s.projectionFields,
		State:      s.traceState,
	})
}

func (s *tableScan) Equal(plan Plan) bool {
	if plan.Type() != PlanTableScan {
		return false
	}
	other := plan.(*tableScan)
	return s.startTime == other.startTime && s.endTime == other.endTime &&
		cmp.Equal(s.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(s.schema, other.schema) &&
		cmp.Equal(s.traceMetadata, other.traceMetadata)
}

func (s *tableScan) Schema() Schema {
	if s.projectionFieldRefs == nil || len(s.projectionFieldRefs) == 0 {
		return s.schema
	}
	return s.schema.Map(s.projectionFieldRefs...)
}

func (s *tableScan) String() string {
	if len(s.projectionFieldRefs) == 0 {
		return fmt.Sprintf("TableScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; projection=None",
			s.startTime, s.endTime, s.traceMetadata.Spec.Group(), s.traceMetadata.Spec.Name())
	} else {
		return fmt.Sprintf("TableScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; projection=%s",
			s.startTime, s.endTime, s.traceMetadata.Spec.Group(), s.traceMetadata.Spec.Name(), formatExpr(", ", s.projectionFieldRefs...))
	}
}

func (s *tableScan) Children() []Plan {
	return []Plan{}
}

func (s *tableScan) Type() PlanType {
	return PlanTableScan
}

func TableScan(startTime, endTime uint64, traceMetadata *common.Metadata, traceState series.TraceState, projection ...string) UnresolvedPlan {
	return &unresolvedTableScan{
		startTime:        startTime,
		endTime:          endTime,
		projectionFields: projection,
		traceMetadata:    traceMetadata,
		traceState:       traceState,
	}
}
