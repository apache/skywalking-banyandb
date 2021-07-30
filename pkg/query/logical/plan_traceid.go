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
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var _ UnresolvedPlan = (*unresolvedTraceIDFetch)(nil)
var _ Plan = (*traceIDFetch)(nil)

type unresolvedTraceIDFetch struct {
	metadata         *common.Metadata
	traceID          string
	projectionFields []string
}

func (t *unresolvedTraceIDFetch) Analyze(s Schema) (Plan, error) {
	if t.projectionFields == nil || len(t.projectionFields) == 0 {
		return &traceIDFetch{
			metadata: t.metadata,
			schema:   s,
			traceID:  t.traceID,
		}, nil
	}

	if s == nil {
		return nil, errors.Wrap(ErrInvalidSchema, "nil")
	}

	fieldRefs, err := s.CreateRef(t.projectionFields...)
	if err != nil {
		return nil, err
	}
	return &traceIDFetch{
		projectionFields:    t.projectionFields,
		projectionFieldRefs: fieldRefs,
		schema:              s,
		traceID:             t.traceID,
		metadata:            t.metadata,
	}, nil
}

func (t *unresolvedTraceIDFetch) Type() PlanType {
	return PlanTraceIDFetch
}

type traceIDFetch struct {
	metadata            *common.Metadata
	traceID             string
	projectionFields    []string
	projectionFieldRefs []*FieldRef
	schema              Schema
}

func (t *traceIDFetch) String() string {
	return fmt.Sprintf("TraceIDFetch: traceID=%s,Metadata{group=%s,name=%s}",
		t.traceID,
		t.metadata.Spec.GetGroup(),
		t.metadata.Spec.GetName(),
	)
}

func (t *traceIDFetch) Children() []Plan {
	return []Plan{}
}

func (t *traceIDFetch) Type() PlanType {
	return PlanTraceIDFetch
}

func (t *traceIDFetch) Schema() Schema {
	return t.schema
}

func (t *traceIDFetch) Equal(plan Plan) bool {
	if plan.Type() != PlanTraceIDFetch {
		return false
	}
	other := plan.(*traceIDFetch)
	return t.traceID == other.traceID &&
		cmp.Equal(t.schema, other.schema) &&
		cmp.Equal(t.metadata, other.metadata)
}

func (t *traceIDFetch) Execute(ec executor.ExecutionContext) ([]data.Entity, error) {
	traceData, err := ec.FetchTrace(*t.metadata, t.traceID, series.ScanOptions{
		Projection: t.projectionFields,
	})
	if err != nil {
		return nil, err
	}
	return traceData.Entities, nil
}

func TraceIDFetch(traceID string, metadata *common.Metadata, projection ...string) UnresolvedPlan {
	return &unresolvedTraceIDFetch{
		metadata:         metadata,
		traceID:          traceID,
		projectionFields: projection,
	}
}
