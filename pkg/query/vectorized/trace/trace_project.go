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
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Project is a no-op FusibleOperator documenting the Phase-2 projection contract.
// Tag values are already decoded *modelv1.TagValue by the source; empty-projection
// batches have no tag columns so r.Tags will naturally be nil.
type Project struct {
	schema *vectorized.BatchSchema
}

// NewProject constructs the Phase-2 projection operator.
func NewProject(schema *vectorized.BatchSchema) *Project {
	return &Project{schema: schema}
}

// Init is a no-op.
func (p *Project) Init(context.Context) error { return nil }

// OutputSchema returns the Phase-2 schema.
func (p *Project) OutputSchema() *vectorized.BatchSchema { return p.schema }

// Process is a no-op; tag values are already in final form.
func (p *Project) Process(context.Context, *vectorized.RecordBatch) error { return nil }

// Close is a no-op.
func (p *Project) Close() error { return nil }

// BatchToTraceResult converts a Phase-2 RecordBatch into a model.TraceResult.
// schema is required to read tag column names for populating r.Tags.
// Returns nil when batch is nil or empty.
func BatchToTraceResult(batch *vectorized.RecordBatch, schema *vectorized.BatchSchema) *model.TraceResult {
	if batch == nil || batch.Len == 0 {
		return nil
	}
	tidCol := Phase2TraceIDs(batch)
	keyCol := Phase2Keys(batch)
	spanCol := Phase2Spans(batch)
	spanIDCol := Phase2SpanIDs(batch)

	tids := tidCol.Data()
	keys := keyCol.Data()
	spans := spanCol.Data()
	spanIDs := spanIDCol.Data()

	if len(tids) == 0 {
		return nil
	}

	r := &model.TraceResult{
		TID: tids[0],
		Key: keys[0],
	}
	r.Spans = append(r.Spans, spans...)
	r.SpanIDs = append(r.SpanIDs, spanIDs...)

	numTagCols := len(schema.Columns) - phase2FixedColumnCount
	if numTagCols > 0 {
		r.Tags = make([]model.Tag, numTagCols)
		for tagIdx := range numTagCols {
			colDef := schema.Columns[phase2FixedColumnCount+tagIdx]
			r.Tags[tagIdx] = model.Tag{Name: colDef.Name}
			tagCol := Phase2TagCol(batch, tagIdx)
			tagData := tagCol.Data()
			r.Tags[tagIdx].Values = append(r.Tags[tagIdx].Values, tagData...)
		}
	}
	return r
}
