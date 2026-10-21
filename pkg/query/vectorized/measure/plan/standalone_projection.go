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

	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type hiddenBatchProjection struct {
	source  vectorized.PullOperator
	schema  *vectorized.BatchSchema
	current *vectorized.RecordBatch
	indices []int
}

func newHiddenBatchProjection(source vectorized.PullOperator, hidden logical.HiddenTagSet) *hiddenBatchProjection {
	p := &hiddenBatchProjection{source: source}
	var defs []vectorized.ColumnDef
	for idx, def := range source.OutputSchema().Columns {
		if def.Role == vectorized.RoleTag && hidden.Contains(def.Name) {
			continue
		}
		defs = append(defs, def)
		p.indices = append(p.indices, idx)
	}
	p.schema = vectorized.NewBatchSchema(defs)
	return p
}
func (p *hiddenBatchProjection) Init(ctx context.Context) error        { return ctx.Err() }
func (p *hiddenBatchProjection) OutputSchema() *vectorized.BatchSchema { return p.schema }
func (p *hiddenBatchProjection) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if p.current != nil {
		p.current.ReleaseOwners()
		p.current = nil
	}
	batch, err := p.source.NextBatch(ctx)
	if err != nil || batch == nil {
		return nil, err
	}
	view := &vectorized.RecordBatch{Schema: p.schema, Len: batch.Len, Selection: batch.Selection, Hints: batch.Hints, Lent: true}
	for _, idx := range p.indices {
		view.Columns = append(view.Columns, batch.Columns[idx])
	}
	view.Owners.PinAll(&batch.Owners)
	p.current = view
	return view, nil
}

func (p *hiddenBatchProjection) Close() error {
	if p.current != nil {
		p.current.ReleaseOwners()
		p.current = nil
	}
	return p.source.Close()
}
