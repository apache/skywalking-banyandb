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

package physical

import (
	"errors"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

var _ Transform = (*tableScanTransform)(nil)

type tableScanTransform struct {
	params  *logical.TableScan
	parents Futures
}

func NewTableScanTransform(params *logical.TableScan) Transform {
	return &tableScanTransform{
		params: params,
	}
}

func (t *tableScanTransform) Run(ec ExecutionContext) Future {
	return NewFuture(func() Result {
		sT, eT := t.params.TimeRange().Begin(), t.params.TimeRange().End()
		entities, err := ec.UniModel().ScanEntity(*t.params.Metadata(), sT, eT, series.ScanOptions{
			Projection: t.params.Projection(),
		})
		if err != nil {
			return Failure(err)
		}
		traceEntities := data.NewTraceWithEntities(entities)
		return Success(NewTraceData(traceEntities))
	})
}

func (t *tableScanTransform) AppendParent(f ...Future) {
	t.parents = t.parents.Append(f...)
}

var _ Transform = (*chunkIDsFetchTransform)(nil)

func NewChunkIDsFetchTransform(params *logical.ChunkIDsFetch) Transform {
	return &chunkIDsFetchTransform{
		params: params,
	}
}

type chunkIDsFetchTransform struct {
	params  *logical.ChunkIDsFetch
	parents Futures
}

func (c *chunkIDsFetchTransform) Run(ec ExecutionContext) Future {
	return c.parents.Then(func(result Result) (Data, error) {
		if result.Error() != nil {
			return nil, result.Error()
		}
		v := result.Success()
		if v.DataType() == ChunkID {
			entities, err := ec.UniModel().FetchEntity(*c.params.Metadata(), v.(*chunkIDs).ids, series.ScanOptions{
				Projection: c.params.Projection(),
			})
			if err != nil {
				return nil, err
			}
			traceEntities := data.NewTraceWithEntities(entities)
			return NewTraceData(traceEntities), nil
		}
		return nil, errors.New("incompatible upstream data type")
	})
}

func (c *chunkIDsFetchTransform) AppendParent(f ...Future) {
	c.parents = c.parents.Append(f...)
}

var _ Transform = (*traceIDFetchTransform)(nil)

type traceIDFetchTransform struct {
	params  *logical.TraceIDFetch
	parents Futures
}

func NewTraceIDFetchTransform(params *logical.TraceIDFetch) Transform {
	return &traceIDFetchTransform{
		params: params,
	}
}

func (t *traceIDFetchTransform) Run(ec ExecutionContext) Future {
	return NewFuture(func() Result {
		trace, err := ec.UniModel().FetchTrace(*t.params.Metadata(), t.params.TraceID)
		if err != nil {
			return Failure(err)
		}
		return Success(NewTraceData(&trace))
	})
}

func (t *traceIDFetchTransform) AppendParent(f ...Future) {
	t.parents = t.parents.Append(f...)
}
