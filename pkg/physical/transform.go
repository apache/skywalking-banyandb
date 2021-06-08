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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

type Transform interface {
	Run(ExecutionContext) Future
	AppendParent(...Future)
}

var _ Transform = (*paginationTransform)(nil)

type paginationTransform struct {
	params  *logical.Pagination
	parents Futures
}

func (p *paginationTransform) Run(ExecutionContext) Future {
	return p.parents.Then(func(result Result) (Data, error) {
		successValues := result.Success()
		if dg, ok := successValues.(DataGroup); ok {
			var entities []*data.Trace
			for _, d := range dg {
				if traceEntities, ok := d.(*traces); ok {
					entities = append(entities, traceEntities.traces...)
				}
			}
			if int(p.params.Offset) < len(entities) {
				if int(p.params.Offset+p.params.Limit) < len(entities) {
					return NewTraceData(entities[p.params.Offset : p.params.Offset+p.params.Limit]...), nil
				} else {
					return NewTraceData(entities[p.params.Offset:]...), nil
				}
			} else {
				return NewTraceData(), nil
			}
		}
		return nil, errors.New("unsupported data type")
	})
}

func (p *paginationTransform) AppendParent(f ...Future) {
	p.parents = append(p.parents, f...)
}

var _ Transform = (*sortedMergeTransform)(nil)

type sortedMergeTransform struct {
	params  *logical.SortedMerge
	parents Futures
}

func (s *sortedMergeTransform) Run(ec ExecutionContext) Future {
	return s.parents.Then(func(result Result) (Data, error) {
		// TODO(megrez): merge traces with hashMap and sort
		panic("implement me")
	})
}

func (s *sortedMergeTransform) AppendParent(f ...Future) {
	s.parents = append(s.parents, f...)
}

var _ Transform = (*chunkIDsMergeTransform)(nil)

type chunkIDsMergeTransform struct {
	params  *logical.ChunkIDsMerge
	parents Futures
}

func NewChunkIDsMergeTransform(params *logical.ChunkIDsMerge) Transform {
	return &chunkIDsMergeTransform{
		params: params,
	}
}

func (c *chunkIDsMergeTransform) Run(ec ExecutionContext) Future {
	return c.parents.Then(func(result Result) (Data, error) {
		sucValues := result.Success()
		if dg, ok := sucValues.(DataGroup); ok {
			var chunkIdMap = make(map[common.ChunkID]struct{})
			for _, d := range dg {
				if chunkIdData, ok := d.(*chunkIDs); ok {
					for _, cid := range chunkIdData.ids {
						chunkIdMap[cid] = struct{}{}
					}
				} else {
					return nil, errors.New("unsupported data type")
				}
			}
			var resp []common.ChunkID
			for k := range chunkIdMap {
				resp = append(resp, k)
			}
			return NewChunkIDs(resp...), nil
		} else {
			return nil, errors.New("unsupported data type")
		}
	})
}

func (c *chunkIDsMergeTransform) AppendParent(f ...Future) {
	c.parents = append(c.parents, f...)
}
