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

var (
	DataTypeNotSupportedErr = errors.New("unsupported data type")
)

type Transform interface {
	Run(ExecutionContext) Future
	AppendParent(...Future)
}

var _ Transform = (*rootTransform)(nil)

type rootTransform struct {
	params *logical.RootOp
}

func NewRootTransform(params *logical.RootOp) Transform {
	return &rootTransform{
		params: params,
	}
}

func (r *rootTransform) Run(ExecutionContext) Future {
	return &emptyFuture{}
}

func (r *rootTransform) AppendParent(...Future) {
	// do nothing
}

var _ Transform = (*paginationTransform)(nil)

type paginationTransform struct {
	params  *logical.Pagination
	parents Futures
}

func NewPaginationTransform(params *logical.Pagination) Transform {
	return &paginationTransform{
		params: params,
	}
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
		return nil, DataTypeNotSupportedErr
	})
}

func (p *paginationTransform) AppendParent(f ...Future) {
	p.parents = append(p.parents, f...)
}

var _ Transform = (*sortMergeTransform)(nil)

type sortMergeTransform struct {
	params  *logical.SortMerge
	parents Futures
}

func NewSortMergeTransform(params *logical.SortMerge) Transform {
	return &sortMergeTransform{
		params: params,
	}
}

func (s *sortMergeTransform) Run(ec ExecutionContext) Future {
	return s.parents.Then(func(result Result) (Data, error) {
		// TODO(megrez): merge traces with hashMap and sort
		panic("implement me")
	})
}

func (s *sortMergeTransform) AppendParent(f ...Future) {
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
			if len(dg) == 1 {
				return dg[0], nil
			} else if len(dg) >= 2 {
				intersection := HashIntersection(dg[0].Unwrap().([]common.ChunkID), dg[1].Unwrap().([]common.ChunkID))
				for i := 2; i < len(dg); i++ {
					intersection = HashIntersection(intersection, dg[i].Unwrap().([]common.ChunkID))
				}
				return NewChunkIDs(intersection...), nil
			} else {
				return NewChunkIDs(), nil
			}
		} else {
			return nil, DataTypeNotSupportedErr
		}
	})
}

func (c *chunkIDsMergeTransform) AppendParent(f ...Future) {
	c.parents = append(c.parents, f...)
}

// HashIntersection has complexity: O(n * x) where x is a factor of hash function efficiency (between 1 and 2)
func HashIntersection(a, b []common.ChunkID) []common.ChunkID {
	set := make([]common.ChunkID, 0)
	hash := make(map[common.ChunkID]struct{})

	for i := 0; i < len(a); i++ {
		el := a[i]
		hash[el] = struct{}{}
	}

	for i := 0; i < len(b); i++ {
		el := b[i]
		if _, found := hash[el]; found {
			set = append(set, el)
		}
	}

	return set
}
