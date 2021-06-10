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
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

var (
	DataTypeNotSupportedErr       = errors.New("unsupported data type")
	MultiWaysMergeNotSupportedErr = errors.New("multi-way merge > 3 not supported")
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
			var ets []*apiv1.Entity
			for _, d := range dg {
				if traceEntities, ok := d.(*entities); ok {
					ets = append(ets, traceEntities.entities...)
				}
			}
			if int(p.params.Offset) < len(ets) {
				if int(p.params.Offset+p.params.Limit) < len(ets) {
					return NewTraceData(ets[p.params.Offset : p.params.Offset+p.params.Limit]...), nil
				} else {
					return NewTraceData(ets[p.params.Offset:]...), nil
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
		sucValues := result.Success()
		if dg, ok := sucValues.(DataGroup); ok {
			if len(dg) == 1 {
				// we have to know the field index in advance
				ExternalSort(dg[0].(*entities).entities, s.params.FieldIdx, s.params.QueryOrder.Sort())
				return dg[0].(*entities), nil
			} else if len(dg) == 2 {
				panic("two way merges")
			} else {
				return nil, MultiWaysMergeNotSupportedErr
			}
		} else {
			return nil, DataTypeNotSupportedErr
		}
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

func ExternalSort(traces []*apiv1.Entity, fieldIdx int, sortAlgorithm apiv1.Sort) {
	sort.Slice(traces, func(i, j int) bool {
		var iPair apiv1.Pair
		traces[i].Fields(&iPair, fieldIdx)
		var jPair apiv1.Pair
		traces[j].Fields(&jPair, fieldIdx)
		lField, _ := getFieldRaw(&iPair)
		rField, _ := getFieldRaw(&jPair)
		comp := bytes.Compare(lField, rField)
		if sortAlgorithm == apiv1.SortASC {
			return comp == -1
		} else {
			return comp == 1
		}
	})
}

func getFieldRaw(pair *apiv1.Pair) ([]byte, error) {
	unionPair := new(flatbuffers.Table)
	if ok := pair.Pair(unionPair); !ok {
		return nil, errors.New("cannot read from pair")
	}
	if pair.PairType() == apiv1.TypedPairStrPair {
		unionStrPairQuery := new(apiv1.StrPair)
		unionStrPairQuery.Init(unionPair.Bytes, unionPair.Pos)
		return unionStrPairQuery.Values(0), nil
	} else if pair.PairType() == apiv1.TypedPairIntPair {
		unionIntPairQuery := new(apiv1.IntPair)
		unionIntPairQuery.Init(unionPair.Bytes, unionPair.Pos)
		return convertToInt64Bytes(unionIntPairQuery.Values(0)), nil
	} else {
		return nil, errors.New("unsupported data types")
	}
}

func convertToInt64Bytes(i64 int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i64))
	return buf
}
