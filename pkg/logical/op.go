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

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ Op = (*rootOp)(nil)

type rootOp struct {
}

func (r *rootOp) Name() string {
	return "Root{}"
}

func (r *rootOp) OpType() string {
	return OpRoot
}

func NewRoot() Op {
	return &rootOp{}
}

var _ Op = (*SortedMerge)(nil)

// SortedMerge define parameters for an aggregate operation,
// QueryOrder contains sorted field and sort algorithm
type SortedMerge struct {
	QueryOrder *apiv1.QueryOrder
}

func (s *SortedMerge) Name() string {
	return fmt.Sprintf("SortedMerge{fieldName=%s,sort=%v}", string(s.QueryOrder.KeyName()), s.QueryOrder.Sort())
}

func (s *SortedMerge) OpType() string {
	return OpSortedMerge
}

func NewSortedMerge(queryOrder *apiv1.QueryOrder) Op {
	return &SortedMerge{QueryOrder: queryOrder}
}

var _ Op = (*Pagination)(nil)

// Pagination defines parameters for paging
type Pagination struct {
	Offset uint32
	Limit  uint32
}

func (p *Pagination) Name() string {
	return fmt.Sprintf("Pagination{Offset=%d,Limit=%d}", p.Offset, p.Limit)
}

func (p *Pagination) OpType() string {
	return OpPagination
}

func NewPagination(offset, limit uint32) Op {
	return &Pagination{
		Offset: offset,
		Limit:  limit,
	}
}

var _ Op = (*ChunkIDsMerge)(nil)

type ChunkIDsMerge struct {
}

func (c *ChunkIDsMerge) Name() string {
	return "ChunkIDsMerge{}"
}

func (c *ChunkIDsMerge) OpType() string {
	return OpChunkIDsMerge
}

func NewChunkIDsMerge() Op {
	return &ChunkIDsMerge{}
}
