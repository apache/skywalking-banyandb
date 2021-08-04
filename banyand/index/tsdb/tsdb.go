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

package tsdb

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/posting"
)

type RangeOpts struct {
	Upper         []byte
	Lower         []byte
	IncludesUpper bool
	IncludesLower bool
}

type GlobalStore interface {
	Window(startTime, endTime uint64) (Searcher, bool)
	Initialize(fields []FieldSpec) error
	Insert(field *Field, chunkID common.ChunkID) error
}

type Searcher interface {
	MatchField(fieldNames []byte) (list posting.List)
	MatchTerms(field *Field) (list posting.List)
	Range(fieldName []byte, opts *RangeOpts) (list posting.List)
}

type store struct {
	memTable *MemTable
	//TODO: add data tables
}

func (s *store) Window(_, _ uint64) (Searcher, bool) {
	return s.memTable, true
}

func (s *store) Initialize(fields []FieldSpec) error {
	return s.memTable.Initialize(fields)
}

func (s *store) Insert(field *Field, chunkID common.ChunkID) error {
	return s.memTable.Insert(field, chunkID)
}

func NewStore(name, group string, shardID uint) GlobalStore {
	return &store{
		memTable: NewMemTable(name, group, shardID),
	}
}
