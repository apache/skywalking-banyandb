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

package inverted

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type GlobalStore interface {
	Searcher() index.Searcher
	Initialize(fields []index.FieldSpec) error
	Insert(field index.Field, docID common.ItemID) error
}

type store struct {
	memTable *MemTable
	//TODO: add data tables
}

func (s *store) Searcher() index.Searcher {
	return s.memTable
}

func (s *store) Initialize(fields []index.FieldSpec) error {
	return s.memTable.Initialize(fields)
}

func (s *store) Insert(field index.Field, chunkID common.ItemID) error {
	return s.memTable.Insert(field, chunkID)
}

func NewStore(name string) GlobalStore {
	return &store{
		memTable: NewMemTable(name),
	}
}
