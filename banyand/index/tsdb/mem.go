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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

var ErrFieldsAbsent = errors.New("fields are absent")

var emptyPostingList = roaring.NewPostingList()

type MemTable struct {
	terms *fieldMap
	name  string
	group string
}

func NewMemTable(name, group string) *MemTable {
	return &MemTable{
		name:  name,
		group: group,
	}
}

type Field struct {
	name  []byte
	value []byte
}

type FieldSpec struct {
	Name string
}

func (m *MemTable) Initialize(fields []FieldSpec) error {
	if len(fields) < 1 {
		return ErrFieldsAbsent
	}
	m.terms = newFieldMap(len(fields))
	for _, f := range fields {
		m.terms.createKey([]byte(f.Name))
	}
	return nil
}

func (m *MemTable) Insert(field *Field, chunkID common.ChunkID) error {
	return m.terms.put(field, chunkID)
}

func (m *MemTable) MatchField(fieldName []byte) (list posting.List) {
	fieldsValues, ok := m.terms.get(fieldName)
	if !ok {
		return emptyPostingList
	}
	return fieldsValues.value.allValues()
}

func (m *MemTable) MatchTerms(field *Field) (list posting.List) {
	fieldsValues, ok := m.terms.get(field.name)
	if !ok {
		return emptyPostingList
	}
	return fieldsValues.value.get(field.value)
}

func (m *MemTable) Range(fieldName []byte, opts *RangeOpts) (list posting.List) {
	fieldsValues, ok := m.terms.get(fieldName)
	if !ok {
		return emptyPostingList
	}
	return fieldsValues.value.getRange(opts)
}
