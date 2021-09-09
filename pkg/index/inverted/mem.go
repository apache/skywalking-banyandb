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
	"bytes"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

var ErrFieldsAbsent = errors.New("fields are absent")

var _ index.Searcher = (*MemTable)(nil)

type MemTable struct {
	terms *fieldMap
	name  string
}

func NewMemTable(name string) *MemTable {
	return &MemTable{
		name:  name,
		terms: newFieldMap(1000),
	}
}

func (m *MemTable) Insert(field index.Field, chunkID common.ItemID) error {
	return m.terms.put(field, chunkID)
}

func (m *MemTable) MatchField(fieldName []byte) (list posting.List) {
	fieldsValues, ok := m.terms.get(fieldName)
	if !ok {
		return roaring.EmptyPostingList
	}
	return fieldsValues.value.allValues()
}

var _ index.FieldIterator = (*fIterator)(nil)

type fIterator struct {
	index     int
	val       *index.PostingValue
	keys      [][]byte
	valueRepo *postingMap
	closed    bool
}

func (f *fIterator) Next() bool {
	if f.closed {
		return false
	}
	f.index++
	if f.index >= len(f.keys) {
		_ = f.Close()
		return false
	}
	f.val = f.valueRepo.getEntry(f.keys[f.index])
	if f.val == nil {
		return f.Next()
	}
	return true
}

func (f *fIterator) Val() *index.PostingValue {
	return f.val
}

func (f *fIterator) Close() error {
	f.closed = true
	return nil
}

func newFieldIterator(keys [][]byte, fValue *postingMap) index.FieldIterator {
	return &fIterator{
		keys:      keys,
		valueRepo: fValue,
		index:     -1,
	}
}

func (m *MemTable) FieldIterator(fieldName []byte, order modelv2.QueryOrder_Sort) index.FieldIterator {
	fieldsValues, ok := m.terms.get(fieldName)
	if !ok {
		return nil
	}
	fValue := fieldsValues.value
	var keys [][]byte
	{
		fValue.mutex.RLock()
		defer fValue.mutex.RUnlock()
		for _, value := range fValue.repo {
			keys = append(keys, value.Key)
		}
	}
	switch order {
	case modelv2.QueryOrder_SORT_ASC:
		sort.SliceStable(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
	case modelv2.QueryOrder_SORT_DESC:
		sort.SliceStable(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) > 0
		})
	}
	return newFieldIterator(keys, fValue)
}

func (m *MemTable) MatchTerms(field index.Field) (list posting.List) {
	fieldsValues, ok := m.terms.get(field.Key)
	if !ok {
		return roaring.EmptyPostingList
	}
	return fieldsValues.value.get(field.Term).Clone()
}

func (m *MemTable) Range(fieldName []byte, opts index.RangeOpts) (list posting.List) {
	fieldsValues, ok := m.terms.get(fieldName)
	if !ok {
		return roaring.EmptyPostingList
	}
	return fieldsValues.value.getRange(opts)
}
