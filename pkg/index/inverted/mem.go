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

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/metadata"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

var (
	_ index.Writer        = (*memTable)(nil)
	_ index.FieldIterable = (*memTable)(nil)
)

type memTable struct {
	fields *fieldMap
}

func newMemTable() *memTable {
	return &memTable{
		fields: newFieldMap(1000),
	}
}

func (m *memTable) Write(field index.Field, itemID common.ItemID) error {
	return m.fields.put(field, itemID)
}

var _ index.FieldIterator = (*fIterator)(nil)

type fIterator struct {
	index     int
	val       *index.PostingValue
	keys      [][]byte
	valueRepo *termMap
	closed    bool
}

func (f *fIterator) Next() bool {
	if f.closed {
		return false
	}
	f.index++
	if f.index >= len(f.keys) {
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

func newFieldIterator(keys [][]byte, fValue *termMap) index.FieldIterator {
	return &fIterator{
		keys:      keys,
		valueRepo: fValue,
		index:     -1,
	}
}

func (m *memTable) Iterator(fieldKey index.FieldKey, rangeOpts index.RangeOpts,
	order modelv2.QueryOrder_Sort) (iter index.FieldIterator, err error) {
	fieldsValues, ok := m.fields.get(fieldKey)
	if !ok {
		return nil, nil
	}
	fValue := fieldsValues.value
	var terms [][]byte
	{
		fValue.mutex.RLock()
		defer fValue.mutex.RUnlock()
		for _, value := range fValue.repo {
			if rangeOpts.Between(value.Term) == 0 {
				terms = append(terms, value.Term)
			}
		}
	}
	if len(terms) < 1 {
		return nil, nil
	}
	switch order {
	case modelv2.QueryOrder_SORT_ASC, modelv2.QueryOrder_SORT_UNSPECIFIED:
		sort.SliceStable(terms, func(i, j int) bool {
			return bytes.Compare(terms[i], terms[j]) < 0
		})
	case modelv2.QueryOrder_SORT_DESC:
		sort.SliceStable(terms, func(i, j int) bool {
			return bytes.Compare(terms[i], terms[j]) > 0
		})
	}
	return newFieldIterator(terms, fValue), nil
}

func (m *memTable) MatchTerms(field index.Field) (posting.List, error) {
	fieldsValues, ok := m.fields.get(field.Key)
	if !ok {
		return roaring.EmptyPostingList, nil
	}
	list := fieldsValues.value.get(field.Term)
	if list == nil {
		return roaring.EmptyPostingList, nil
	}
	return list, nil
}

var _ kv.Iterator = (*flushIterator)(nil)

type flushIterator struct {
	fieldIdx     int
	termIdx      int
	key          []byte
	value        []byte
	fields       *fieldMap
	valid        bool
	err          error
	termMetadata metadata.Term
}

func (i *flushIterator) Next() {
	if i.fieldIdx >= len(i.fields.lst) {
		i.valid = false
		return
	}
	fieldID := i.fields.lst[i.fieldIdx]
	terms := i.fields.repo[fieldID]
	if i.termIdx < len(terms.value.lst) {
		i.termIdx++
		if !i.setCurr() {
			i.Next()
		}
		return
	}
	i.fieldIdx++
	i.termIdx = 0
	if !i.setCurr() {
		i.Next()
	}
}

func (i *flushIterator) Rewind() {
	i.fieldIdx = 0
	i.termIdx = 0
	i.valid = true
	if !i.setCurr() {
		i.valid = false
	}
}

func (i *flushIterator) Seek(_ []byte) {
	panic("unsupported")
}

func (i *flushIterator) Key() []byte {
	return i.key
}

func (i *flushIterator) Val() []byte {
	return i.value
}

func (i *flushIterator) Valid() bool {
	return i.valid
}

func (i *flushIterator) Close() error {
	return i.err
}

func (i *flushIterator) setCurr() bool {
	if i.fieldIdx >= len(i.fields.lst) {
		return false
	}
	fieldID := i.fields.lst[i.fieldIdx]
	term := i.fields.repo[fieldID]
	if i.termIdx >= len(term.value.lst) {
		return false
	}
	valueID := term.value.lst[i.termIdx]
	value := term.value.repo[valueID]
	v, err := value.Value.Marshall()
	if err != nil {
		i.err = multierr.Append(i.err, err)
		return false
	}
	i.value = v
	f := index.Field{
		Key:  term.key,
		Term: value.Term,
	}
	i.key, err = f.Marshal(i.termMetadata)
	if err != nil {
		i.err = multierr.Append(i.err, err)
		return false
	}
	return true
}

func (m *memTable) Iter(termMetadata metadata.Term) kv.Iterator {
	return &flushIterator{
		fields:       m.fields,
		termMetadata: termMetadata,
	}
}
