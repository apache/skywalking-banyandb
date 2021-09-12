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

package index

import (
	"bytes"

	"go.uber.org/multierr"

	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
)

type CompositePostingValueFn = func(term, value []byte, delegated kv.Iterator) (*PostingValue, error)

var _ FieldIterator = (*FieldIteratorTemplate)(nil)

type FieldIteratorTemplate struct {
	delegated kv.Iterator

	init      bool
	curr      *PostingValue
	err       error
	termRange RangeOpts
	fn        CompositePostingValueFn
	reverse   bool
	field     Field
}

func (f *FieldIteratorTemplate) Next() bool {
	if !f.init {
		f.init = true
		f.delegated.Seek(f.field.Marshal())
	}
	if !f.delegated.Valid() {
		return false
	}
	field := &Field{}
	err := field.Unmarshal(f.delegated.Key())
	if err != nil {
		f.err = err
		return false
	}
	if !bytes.Equal(field.Key, f.field.Key) {
		return false
	}
	pv, err := f.fn(field.Term, f.delegated.Val(), f.delegated)
	if err != nil {
		f.err = err
		return false
	}
	in := f.termRange.Between(pv.Term)
	switch {
	case in > 0:
		if f.reverse {
			return f.Next()
		} else {
			return false
		}
	case in < 0:
		if f.reverse {
			return false
		} else {
			return f.Next()
		}
	}
	f.curr = pv
	return true
}

func (f *FieldIteratorTemplate) Val() *PostingValue {
	return f.curr
}

func (f *FieldIteratorTemplate) Close() error {
	return f.delegated.Close()
}

func NewFieldIteratorTemplate(fieldKey FieldKey, termRange RangeOpts, order modelv2.QueryOrder_Sort, iterable kv.Iterable, fn CompositePostingValueFn) *FieldIteratorTemplate {
	var reverse bool
	var term []byte
	switch order {
	case modelv2.QueryOrder_SORT_ASC, modelv2.QueryOrder_SORT_UNSPECIFIED:
		term = termRange.Lower
		reverse = false
	case modelv2.QueryOrder_SORT_DESC:
		term = termRange.Upper
		reverse = true
	}
	if order == modelv2.QueryOrder_SORT_DESC {
		reverse = true
	}
	iter := iterable.NewIterator(kv.ScanOpts{
		Prefix:  fieldKey.Marshal(),
		Reverse: reverse,
	})
	return &FieldIteratorTemplate{
		delegated: iter,
		termRange: termRange,
		fn:        fn,
		reverse:   reverse,
		field: Field{
			Key:  fieldKey.Marshal(),
			Term: term,
		},
	}
}

type SwitchFn = func(a, b []byte) bool

var _ FieldIterator = (*mergedIterator)(nil)

type mergedIterator struct {
	inner        []FieldIterator
	drained      []FieldIterator
	drainedCount int
	cur          *PostingValue
	switchFn     SwitchFn
	init         bool
	closed       bool
}

func NewMergedIterator(merged []FieldIterator, fn SwitchFn) FieldIterator {
	return &mergedIterator{
		inner:    merged,
		drained:  make([]FieldIterator, len(merged)),
		switchFn: fn,
	}
}

func (m *mergedIterator) Next() bool {
	if m.closed {
		return false
	}
	if m.allDrained() {
		return false
	}
	if !m.init {
		for i, iterator := range m.inner {
			if !iterator.Next() {
				m.drain(i)
			}
		}
		if m.allDrained() {
			return false
		}
		m.init = true
	}
	var head FieldIterator
	var headIndex int
	for i, iterator := range m.inner {
		if iterator == nil {
			continue
		}
		if head == nil {
			head = iterator
			continue
		}
		if m.switchFn(head.Val().Term, iterator.Val().Term) {
			head = iterator
			headIndex = i
		}
	}
	m.cur = head.Val()
	if !head.Next() {
		m.drain(headIndex)
	}
	return true
}

func (m *mergedIterator) Val() *PostingValue {
	return m.cur
}

func (m *mergedIterator) Close() error {
	m.closed = true
	var err error
	for _, iterator := range m.drained {
		if iterator == nil {
			continue
		}
		err = multierr.Append(err, iterator.Close())
	}
	return err
}

func (m *mergedIterator) drain(index int) {
	m.drained[index], m.inner[index] = m.inner[index], nil
	m.drainedCount++
}

func (m *mergedIterator) allDrained() bool {
	return m.drainedCount == len(m.inner)
}
