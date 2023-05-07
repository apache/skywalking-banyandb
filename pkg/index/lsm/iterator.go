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

package lsm

import (
	"bytes"
	"math"
	"sync"

	"go.uber.org/multierr"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type compositePostingValueFn = func(term, value []byte, delegated kv.Iterator) (*index.PostingValue, error)

var (
	_            index.FieldIterator = (*fieldIteratorTemplate)(nil)
	defaultUpper                     = convert.Uint64ToBytes(math.MaxUint64)
	defaultLower                     = convert.Uint64ToBytes(0)
)

type fieldIteratorTemplate struct {
	err       error
	delegated *delegateIterator
	cur       *index.PostingValue
	fn        compositePostingValueFn
	closer    *run.Closer
	seekKey   []byte
	termRange index.RangeOpts
	closeOnce sync.Once
	init      bool
	reverse   bool
}

func (f *fieldIteratorTemplate) Next() bool {
	if !f.init {
		f.init = true
		f.delegated.Seek(f.seekKey)
	}
	if !f.delegated.Valid() {
		return false
	}
	pv, err := f.fn(f.delegated.Field().Term, f.delegated.Val(), f.delegated)
	if err != nil {
		f.err = err
		return false
	}
	in := f.termRange.Between(pv.Term)
	switch {
	case in > 0:
		if f.reverse {
			return f.Next()
		}
		return false
	case in < 0:
		if f.reverse {
			return false
		}
		return f.Next()
	}
	f.cur = pv
	return true
}

func (f *fieldIteratorTemplate) Val() *index.PostingValue {
	return f.cur
}

func (f *fieldIteratorTemplate) Close() (err error) {
	f.closeOnce.Do(func() {
		defer f.closer.Done()
		err = multierr.Combine(f.err, f.delegated.Close())
	})
	return err
}

func newFieldIteratorTemplate(l *logger.Logger, fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort, iterable kv.Iterable,
	closer *run.Closer, fn compositePostingValueFn,
) *fieldIteratorTemplate {
	if termRange.Upper == nil {
		termRange.Upper = defaultUpper
	}
	if termRange.Lower == nil {
		termRange.Lower = defaultLower
	}
	var reverse bool
	var term []byte
	switch order {
	case modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_UNSPECIFIED:
		term = termRange.Lower
		reverse = false
	case modelv1.Sort_SORT_DESC:
		term = termRange.Upper
		reverse = true
	}
	iter := iterable.NewIterator(kv.ScanOpts{
		Prefix:  fieldKey.Marshal(),
		Reverse: reverse,
	})
	field := index.Field{
		Key:  fieldKey,
		Term: term,
	}
	return &fieldIteratorTemplate{
		delegated: newDelegateIterator(iter, fieldKey, l),
		termRange: termRange,
		fn:        fn,
		reverse:   reverse,
		seekKey:   field.Marshal(),
		closer:    closer,
	}
}

func parseKey(fieldKey index.FieldKey, key []byte) (index.Field, error) {
	f := &index.Field{
		Key: fieldKey,
	}
	err := f.Unmarshal(key)
	if err != nil {
		return *f, err
	}
	return *f, nil
}

var _ kv.Iterator = (*delegateIterator)(nil)

type delegateIterator struct {
	delegated     kv.Iterator
	l             *logger.Logger
	fieldKeyBytes []byte
	curField      index.Field
	fieldKey      index.FieldKey
	closed        bool
}

func newDelegateIterator(delegated kv.Iterator, fieldKey index.FieldKey, l *logger.Logger) *delegateIterator {
	fieldKeyBytes := fieldKey.Marshal()
	return &delegateIterator{
		delegated:     delegated,
		fieldKey:      fieldKey,
		fieldKeyBytes: fieldKeyBytes,
		l:             l,
	}
}

func (di *delegateIterator) Next() {
	di.delegated.Next()
}

func (di *delegateIterator) Rewind() {
	di.delegated.Rewind()
}

func (di *delegateIterator) Seek(key []byte) {
	di.delegated.Seek(key)
}

func (di *delegateIterator) Key() []byte {
	return di.delegated.Key()
}

func (di *delegateIterator) RawKey() []byte {
	return di.delegated.RawKey()
}

func (di *delegateIterator) Field() index.Field {
	return di.curField
}

func (di *delegateIterator) Val() []byte {
	return di.delegated.Val()
}

func (di *delegateIterator) Valid() bool {
	if di.closed || !di.delegated.Valid() {
		return false
	}
	var err error
	di.curField, err = parseKey(di.fieldKey, di.Key())
	if err != nil {
		di.l.Error().Err(err).Msg("fail to parse field from key")
		return false
	}
	if !bytes.Equal(di.curField.Key.Marshal(), di.fieldKeyBytes) {
		if e := di.l.Debug(); e.Enabled() {
			e.Uint64("series_id", uint64(di.fieldKey.SeriesID)).
				Uint32("index_rule_id", di.fieldKey.IndexRuleID).
				Msg("reached the limitation of the field(series_id+index_rule_id)")
		}
		return false
	}
	return true
}

func (di *delegateIterator) Close() error {
	di.closed = true
	return di.delegated.Close()
}
