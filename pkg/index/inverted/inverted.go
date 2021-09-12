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
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ index.Store = (*store)(nil)

type store struct {
	diskTable         kv.IndexStore
	memTable          *memTable
	immutableMemTable *memTable
	rwMutex           sync.RWMutex
}

type StoreOpts struct {
	Path   string
	Logger *logger.Logger
}

func NewStore(opts StoreOpts) (index.Store, error) {
	diskTable, err := kv.OpenIndexStore(0, opts.Path, kv.IndexWithLogger(opts.Logger))
	if err != nil {
		return nil, err
	}
	return &store{
		memTable:  newMemTable(),
		diskTable: diskTable,
	}, nil
}

func (s *store) Close() error {
	return s.diskTable.Close()
}

func (s *store) Write(field index.Field, chunkID common.ItemID) error {
	return s.memTable.Write(field, chunkID)
}

func (s *store) Flush() error {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	if s.immutableMemTable == nil {
		s.immutableMemTable = s.memTable
		s.memTable = newMemTable()
	}
	err := s.diskTable.
		Handover(s.immutableMemTable.Iter())
	if err != nil {
		return err
	}
	s.immutableMemTable = nil
	return nil
}

func (s *store) MatchField(fieldKey index.FieldKey) (posting.List, error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (posting.List, error) {
	result := roaring.NewPostingList()
	result, errMem := s.searchInMemTables(result, func(table *memTable) (posting.List, error) {
		list, errInner := table.MatchTerms(field)
		if errInner != nil {
			return nil, errInner
		}
		return list, nil
	})
	if errMem != nil {
		return nil, errors.Wrap(errMem, "mem table of inverted index")
	}
	raw, errTable := s.diskTable.Get(field.Marshal())
	switch {
	case errors.Is(errTable, kv.ErrKeyNotFound):
		return result, nil
	case errTable != nil:
		return nil, errors.Wrap(errTable, "disk table of inverted index")
	}
	list := roaring.NewPostingList()
	err := list.Unmarshall(raw)
	if err != nil {
		return nil, err
	}
	err = result.Union(list)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, err error) {
	iter, found := s.Iterator(fieldKey, opts, modelv2.QueryOrder_SORT_ASC)
	if !found {
		return roaring.EmptyPostingList, nil
	}
	list = roaring.NewPostingList()
	for iter.Next() {
		err = multierr.Append(err, list.Union(iter.Val().Value))
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts,
	order modelv2.QueryOrder_Sort) (index.FieldIterator, bool) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	tt := []*memTable{s.memTable, s.immutableMemTable}
	iters := make([]index.FieldIterator, 0, len(tt)+1)
	for _, table := range tt {
		if table == nil {
			continue
		}
		it, found := table.Iterator(fieldKey, termRange, order)
		if !found {
			continue
		}
		iters = append(iters, it)
	}
	it := index.NewFieldIteratorTemplate(fieldKey, termRange, order, s.diskTable, func(term, val []byte, delegated kv.Iterator) (*index.PostingValue, error) {
		list := roaring.NewPostingList()
		err := list.Unmarshall(val)
		if err != nil {
			return nil, err
		}

		pv := &index.PostingValue{
			Term:  term,
			Value: list,
		}

		for ; delegated.Valid(); delegated.Next() {
			f := index.Field{}
			err := f.Unmarshal(delegated.Key())
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(f.Term, term) {
				break
			}
			l := roaring.NewPostingList()
			err = l.Unmarshall(delegated.Val())
			if err != nil {
				return nil, err
			}
			err = pv.Value.Union(l)
			if err != nil {
				return nil, err
			}
		}
		return pv, nil
	})
	iters = append(iters, it)
	if len(iters) < 1 {
		return nil, false
	}
	var fn index.SwitchFn
	switch order {
	case modelv2.QueryOrder_SORT_ASC, modelv2.QueryOrder_SORT_UNSPECIFIED:
		fn = func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		}
	case modelv2.QueryOrder_SORT_DESC:
		fn = func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}
	}
	return index.NewMergedIterator(iters, fn), true
}

func (s *store) searchInMemTables(result posting.List, entityFunc entityFunc) (posting.List, error) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	tt := []*memTable{s.memTable, s.immutableMemTable}
	for _, table := range tt {
		if table == nil {
			continue
		}
		list, err := entityFunc(table)
		if err != nil {
			return result, err
		}
		err = result.Union(list)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

type entityFunc func(table *memTable) (posting.List, error)
