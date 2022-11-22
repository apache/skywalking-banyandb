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

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

var ErrUnsupportedOperation = errors.New("unsupported operation")

func (s *store) MatchField(fieldKey index.FieldKey) (list posting.List, err error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (list posting.List, err error) {
	f, err := field.Marshal()
	if err != nil {
		return nil, err
	}
	list = roaring.NewPostingList()
	err = s.lsm.GetAll(f, func(itemID []byte) error {
		list.Insert(common.ItemID(convert.BytesToUint64(itemID)))
		return nil
	})
	if errors.Is(err, kv.ErrKeyNotFound) {
		return roaring.EmptyPostingList, nil
	}
	return
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, err error) {
	iter, err := s.Iterator(fieldKey, opts, modelv1.Sort_SORT_ASC)
	if err != nil {
		return roaring.EmptyPostingList, err
	}
	list = roaring.NewPostingList()
	for iter.Next() {
		err = multierr.Append(err, list.Union(iter.Val().Value))
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort) (index.FieldIterator, error) {
	return index.NewFieldIteratorTemplate(s.l, fieldKey, termRange, order, s.lsm,
		func(term, value []byte, delegated kv.Iterator) (*index.PostingValue, error) {
			pv := &index.PostingValue{
				Term:  term,
				Value: roaring.NewPostingListWithInitialData(convert.BytesToUint64(value)),
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
				itemID := convert.BytesToUint64(delegated.Val())
				if e := s.l.Debug(); e.Enabled() {
					e.Uint64("series_id", uint64(fieldKey.SeriesID)).
						Uint64("index_rule_id", uint64(fieldKey.IndexRuleID)).
						Uint64("item_id", itemID).
						Msg("fetched item from the index")
				}
				pv.Value.Insert(common.ItemID(itemID))
			}
			return pv, nil
		})
}

func (s *store) Match(_ index.FieldKey, _ []string) (posting.List, error) {
	return nil, errors.WithMessage(ErrUnsupportedOperation, "LSM-Tree index doesn't support full-text searching")
}
