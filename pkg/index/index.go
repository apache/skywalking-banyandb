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
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var ErrMalformed = errors.New("the data is malformed")

type FieldKey struct {
	SeriesID    common.SeriesID
	IndexRuleID uint32
	Analyzer    databasev1.IndexRule_Analyzer
}

func (f FieldKey) MarshalIndexRule() string {
	return string(convert.Uint32ToBytes(f.IndexRuleID))
}

func (f FieldKey) Marshal() []byte {
	return bytes.Join([][]byte{
		f.SeriesID.Marshal(),
		convert.Uint32ToBytes(f.IndexRuleID),
	}, nil)
}

func (f FieldKey) MarshalToStr() string {
	return string(f.Marshal())
}

func (f *FieldKey) Unmarshal(raw []byte) error {
	f.SeriesID = common.SeriesID(convert.BytesToUint64(raw[0:8]))
	f.IndexRuleID = convert.BytesToUint32(raw[8:])
	return nil
}

func (f FieldKey) Equal(other FieldKey) bool {
	return f.SeriesID == other.SeriesID && f.IndexRuleID == other.IndexRuleID
}

type Field struct {
	Key  FieldKey
	Term []byte
}

func (f Field) Marshal() ([]byte, error) {
	return bytes.Join([][]byte{f.Key.Marshal(), f.Term}, nil), nil
}

func (f *Field) Unmarshal(raw []byte) error {
	if len(raw) < 13 {
		return errors.WithMessagef(ErrMalformed, "malformed field: expected more than 12, got %d", len(raw))
	}
	fk := &f.Key
	err := fk.Unmarshal(raw[:12])
	if err != nil {
		return errors.Wrap(err, "unmarshal a field")
	}
	term := raw[12:]
	f.Term = make([]byte, len(term))
	copy(f.Term, term)
	return nil
}

type RangeOpts struct {
	Upper         []byte
	Lower         []byte
	IncludesUpper bool
	IncludesLower bool
}

func (r RangeOpts) Between(value []byte) int {
	if r.Upper != nil {
		var in bool
		if r.IncludesUpper {
			in = bytes.Compare(r.Upper, value) >= 0
		} else {
			in = bytes.Compare(r.Upper, value) > 0
		}
		if !in {
			return 1
		}
	}
	if r.Lower != nil {
		var in bool
		if r.IncludesLower {
			in = bytes.Compare(r.Lower, value) <= 0
		} else {
			in = bytes.Compare(r.Lower, value) < 0
		}
		if !in {
			return -1
		}
	}
	return 0
}

type FieldIterator interface {
	Next() bool
	Val() *PostingValue
	Close() error
}

var EmptyFieldIterator = &emptyIterator{}

type emptyIterator struct{}

func (i *emptyIterator) Next() bool {
	return false
}

func (i *emptyIterator) Val() *PostingValue {
	return nil
}

func (i *emptyIterator) Close() error {
	return nil
}

type PostingValue struct {
	Term  []byte
	Value posting.List
}

type Writer interface {
	Write(fields []Field, itemID common.ItemID) error
}

type FieldIterable interface {
	Iterator(fieldKey FieldKey, termRange RangeOpts, order modelv1.Sort) (iter FieldIterator, err error)
}

type Searcher interface {
	FieldIterable
	Match(fieldKey FieldKey, match []string) (list posting.List, err error)
	MatchField(fieldKey FieldKey) (list posting.List, err error)
	MatchTerms(field Field) (list posting.List, err error)
	Range(fieldKey FieldKey, opts RangeOpts) (list posting.List, err error)
}

type Store interface {
	observability.Observable
	io.Closer
	Writer
	Searcher
}

type GetSearcher func(location databasev1.IndexRule_Type) (Searcher, error)

type Filter interface {
	fmt.Stringer
	Execute(getSearcher GetSearcher, seriesID common.SeriesID) (posting.List, error)
}
