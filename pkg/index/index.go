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
	"io"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var ErrMalformed = errors.New("the data is malformed")

type FieldKey struct {
	SeriesID  common.SeriesID
	IndexRule string
}

func (f FieldKey) Marshal() []byte {
	return bytes.Join([][]byte{
		f.SeriesID.Marshal(),
		[]byte(f.IndexRule),
	}, nil)
}

func (f FieldKey) Equal(other FieldKey) bool {
	return f.SeriesID == other.SeriesID && f.IndexRule == other.IndexRule
}

type Field struct {
	Key  []byte
	Term []byte
}

func (f Field) Marshal() []byte {
	return bytes.Join([][]byte{f.Key, f.Term}, []byte(":"))
}

func (f *Field) Unmarshal(raw []byte) error {
	bb := bytes.SplitN(raw, []byte(":"), 2)
	if len(bb) < 2 {
		return errors.Wrap(ErrMalformed, "unable to unmarshal the field")
	}
	f.Key = make([]byte, len(bb[0]))
	copy(f.Key, bb[0])
	f.Term = make([]byte, len(bb[1]))
	copy(f.Term, bb[1])
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

type PostingValue struct {
	Term  []byte
	Value posting.List
}

type Writer interface {
	Write(field Field, itemID common.ItemID) error
}

type FieldIterable interface {
	Iterator(fieldKey FieldKey, termRange RangeOpts, order modelv2.QueryOrder_Sort) (iter FieldIterator, found bool)
}

type Searcher interface {
	FieldIterable
	MatchField(fieldKey FieldKey) (list posting.List, err error)
	MatchTerms(field Field) (list posting.List, err error)
	Range(fieldKey FieldKey, opts RangeOpts) (list posting.List, err error)
}

type Store interface {
	io.Closer
	Writer
	Searcher
}
