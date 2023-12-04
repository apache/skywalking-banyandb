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

package storage

import (
	"bytes"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

type Series struct {
	Subject      string
	EntityValues []*modelv1.TagValue
	Buffer       []byte
	ID           common.SeriesID
}

func (s *Series) marshal() error {
	s.Buffer = marshalEntityValue(s.Buffer, convert.StringToBytes(s.Subject))
	var err error
	for _, tv := range s.EntityValues {
		if s.Buffer, err = marshalTagValue(s.Buffer, tv); err != nil {
			return errors.WithMessage(err, "marshal subject and entity values")
		}
	}
	s.ID = common.SeriesID(convert.Hash(s.Buffer))
	return nil
}

func (s *Series) unmarshal(src []byte) error {
	var err error
	s.Buffer = s.Buffer[:0]
	if s.Buffer, src, err = unmarshalEntityValue(s.Buffer, src); err != nil {
		return errors.WithMessage(err, "unmarshal subject")
	}
	s.Subject = string(s.Buffer)
	for len(src) > 0 {
		s.Buffer = s.Buffer[:0]
		var tv *modelv1.TagValue
		if s.Buffer, src, tv, err = unmarshalTagValue(s.Buffer, src); err != nil {
			return errors.WithMessage(err, "unmarshal tag value")
		}
		s.EntityValues = append(s.EntityValues, tv)
	}
	return nil
}

func (s *Series) reset() {
	s.ID = 0
	s.Subject = ""
	s.EntityValues = s.EntityValues[:0]
	s.Buffer = s.Buffer[:0]
}

type SeriesPool struct {
	pool sync.Pool
}

func (sp *SeriesPool) Get() *Series {
	sv := sp.pool.Get()
	if sv == nil {
		return &Series{}
	}
	return sv.(*Series)
}

func (sp *SeriesPool) Put(s *Series) {
	s.reset()
	sp.pool.Put(s)
}

// AnyEntry is the `*` for a regular expression. It could match "any" Entry in an Entity.
var AnyEntry = &modelv1.TagValue_Null{}

const (
	entityDelimiter = '|'
	escape          = '\\'
)

var anyWildcard = []byte{'*'}

func marshalEntityValue(dest, src []byte) []byte {
	if src == nil {
		dest = append(dest, entityDelimiter)
		return dest
	}
	if bytes.IndexByte(src, entityDelimiter) < 0 && bytes.IndexByte(src, escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, entityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == entityDelimiter || b == escape {
			dest = append(dest, escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, entityDelimiter)
	return dest
}

func unmarshalEntityValue(dest, src []byte) ([]byte, []byte, error) {
	if len(src) == 0 {
		return nil, nil, errors.New("empty entity value")
	}
	if src[0] == entityDelimiter {
		return dest, src[1:], nil
	}
	for len(src) > 0 {
		if src[0] == escape {
			if len(src) < 2 {
				return nil, nil, errors.New("invalid escape character")
			}
			src = src[1:]
			dest = append(dest, src[0])
		} else if src[0] == entityDelimiter {
			return dest, src[1:], nil
		} else {
			dest = append(dest, src[0])
		}
		src = src[1:]
	}
	return nil, nil, errors.New("invalid entity value")
}

// SeriesList is a collection of Series.
type SeriesList []*Series

func (a SeriesList) Len() int {
	return len(a)
}

func (a SeriesList) Less(i, j int) bool {
	return a[i].ID < a[j].ID
}

func (a SeriesList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Merge other SeriesList with this one to create a new SeriesList.
func (a SeriesList) Merge(other SeriesList) SeriesList {
	if len(other) == 0 {
		return a
	}
	sort.Sort(other)
	if len(a) == 0 {
		return other
	}
	final := SeriesList{}
	i := 0
	j := 0
	for i < len(a) && j < len(other) {
		if a[i].ID < other[j].ID {
			final = append(final, a[i])
			i++
		} else {
			// deduplication
			if a[i].ID == other[j].ID {
				i++
			}
			final = append(final, other[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		final = append(final, a[i])
	}
	for ; j < len(other); j++ {
		final = append(final, other[j])
	}
	return final
}

func (a SeriesList) toList() posting.List {
	pl := roaring.NewPostingList()
	for _, v := range a {
		pl.Insert(uint64(v.ID))
	}
	return pl
}
