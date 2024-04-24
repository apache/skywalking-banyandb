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

package v1

import (
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

// Series denotes a series of data points.
type Series struct {
	Subject      string
	EntityValues []*modelv1.TagValue
	Buffer       []byte
	ID           common.SeriesID
}

// Marshal encodes series to internal Buffer and generates ID.
func (s *Series) Marshal() error {
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

// Unmarshal decodes series from internal Buffer.
func (s *Series) Unmarshal(src []byte) error {
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

// SeriesPool is a pool of Series.
type SeriesPool struct {
	pool sync.Pool
}

// Generate creates a new Series or gets one from the pool.
func (sp *SeriesPool) Generate() *Series {
	sv := sp.pool.Get()
	if sv == nil {
		return &Series{}
	}
	return sv.(*Series)
}

// Release puts a Series back to the pool.
func (sp *SeriesPool) Release(s *Series) {
	s.reset()
	sp.pool.Put(s)
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

// ToList converts SeriesList to posting.List.
func (a SeriesList) ToList() posting.List {
	pl := roaring.NewPostingList()
	for _, v := range a {
		pl.Insert(uint64(v.ID))
	}
	return pl
}

// IDs returns the IDs of the SeriesList.
func (a SeriesList) IDs() []common.SeriesID {
	ids := make([]common.SeriesID, 0, len(a))
	for _, v := range a {
		ids = append(ids, v.ID)
	}
	return ids
}
