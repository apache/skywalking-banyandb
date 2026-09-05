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

package measure

import (
	"bytes"
	"container/list"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ sort.Comparable = (*comparableDataPoint)(nil)

type comparableDataPoint struct {
	*measurev1.InternalDataPoint
	sortField []byte
}

func newComparableElement(idp *measurev1.InternalDataPoint, sortByTime bool, sortTagSpec logical.TagSpec) (*comparableDataPoint, error) {
	dp := idp.GetDataPoint()
	var sortField []byte
	if sortByTime {
		sortField = convert.Uint64ToBytes(uint64(dp.Timestamp.AsTime().UnixNano()))
	} else {
		var err error
		sortField, err = pbv1.MarshalTagValue(dp.TagFamilies[sortTagSpec.TagFamilyIdx].Tags[sortTagSpec.TagIdx].Value)
		if err != nil {
			return nil, err
		}
	}

	return &comparableDataPoint{
		InternalDataPoint: idp,
		sortField:         sortField,
	}, nil
}

func (e *comparableDataPoint) SortedField() []byte {
	return e.sortField
}

var _ executor.MIterator = (*sortedMIterator)(nil)

type sortedMIterator struct {
	sort.Iterator[*comparableDataPoint]
	data        *list.List
	uniqueData  map[uint64]*measurev1.InternalDataPoint
	cur         *measurev1.InternalDataPoint
	seenSids    map[uint64]struct{}
	initialized bool
	exhausted   bool
	closed      bool
	indexMode   bool
}

func (s *sortedMIterator) init() {
	if s.initialized {
		return
	}
	s.initialized = true
	if !s.Iterator.Next() {
		s.exhausted = true
		return
	}
	s.data = list.New()
	s.uniqueData = make(map[uint64]*measurev1.InternalDataPoint)
	if s.indexMode {
		s.seenSids = make(map[uint64]struct{})
	}
	s.loadDps()
}

func (s *sortedMIterator) Next() bool {
	if s.data == nil {
		return false
	}
	if s.data.Len() == 0 {
		s.loadDps()
		if s.data.Len() == 0 {
			return false
		}
	}
	idp := s.data.Front()
	s.data.Remove(idp)
	s.cur = idp.Value.(*measurev1.InternalDataPoint)
	return true
}

// loadDps consumes sort-field-equal groups from the underlying iterator and
// pushes their deduplicated rows into s.data. When indexMode is true, rows
// whose Sid was already emitted in a prior group are skipped — without this,
// cross-node duplicates that carry different per-node "last-write" timestamps
// would slip past hashDataPoint (which keys on Sid+timestamp) because they
// land in different sort-field groups. When every row in a group is filtered,
// the loop advances to the next group so callers see continued iteration.
func (s *sortedMIterator) loadDps() {
	for !s.exhausted {
		s.loadOneGroup()
		if s.data.Len() > 0 {
			return
		}
	}
}

func (s *sortedMIterator) loadOneGroup() {
	for k := range s.uniqueData {
		delete(s.uniqueData, k)
	}
	first := s.Iterator.Val()
	s.uniqueData[hashDataPoint(first.GetDataPoint())] = first.InternalDataPoint
	for {
		if !s.Iterator.Next() {
			s.exhausted = true
			break
		}
		v := s.Iterator.Val()
		if bytes.Equal(first.SortedField(), v.SortedField()) {
			key := hashDataPoint(v.GetDataPoint())
			if existed, ok := s.uniqueData[key]; ok {
				if v.GetDataPoint().Version > existed.GetDataPoint().Version {
					s.uniqueData[key] = v.InternalDataPoint
				}
			} else {
				s.uniqueData[key] = v.InternalDataPoint
			}
		} else {
			break
		}
	}
	for _, v := range s.uniqueData {
		if s.indexMode {
			sid := v.GetDataPoint().GetSid()
			if _, dup := s.seenSids[sid]; dup {
				continue
			}
			s.seenSids[sid] = struct{}{}
		}
		s.data.PushBack(v)
	}
}

func (s *sortedMIterator) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{s.cur}
}

func (s *sortedMIterator) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.exhausted = true
	s.data = nil
	s.uniqueData = nil
	s.seenSids = nil
	s.cur = nil
	if s.Iterator == nil {
		return nil
	}
	return s.Iterator.Close()
}

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// hashDataPoint calculates the hash value of a data point with fnv64a.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func hashDataPoint(dp *measurev1.DataPoint) uint64 {
	h := uint64(offset64)
	h = (h ^ dp.Sid) * prime64
	h = (h ^ uint64(dp.Timestamp.Seconds)) * prime64
	h = (h ^ uint64(dp.Timestamp.Nanos)) * prime64
	return h
}
