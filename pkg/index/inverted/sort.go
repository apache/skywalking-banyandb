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

// Package inverted implements a inverted index repository.
package inverted

import (
	"context"
	"errors"
	"io"
	"math"

	"github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func (s *store) Sort(sids []common.SeriesID, fieldKey index.FieldKey, order modelv1.Sort, preLoadSize int) (iter index.FieldIterator, err error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.MarshalIndexRule()
	tqs := make([]bluge.Query, len(sids))
	for i := range sids {
		tq := bluge.NewTermQuery(string(sids[i].Marshal()))
		tq.SetField(seriesIDField)
		tqs[i] = tq
	}
	var query bluge.Query
	if len(tqs) == 0 {
		query = bluge.NewMatchAllQuery()
	} else {
		bq := bluge.NewBooleanQuery()
		bq.AddShould(tqs...)
		bq.SetMinShould(1)
		query = bq
	}

	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	result := &sortIterator{
		query:     query,
		reader:    reader,
		sortedKey: sortedKey,
		size:      preLoadSize,
		sid:       fieldKey.SeriesID,
	}
	return result, nil
}

type sortIterator struct {
	query     bluge.Query
	err       error
	reader    *bluge.Reader
	current   *blugeMatchIterator
	sortedKey string
	size      int
	skipped   int
	sid       common.SeriesID
}

func (si *sortIterator) Next() bool {
	if si.err != nil {
		return false
	}
	if si.current == nil {
		return si.loadCurrent()
	}

	if si.next() {
		return true
	}
	si.current.Close()
	return si.loadCurrent()
}

func (si *sortIterator) loadCurrent() bool {
	size := si.size + si.skipped
	if size < 0 {
		// overflow
		size = math.MaxInt64
	}
	topNSearch := bluge.NewTopNSearch(size, si.query).SortBy([]string{si.sortedKey})
	if si.skipped > 0 {
		topNSearch = topNSearch.SetFrom(si.skipped)
	}

	documentMatchIterator, err := si.reader.Search(context.Background(), topNSearch)
	if err != nil {
		si.err = err
		return false
	}

	iter := newBlugeMatchIterator(documentMatchIterator, nil, true)
	si.current = &iter
	if si.next() {
		return true
	}
	si.err = io.EOF
	return false
}

func (si *sortIterator) next() bool {
	if si.current.Next() {
		si.skipped++
		return true
	}
	return false
}

func (si *sortIterator) Val() *index.ItemRef {
	return si.current.Val()
}

func (si *sortIterator) Close() error {
	if errors.Is(si.err, io.EOF) {
		si.err = nil
		if si.current != nil {
			return errors.Join(si.current.Close(), si.reader.Close())
		}
		return si.reader.Close()
	}
	if si.current == nil {
		return errors.Join(si.err, si.reader.Close())
	}
	return errors.Join(si.err, si.current.Close(), si.reader.Close())
}
