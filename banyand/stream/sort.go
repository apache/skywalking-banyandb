// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"errors"

	"go.uber.org/multierr"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func (s *stream) Sort(ctx context.Context, sso pbv1.StreamSortOptions) (ssr pbv1.StreamSortResult, err error) {
	if sso.TimeRange == nil || len(sso.Entities) < 1 {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sso.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	db := s.databaseSupplier.SupplyTSDB()
	if db == nil {
		return ssr, nil
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sso.TimeRange)
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	series := make([]*pbv1.Series, len(sso.Entities))
	for i := range sso.Entities {
		series[i] = &pbv1.Series{
			Subject:      sso.Name,
			EntityValues: sso.Entities[i],
		}
	}
	seriesList, err := tsdb.Lookup(ctx, series)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return ssr, nil
	}

	iters, err := s.buildSeriesByIndex(tabWrappers, seriesList, sso)
	if err != nil {
		return nil, err
	}
	if len(iters) == 0 {
		return ssr, nil
	}

	it := newItemIter(iters, sso.Order.Sort)
	defer func() {
		err = multierr.Append(err, it.Close())
	}()

	ces := newColumnElements()
	for it.Next() {
		nextItem := it.Val()
		e := nextItem.element
		ces.BuildFromElement(e, sso.TagProjection)
		if len(ces.timestamp) >= sso.MaxElementSize {
			break
		}
	}
	return ces, err
}

// newItemIter returns a ItemIterator which mergers several tsdb.Iterator by input sorting order.
func newItemIter(iters []*searcherIterator, s modelv1.Sort) itersort.Iterator[item] {
	var ii []itersort.Iterator[item]
	for _, iter := range iters {
		ii = append(ii, iter)
	}
	if s == modelv1.Sort_SORT_DESC {
		return itersort.NewItemIter[item](ii, true)
	}
	return itersort.NewItemIter[item](ii, false)
}
