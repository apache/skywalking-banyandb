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
	"context"
	"path"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// OrderBy specifies the order of the result.
type OrderBy struct {
	Index *databasev1.IndexRule
	Sort  modelv1.Sort
}

type seriesIndex struct {
	store    index.SeriesStore
	l        *logger.Logger
	position common.Position
}

func newSeriesIndex(ctx context.Context, root string) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
	}
	var err error
	if si.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:   path.Join(root, "idx"),
		Logger: si.l,
	}); err != nil {
		return nil, err
	}
	return si, nil
}

var entityKey = index.FieldKey{}

func (s *seriesIndex) createPrimary(series *Series) (*Series, error) {
	if err := series.marshal(); err != nil {
		return nil, err
	}
	id, err := s.store.Search(series.Buffer)
	if err != nil {
		return nil, err
	}
	if id > 0 {
		return series, nil
	}
	evv := make([]byte, len(series.Buffer))
	copy(evv, series.Buffer)
	if err := s.store.Create(index.Series{
		ID:           series.ID,
		EntityValues: evv,
	}); err != nil {
		return nil, errors.WithMessagef(err, "create entity values -> seriesID: %s", string(evv))
	}
	return series, nil
}

func (s *seriesIndex) createSecondary(docs index.Documents) error {
	return s.store.Batch(docs)
}

var rangeOpts = index.RangeOpts{}

func (s *seriesIndex) SearchPrimary(ctx context.Context, series *Series) (SeriesList, error) {
	var hasAny, hasWildcard bool
	var prefixIndex int

	for i, tv := range series.EntityValues {
		if tv == nil {
			return nil, errors.New("nil tag value")
		}
		if tv.Value == AnyEntry {
			if !hasAny {
				hasAny = true
				prefixIndex = i
			}
			continue
		}
		if hasAny {
			hasWildcard = true
			break
		}
	}

	var err error

	if hasAny {
		var ss []index.Series
		if hasWildcard {
			if err = series.marshal(); err != nil {
				return nil, err
			}
			ss, err = s.store.SearchWildcard(series.Buffer)
			if err != nil {
				return nil, err
			}
			return convertIndexSeriesToSeriesList(ss)
		}
		series.EntityValues = series.EntityValues[:prefixIndex]
		if err = series.marshal(); err != nil {
			return nil, err
		}
		ss, err = s.store.SearchPrefix(series.Buffer)
		if err != nil {
			return nil, err
		}
		return convertIndexSeriesToSeriesList(ss)
	}
	if err = series.marshal(); err != nil {
		return nil, err
	}
	var seriesID common.SeriesID
	seriesID, err = s.store.Search(series.Buffer)
	if err != nil {
		return nil, err
	}
	if seriesID > 0 {
		series.ID = seriesID
		return SeriesList{series}, nil
	}
	return nil, nil
}

func convertIndexSeriesToSeriesList(indexSeries []index.Series) (SeriesList, error) {
	seriesList := make(SeriesList, 0, len(indexSeries))
	for _, s := range indexSeries {
		var series Series
		series.ID = s.ID
		if err := series.unmarshal(s.EntityValues); err != nil {
			return nil, err
		}
		seriesList = append(seriesList, &series)
	}
	return seriesList, nil
}

func (s *seriesIndex) SearchSecondary(ctx context.Context, series *Series, filter index.Filter, order *OrderBy) (SeriesList, error) {
	seriesList, err := s.SearchPrimary(ctx, series)
	if err != nil {
		return nil, err
	}

	pl := seriesList.toList()
	if filter != nil {
		var plFilter posting.List
		plFilter, err = filter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
			return s.store, nil
		}, 0)
		if err != nil {
			return nil, err
		}
		if err = pl.Intersect(plFilter); err != nil {
			return nil, err
		}
	}

	if order == nil {
		return filterSeriesList(seriesList, pl), nil
	}

	fieldKey := index.FieldKey{
		IndexRuleID: order.Index.GetMetadata().Id,
	}
	iter, err := s.store.Iterator(fieldKey, rangeOpts, order.Sort)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var sortedSeriesList SeriesList
	for iter.Next() {
		pv := iter.Val().Value
		if err = pv.Intersect(pl); err != nil {
			return nil, err
		}
		if pv.IsEmpty() {
			continue
		}
		sortedSeriesList = appendSeriesList(sortedSeriesList, seriesList, pv)
		if err != nil {
			return nil, err
		}
	}
	return sortedSeriesList, err
}

func filterSeriesList(seriesList SeriesList, filter posting.List) SeriesList {
	for i := 0; i < len(seriesList); i++ {
		if !filter.Contains(uint64(seriesList[i].ID)) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			i--
		}
	}
	return seriesList
}

func appendSeriesList(dest, src SeriesList, filter posting.List) SeriesList {
	for i := 0; i < len(src); i++ {
		if !filter.Contains(uint64(src[i].ID)) {
			continue
		}
		dest = append(dest, src[i])
	}
	return dest
}

func (s *seriesIndex) Close() error {
	return s.store.Close()
}
