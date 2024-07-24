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
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func (s *segment[T, O]) IndexDB() IndexDB {
	return s.index
}

func (s *segment[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	return s.index.searchPrimary(ctx, series)
}

type seriesIndex struct {
	store index.SeriesStore
	l     *logger.Logger
}

func newSeriesIndex(ctx context.Context, root string, flushTimeoutSeconds int64) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
	}
	var err error
	if si.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:         path.Join(root, "sidx"),
		Logger:       si.l,
		BatchWaitSec: flushTimeoutSeconds,
	}); err != nil {
		return nil, err
	}
	return si, nil
}

func (s *seriesIndex) Write(docs index.Documents) error {
	return s.store.Batch(index.Batch{
		Documents: docs,
	})
}

var rangeOpts = index.RangeOpts{}

func (s *seriesIndex) searchPrimary(ctx context.Context, series []*pbv1.Series) (sl pbv1.SeriesList, err error) {
	seriesMatchers := make([]index.SeriesMatcher, len(series))
	for i := range series {
		seriesMatchers[i], err = convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return nil, err
		}
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "seriesIndex.searchPrimary")
		span.Tagf("matchers", "%v", seriesMatchers)
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	ss, err := s.store.Search(ctx, seriesMatchers)
	if err != nil {
		return nil, err
	}
	result, err := convertIndexSeriesToSeriesList(ss)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to convert index series to series list, matchers: %v, matched: %d", seriesMatchers, len(ss))
	}
	if span != nil {
		span.Tagf("matched", "%d", len(result))
	}
	return result, nil
}

var emptySeriesMatcher = index.SeriesMatcher{}

func convertEntityValuesToSeriesMatcher(series *pbv1.Series) (index.SeriesMatcher, error) {
	var hasAny, hasWildcard bool
	var prefixIndex int

	for i, tv := range series.EntityValues {
		if tv == nil {
			return emptySeriesMatcher, errors.New("unexpected nil tag value")
		}
		if tv == pbv1.AnyTagValue {
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
		if hasWildcard {
			if err = series.MarshalWithWildcard(); err != nil {
				return emptySeriesMatcher, err
			}
			return index.SeriesMatcher{
				Type:  index.SeriesMatcherTypeWildcard,
				Match: series.Buffer,
			}, nil
		}
		series.EntityValues = series.EntityValues[:prefixIndex]
		if err = series.Marshal(); err != nil {
			return emptySeriesMatcher, err
		}
		return index.SeriesMatcher{
			Type:  index.SeriesMatcherTypePrefix,
			Match: series.Buffer,
		}, nil
	}
	if err = series.Marshal(); err != nil {
		return emptySeriesMatcher, err
	}
	return index.SeriesMatcher{
		Type:  index.SeriesMatcherTypeExact,
		Match: series.Buffer,
	}, nil
}

func convertIndexSeriesToSeriesList(indexSeries []index.Series) (pbv1.SeriesList, error) {
	seriesList := make(pbv1.SeriesList, 0, len(indexSeries))
	for _, s := range indexSeries {
		var series pbv1.Series
		series.ID = s.ID
		if err := series.Unmarshal(s.EntityValues); err != nil {
			return nil, errors.WithMessagef(err, "failed to unmarshal series: %s", s.EntityValues)
		}
		seriesList = append(seriesList, &series)
	}
	return seriesList, nil
}

func (s *seriesIndex) Search(ctx context.Context, series []*pbv1.Series, blugeQuery *inverted.BlugeQuery,
	filter index.Filter, order *model.OrderBy, preloadSize int,
) (sl pbv1.SeriesList, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		var span *query.Span
		span, ctx = tracer.StartSpan(ctx, "seriesIndex.Search")
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	seriesList, err := s.searchPrimary(ctx, series)
	if err != nil {
		return nil, err
	}

	pl := seriesList.ToList()
	if blugeQuery != nil {
		var plFilter posting.List
		func() {
			if tracer != nil {
				span, _ := tracer.StartSpan(ctx, "filter")
				span.Tag("exp", filter.String())
				defer func() {
					if err != nil {
						span.Error(err)
					} else {
						span.Tagf("matched", "%d", plFilter.Len())
						span.Tagf("total", "%d", pl.Len())
					}
					span.Stop()
				}()
			}
			if plFilter, err = s.store.Execute(blugeQuery); err != nil {
				return
			}
			if plFilter == nil {
				return
			}
			err = pl.Intersect(plFilter)
		}()
		if err != nil {
			return nil, err
		}
	}

	if order == nil || order.Index == nil {
		return filterSeriesList(seriesList, pl), nil
	}

	fieldKey := index.FieldKey{
		IndexRuleID: order.Index.GetMetadata().Id,
	}
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "sort")
		span.Tagf("preload", "%d", preloadSize)
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	iter, err := s.store.Iterator(fieldKey, rangeOpts, order.Sort, preloadSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var sortedSeriesList pbv1.SeriesList
	var r int
	for iter.Next() {
		r++
		docID := iter.Val().DocID
		if !pl.Contains(docID) {
			continue
		}
		sortedSeriesList = appendSeriesList(sortedSeriesList, seriesList, common.SeriesID(docID))
		if err != nil {
			return nil, err
		}
	}
	if span != nil {
		span.Tagf("rounds", "%d", r)
		span.Tagf("size", "%d", len(sortedSeriesList))
	}
	return sortedSeriesList, err
}

func filterSeriesList(seriesList pbv1.SeriesList, filter posting.List) pbv1.SeriesList {
	for i := 0; i < len(seriesList); i++ {
		if !filter.Contains(uint64(seriesList[i].ID)) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			i--
		}
	}
	return seriesList
}

func appendSeriesList(dest, src pbv1.SeriesList, target common.SeriesID) pbv1.SeriesList {
	for i := 0; i < len(src); i++ {
		if target == src[i].ID {
			dest = append(dest, src[i])
			break
		}
	}
	return dest
}

func (s *seriesIndex) Close() error {
	return s.store.Close()
}
