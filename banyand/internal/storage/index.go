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
	"maps"
	"path"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

func (s *segment[T, O]) IndexDB() IndexDB {
	return s.index
}

func (s *segment[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	sl, _, err := s.index.filter(ctx, series, nil, nil)
	return sl, err
}

type seriesIndex struct {
	store   index.SeriesStore
	l       *logger.Logger
	metrics *inverted.Metrics
	p       common.Position
}

func newSeriesIndex(ctx context.Context, root string, flushTimeoutSeconds int64, metrics *inverted.Metrics) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
		p: common.GetPosition(ctx),
	}
	opts := inverted.StoreOpts{
		Path:         path.Join(root, "sidx"),
		Logger:       si.l,
		BatchWaitSec: flushTimeoutSeconds,
	}
	if metrics != nil {
		opts.Metrics = metrics
		si.metrics = opts.Metrics
	}
	var err error
	if si.store, err = inverted.NewStore(opts); err != nil {
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

func (s *seriesIndex) filter(ctx context.Context, series []*pbv1.Series,
	projection []index.FieldKey, secondaryQuery index.Query,
) (sl pbv1.SeriesList, fields FieldResultList, err error) {
	seriesMatchers := make([]index.SeriesMatcher, len(series))
	for i := range series {
		seriesMatchers[i], err = convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return nil, nil, err
		}
	}
	indexQuery, err := s.store.BuildQuery(seriesMatchers, secondaryQuery)
	if err != nil {
		return nil, nil, err
	}
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "seriesIndex.search")
		span.Tagf("query", "%s", indexQuery.String())
		defer func() {
			span.Tagf("matched", "%d", len(sl))
			if len(fields) > 0 {
				span.Tagf("field_length", "%d", len(fields[0]))
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	ss, err := s.store.Search(ctx, projection, indexQuery)
	if err != nil {
		return nil, nil, err
	}
	sl, fields, err = convertIndexSeriesToSeriesList(ss, len(projection) > 0)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to convert index series to series list, matchers: %v, matched: %d", seriesMatchers, len(ss))
	}
	return sl, fields, nil
}

var emptySeriesMatcher = index.SeriesMatcher{}

func convertEntityValuesToSeriesMatcher(series *pbv1.Series) (index.SeriesMatcher, error) {
	var hasAny, hasWildcard bool
	var prefixIndex int
	var localSeries pbv1.Series
	series.CopyTo(&localSeries)

	for i, tv := range localSeries.EntityValues {
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
			if err = localSeries.MarshalWithWildcard(); err != nil {
				return emptySeriesMatcher, err
			}
			return index.SeriesMatcher{
				Type:  index.SeriesMatcherTypeWildcard,
				Match: localSeries.Buffer,
			}, nil
		}
		localSeries.EntityValues = localSeries.EntityValues[:prefixIndex]
		if err = localSeries.Marshal(); err != nil {
			return emptySeriesMatcher, err
		}
		return index.SeriesMatcher{
			Type:  index.SeriesMatcherTypePrefix,
			Match: localSeries.Buffer,
		}, nil
	}
	if err = localSeries.Marshal(); err != nil {
		return emptySeriesMatcher, err
	}
	return index.SeriesMatcher{
		Type:  index.SeriesMatcherTypeExact,
		Match: localSeries.Buffer,
	}, nil
}

func convertIndexSeriesToSeriesList(indexSeries []index.SeriesDocument, hasFields bool) (pbv1.SeriesList, FieldResultList, error) {
	seriesList := make(pbv1.SeriesList, 0, len(indexSeries))
	var fields FieldResultList
	if hasFields {
		fields = make(FieldResultList, 0, len(indexSeries))
	}
	for _, s := range indexSeries {
		var series pbv1.Series
		series.ID = s.Key.ID
		if err := series.Unmarshal(s.Key.EntityValues); err != nil {
			return nil, nil, errors.WithMessagef(err, "failed to unmarshal series: %s", s.Key.EntityValues)
		}
		seriesList = append(seriesList, &series)
		if fields != nil {
			fields = append(fields, s.Fields)
		}
	}
	return seriesList, fields, nil
}

func (s *seriesIndex) Search(ctx context.Context, series []*pbv1.Series, opts IndexSearchOpts) (sl pbv1.SeriesList, frl FieldResultList, err error) {
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

	if opts.Order == nil || opts.Order.Index == nil {
		var seriesList pbv1.SeriesList
		var fieldResultList FieldResultList
		if opts.Query != nil {
			seriesList, fieldResultList, err = s.filter(ctx, series, opts.Projection, opts.Query)
		} else {
			seriesList, fieldResultList, err = s.filter(ctx, series, opts.Projection, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		return seriesList, fieldResultList, nil
	}

	fieldKey := index.FieldKey{
		IndexRuleID: opts.Order.Index.GetMetadata().Id,
	}
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "sort")
		span.Tagf("preload", "%d", opts.PreloadSize)
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	seriesMatchers := make([]index.SeriesMatcher, len(series))
	for i := range series {
		seriesMatchers[i], err = convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return nil, nil, err
		}
	}
	query, err := s.store.BuildQuery(seriesMatchers, opts.Query)
	if err != nil {
		return nil, nil, err
	}
	iter, err := s.store.Iterator(ctx, fieldKey, rangeOpts,
		opts.Order.Sort, opts.PreloadSize, query, opts.Projection)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var r int
	result := make([]index.SeriesDocument, 0, 10)
	for iter.Next() {
		r++
		val := iter.Val()
		var doc index.SeriesDocument
		doc.Fields = maps.Clone(val.Values)
		doc.Key.ID = common.SeriesID(val.DocID)
		doc.Key.EntityValues = val.EntityValues
		result = append(result, doc)
	}
	sortedSeriesList, sortedFieldResultList, err := convertIndexSeriesToSeriesList(result, len(opts.Projection) > 0)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to convert index series to series list, matchers: %v, matched: %d", seriesMatchers, len(result))
	}
	if span != nil {
		span.Tagf("query", "%s", iter.Query().String())
		span.Tagf("rounds", "%d", r)
		span.Tagf("size", "%d", len(sortedSeriesList))
	}
	return sortedSeriesList, sortedFieldResultList, err
}

func (s *seriesIndex) Close() error {
	s.metrics.DeleteAll(s.p.SegLabelValues()...)
	return s.store.Close()
}
