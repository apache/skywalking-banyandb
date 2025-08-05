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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const seriesIndexDirName = "sidx"

func (s *segment[T, O]) IndexDB() IndexDB {
	return s.index
}

func (s *segment[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	sl, err := s.index.filter(ctx, series, nil, nil, nil)
	return sl.SeriesList, err
}

type seriesIndex struct {
	store   index.SeriesStore
	l       *logger.Logger
	metrics *inverted.Metrics
	p       common.Position
}

func newSeriesIndex(ctx context.Context, root string, flushTimeoutSeconds int64, cacheMaxBytes int,
	metrics *inverted.Metrics,
) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
		p: common.GetPosition(ctx),
	}
	opts := inverted.StoreOpts{
		Path:                   path.Join(root, seriesIndexDirName),
		Logger:                 si.l,
		BatchWaitSec:           flushTimeoutSeconds,
		CacheMaxBytes:          cacheMaxBytes,
		EnableDeduplication:    true,
		ExternalSegmentTempDir: path.Join(root, inverted.ExternalSegmentTempDirName),
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

func (s *seriesIndex) Insert(docs index.Documents) error {
	return s.store.InsertSeriesBatch(index.Batch{
		Documents: docs,
	})
}

func (s *seriesIndex) Update(docs index.Documents) error {
	return s.store.UpdateSeriesBatch(index.Batch{
		Documents: docs,
	})
}

func (s *seriesIndex) EnableExternalSegments() (index.ExternalSegmentStreamer, error) {
	return s.store.EnableExternalSegments()
}

func (s *seriesIndex) filter(ctx context.Context, series []*pbv1.Series,
	projection []index.FieldKey, secondaryQuery index.Query, timeRange *timestamp.TimeRange,
) (data SeriesData, err error) {
	if len(series) == 0 && secondaryQuery == nil {
		return data, nil
	}
	var seriesMatchers []index.SeriesMatcher
	if len(series) > 0 {
		seriesMatchers = make([]index.SeriesMatcher, len(series))
		for i := range series {
			seriesMatchers[i], err = convertEntityValuesToSeriesMatcher(series[i])
			if err != nil {
				return SeriesData{}, err
			}
		}
	}

	indexQuery, err := s.store.BuildQuery(seriesMatchers, secondaryQuery, timeRange)
	if err != nil {
		return SeriesData{}, err
	}
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "seriesIndex.search")
		span.Tagf("query", "%s", indexQuery.String())
		defer func() {
			span.Tagf("matched", "%d", len(data.SeriesList))
			if len(data.Fields) > 0 {
				span.Tagf("field_length", "%d", len(data.Fields[0]))
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	ss, err := s.store.Search(ctx, projection, indexQuery, 0)
	if err != nil {
		return SeriesData{}, err
	}
	if len(ss) == 0 {
		return SeriesData{}, nil
	}
	data.SeriesList, data.Fields, data.Timestamps, data.Versions, err = convertIndexSeriesToSeriesList(ss, len(projection) > 0)
	if err != nil {
		return SeriesData{}, errors.WithMessagef(err, "failed to convert index series to series list, matchers: %v, matched: %d", seriesMatchers, len(ss))
	}
	return data, nil
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

func convertIndexSeriesToSeriesList(indexSeries []index.SeriesDocument, hasFields bool) (pbv1.SeriesList, FieldResultList, []int64, []int64, error) {
	seriesList := make(pbv1.SeriesList, 0, len(indexSeries))
	var fields FieldResultList
	if hasFields {
		fields = make(FieldResultList, 0, len(indexSeries))
	}
	var timestamps, versions []int64
	for _, s := range indexSeries {
		var series pbv1.Series
		if err := series.Unmarshal(s.Key.EntityValues); err != nil {
			return nil, nil, nil, nil, errors.WithMessagef(err, "failed to unmarshal series: %s", s.Key.EntityValues)
		}
		seriesList = append(seriesList, &series)
		if fields != nil {
			fields = append(fields, s.Fields)
		}
		if s.Timestamp > 0 {
			timestamps = append(timestamps, s.Timestamp)
		}
		if s.Version > 0 {
			versions = append(versions, s.Version)
		}
	}
	return seriesList, fields, timestamps, versions, nil
}

func (s *seriesIndex) Search(ctx context.Context, series []*pbv1.Series, opts IndexSearchOpts,
) (sd SeriesData, sortedValues [][]byte, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		var span *query.Span
		span, ctx = tracer.StartSpan(ctx, "seriesIndex.Search")
		if opts.Query != nil {
			span.Tagf("secondary_query", "%s", opts.Query.String())
		}
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}

	if opts.Order == nil || opts.Order.Index == nil {
		sd, err = s.filter(ctx, series, opts.Projection, opts.Query, opts.TimeRange)
		if err != nil {
			return sd, nil, err
		}
		return sd, nil, nil
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
			return sd, nil, err
		}
	}
	query, err := s.store.BuildQuery(seriesMatchers, opts.Query, opts.TimeRange)
	if err != nil {
		return sd, nil, err
	}
	iter, err := s.store.SeriesSort(ctx, query, opts.Order,
		opts.PreloadSize, opts.Projection)
	if err != nil {
		return sd, nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var r int
	for iter.Next() {
		r++
		val := iter.Val()
		var series pbv1.Series
		if err = series.Unmarshal(val.EntityValues); err != nil {
			return sd, nil, errors.WithMessagef(err, "failed to unmarshal series: %s", val.EntityValues)
		}
		sd.SeriesList = append(sd.SeriesList, &series)
		sd.Timestamps = append(sd.Timestamps, val.Timestamp)
		sd.Versions = append(sd.Versions, val.Version)
		if len(opts.Projection) > 0 {
			sd.Fields = append(sd.Fields, maps.Clone(val.Values))
		}
		sortedValues = append(sortedValues, val.SortedValue)
	}
	if span != nil {
		span.Tagf("query", "%s", iter.Query().String())
		span.Tagf("rounds", "%d", r)
		span.Tagf("size", "%d", len(sd.SeriesList))
	}
	return sd, sortedValues, err
}

func (s *seriesIndex) SearchWithoutSeries(ctx context.Context, opts IndexSearchOpts) (sd SeriesData, sortedValues [][]byte, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		var span *query.Span
		span, ctx = tracer.StartSpan(ctx, "seriesIndex.SearchWithoutSeries")
		if opts.Query != nil {
			span.Tagf("secondary_query", "%s", opts.Query.String())
		}
		defer func() {
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}

	if opts.Order == nil || opts.Order.Index == nil {
		sd, err = s.filter(ctx, nil, opts.Projection, opts.Query, opts.TimeRange)
		if err != nil {
			return sd, nil, err
		}
		return sd, nil, nil
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

	iter, err := s.store.SeriesSort(ctx, opts.Query, opts.Order,
		opts.PreloadSize, opts.Projection)
	if err != nil {
		return sd, nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var r int
	for iter.Next() {
		r++
		val := iter.Val()
		var series pbv1.Series
		if err = series.Unmarshal(val.EntityValues); err != nil {
			return sd, nil, errors.WithMessagef(err, "failed to unmarshal series: %s", val.EntityValues)
		}
		sd.SeriesList = append(sd.SeriesList, &series)
		sd.Timestamps = append(sd.Timestamps, val.Timestamp)
		sd.Versions = append(sd.Versions, val.Version)
		if len(opts.Projection) > 0 {
			sd.Fields = append(sd.Fields, maps.Clone(val.Values))
		}
		sortedValues = append(sortedValues, val.SortedValue)
	}
	if span != nil {
		span.Tagf("query", "%s", iter.Query().String())
		span.Tagf("rounds", "%d", r)
		span.Tagf("size", "%d", len(sd.SeriesList))
	}
	return sd, sortedValues, err
}

func (s *seriesIndex) Close() error {
	s.metrics.DeleteAll(s.p.SegLabelValues()...)
	return s.store.Close()
}
