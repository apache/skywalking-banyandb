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
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func (s *segment[T, O]) IndexDB() IndexDB {
	return s.index
}

func (s *segment[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	sl, _, err := s.index.searchPrimary(ctx, series, nil)
	return sl, err
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

func (s *seriesIndex) searchPrimary(ctx context.Context, series []*pbv1.Series, projection []index.FieldKey) (sl pbv1.SeriesList, fields FieldResultList, err error) {
	seriesMatchers := make([]index.SeriesMatcher, len(series))
	for i := range series {
		seriesMatchers[i], err = convertEntityValuesToSeriesMatcher(series[i])
		if err != nil {
			return nil, nil, err
		}
	}
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "seriesIndex.searchPrimary")
		span.Tagf("matchers", "%v", seriesMatchers)
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
	ss, err := s.store.Search(ctx, seriesMatchers, projection)
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
	seriesList, fieldResultList, err := s.searchPrimary(ctx, series, opts.Projection)
	if err != nil {
		return nil, nil, err
	}

	pl := seriesList.ToList()
	if opts.Filter != nil && opts.Filter != logical.ENode {
		var plFilter posting.List
		func() {
			if tracer != nil {
				span, _ := tracer.StartSpan(ctx, "filter")
				span.Tag("exp", opts.Filter.String())
				var projectionStrBuilder strings.Builder
				if len(opts.Projection) > 0 {
					projectionStrBuilder.WriteString("[")
					for i, p := range opts.Projection {
						if i > 0 {
							projectionStrBuilder.WriteString(", ")
						}
						projectionStrBuilder.WriteRune(rune(p.IndexRuleID))
					}
					projectionStrBuilder.WriteString("]")
					span.Tagf("projection", "%s", projectionStrBuilder.String())
				}
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
			if plFilter, err = opts.Filter.Execute(func(_ databasev1.IndexRule_Type) (index.Searcher, error) {
				return s.store, nil
			}, 0); err != nil {
				return
			}
			if plFilter == nil {
				return
			}
			err = pl.Intersect(plFilter)
		}()
		if err != nil {
			return nil, nil, err
		}
	}

	if opts.Order == nil || opts.Order.Index == nil {
		sl, frl = filterSeriesList(seriesList, fieldResultList, pl)
		return sl, frl, nil
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
	iter, err := s.store.Iterator(fieldKey, rangeOpts,
		opts.Order.Sort, opts.PreloadSize)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var sortedSeriesList pbv1.SeriesList
	var sortedFieldResultList FieldResultList
	var r int
	for iter.Next() {
		r++
		docID := iter.Val().DocID
		if !pl.Contains(docID) {
			continue
		}
		sortedSeriesList, sortedFieldResultList = appendSeriesList(
			sortedSeriesList, seriesList,
			sortedFieldResultList, fieldResultList,
			common.SeriesID(docID))
		if err != nil {
			return nil, nil, err
		}
	}
	if span != nil {
		span.Tagf("rounds", "%d", r)
		span.Tagf("size", "%d", len(sortedSeriesList))
	}
	return sortedSeriesList, sortedFieldResultList, err
}

func filterSeriesList(seriesList pbv1.SeriesList, fieldResultList FieldResultList, filter posting.List) (pbv1.SeriesList, FieldResultList) {
	for i := 0; i < len(seriesList); i++ {
		if !filter.Contains(uint64(seriesList[i].ID)) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			if fieldResultList != nil {
				fieldResultList = append(fieldResultList[:i], fieldResultList[i+1:]...)
			}
			i--
		}
	}
	return seriesList, fieldResultList
}

func appendSeriesList(dest, src pbv1.SeriesList, destFRL, srcFRL FieldResultList, target common.SeriesID) (pbv1.SeriesList, FieldResultList) {
	for i := 0; i < len(src); i++ {
		if target == src[i].ID {
			dest = append(dest, src[i])
			if srcFRL != nil {
				destFRL = append(destFRL, srcFRL[i])
			}
			break
		}
	}
	return dest, destFRL
}

func (s *seriesIndex) Close() error {
	return s.store.Close()
}
