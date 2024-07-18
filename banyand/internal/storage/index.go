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
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func (d *database[T, O]) IndexDB() IndexDB {
	return d.indexController.getHot()
}

func (d *database[T, O]) Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	return d.indexController.searchPrimary(ctx, series)
}

type seriesIndex struct {
	startTime time.Time
	store     index.SeriesStore
	l         *logger.Logger
	path      string
}

func newSeriesIndex(ctx context.Context, path string, startTime time.Time, flushTimeoutSeconds int64) (*seriesIndex, error) {
	si := &seriesIndex{
		path:      path,
		startTime: startTime,
		l:         logger.Fetch(ctx, "series_index"),
	}
	var err error
	if si.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:         path,
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

func (s *seriesIndex) Search(ctx context.Context, series []*pbv1.Series, filter index.Filter, order *pbv1.OrderBy, preloadSize int) (sl pbv1.SeriesList, err error) {
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
	if filter != nil && filter != logical.ENode {
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
			if plFilter, err = filter.Execute(func(_ databasev1.IndexRule_Type) (index.Searcher, error) {
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

type seriesIndexController[T TSTable, O any] struct {
	clock           timestamp.Clock
	hot             *seriesIndex
	standby         *seriesIndex
	l               *logger.Logger
	location        string
	opts            TSDBOpts[T, O]
	standbyLiveTime time.Duration
	sync.RWMutex
}

func newSeriesIndexController[T TSTable, O any](
	ctx context.Context,
	opts TSDBOpts[T, O],
) (*seriesIndexController[T, O], error) {
	l := logger.Fetch(ctx, "seriesIndexController")
	clock, ctx := timestamp.GetClock(ctx)
	var standbyLiveTime time.Duration
	switch opts.TTL.Unit {
	case HOUR:
		standbyLiveTime = time.Hour
	case DAY:
		standbyLiveTime = 24 * time.Hour
	default:
	}
	sic := &seriesIndexController[T, O]{
		opts:            opts,
		clock:           clock,
		standbyLiveTime: standbyLiveTime,
		location:        filepath.Clean(opts.Location),
		l:               l,
	}
	idxName, err := sic.loadIdx()
	if err != nil {
		return nil, err
	}
	switch len(idxName) {
	case 0:
		if sic.hot, err = sic.newIdx(ctx, clock.Now()); err != nil {
			return nil, err
		}
	case 1:
		if sic.hot, err = sic.openIdx(ctx, idxName[0]); err != nil {
			return nil, err
		}
	case 2:
		if sic.hot, err = sic.openIdx(ctx, idxName[0]); err != nil {
			return nil, err
		}
		if sic.standby, err = sic.openIdx(ctx, idxName[1]); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unexpected series index count")
	}
	return sic, nil
}

func (sic *seriesIndexController[T, O]) getHot() *seriesIndex {
	sic.RLock()
	defer sic.RUnlock()
	return sic.hot
}

func (sic *seriesIndexController[T, O]) loadIdx() ([]string, error) {
	idxName := make([]string, 0)
	if err := walkDir(
		sic.location,
		"idx",
		func(suffix string) error {
			idxName = append(idxName, "idx-"+suffix)
			return nil
		}); err != nil {
		return nil, err
	}
	sort.StringSlice(idxName).Sort()
	if len(idxName) > 2 {
		redundantIdx := idxName[:len(idxName)-2]
		for i := range redundantIdx {
			lfs.MustRMAll(filepath.Join(sic.location, redundantIdx[i]))
		}
		idxName = idxName[len(idxName)-2:]
	}
	return idxName, nil
}

func (sic *seriesIndexController[T, O]) newIdx(ctx context.Context, now time.Time) (*seriesIndex, error) {
	ts := sic.opts.TTL.Unit.standard(now)
	return sic.openIdx(ctx, fmt.Sprintf("idx-%016x", ts.UnixNano()))
}

func (sic *seriesIndexController[T, O]) newNextIdx(ctx context.Context, now time.Time) (*seriesIndex, error) {
	ts := sic.opts.TTL.Unit.standard(now)
	ts = ts.Add(sic.standbyLiveTime)
	return sic.openIdx(ctx, fmt.Sprintf("idx-%016x", ts.UnixNano()))
}

func (sic *seriesIndexController[T, O]) openIdx(ctx context.Context, name string) (*seriesIndex, error) {
	p := path.Join(sic.location, name)
	if ts, ok := strings.CutPrefix(name, "idx-"); ok {
		t, err := strconv.ParseInt(ts, 16, 64)
		if err != nil {
			return nil, err
		}

		return newSeriesIndex(ctx, p, sic.opts.TTL.Unit.standard(time.Unix(0, t)), sic.opts.SeriesIndexFlushTimeoutSeconds)
	}
	return nil, errors.New("unexpected series index name")
}

func (sic *seriesIndexController[T, O]) run(now, deadline time.Time) (err error) {
	ctx := context.WithValue(context.Background(), logger.ContextKey, sic.l)
	if _, err := sic.loadIdx(); err != nil {
		sic.l.Warn().Err(err).Msg("fail to clear redundant series index")
	}

	sic.Lock()
	defer sic.Unlock()
	if sic.hot.startTime.Compare(deadline) <= 0 {
		return sic.handleStandby(ctx, now, deadline)
	}

	if sic.standby != nil {
		return nil
	}
	liveTime := sic.hot.startTime.Sub(deadline)
	if liveTime <= 0 || liveTime > sic.standbyLiveTime {
		return nil
	}
	return sic.createStandby(ctx, now, deadline)
}

func (sic *seriesIndexController[T, O]) handleStandby(ctx context.Context, now, deadline time.Time) error {
	sic.l.Info().Time("deadline", deadline).Msg("start to swap series index")

	if sic.standby == nil {
		var err error
		sic.standby, err = sic.newIdx(ctx, now)
		if err != nil {
			return err
		}
	}

	standby := sic.hot
	sic.hot = sic.standby
	sic.standby = nil

	if err := standby.Close(); err != nil {
		sic.l.Warn().Err(err).Msg("fail to close standby series index")
	}

	lfs.MustRMAll(standby.path)
	sic.l.Info().Str("path", standby.path).Msg("dropped series index")
	lfs.SyncPath(sic.location)

	return nil
}

func (sic *seriesIndexController[T, O]) createStandby(ctx context.Context, now, deadline time.Time) error {
	if sic.standby != nil {
		return nil
	}
	sic.l.Info().Time("deadline", deadline).Msg("start to create standby series index")
	standby, err := sic.newNextIdx(ctx, now)
	if err != nil {
		sic.l.Error().Err(err).Msg("fail to create standby series index")
		return err
	}

	sic.standby = standby
	return nil
}

func (sic *seriesIndexController[T, O]) Write(docs index.Documents) error {
	sic.RLock()
	defer sic.RUnlock()
	if sic.standby != nil {
		return sic.standby.Write(docs)
	}
	return sic.hot.Write(docs)
}

func (sic *seriesIndexController[T, O]) searchPrimary(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error) {
	sic.RLock()
	defer sic.RUnlock()

	sl, err := sic.hot.searchPrimary(ctx, series)
	if err != nil {
		return nil, err
	}
	if len(sl) > 0 || sic.standby == nil {
		return sl, nil
	}
	return sic.standby.searchPrimary(ctx, series)
}

func (sic *seriesIndexController[T, O]) Search(ctx context.Context, series []*pbv1.Series,
	filter index.Filter, order *pbv1.OrderBy, preloadSize int,
) (pbv1.SeriesList, error) {
	sic.RLock()
	defer sic.RUnlock()

	sl, err := sic.hot.Search(ctx, series, filter, order, preloadSize)
	if err != nil {
		return nil, err
	}
	if len(sl) > 0 || sic.standby == nil {
		return sl, nil
	}
	return sic.standby.Search(ctx, series, filter, order, preloadSize)
}

func (sic *seriesIndexController[T, O]) Close() error {
	sic.Lock()
	defer sic.Unlock()
	if sic.standby != nil {
		return multierr.Combine(sic.hot.Close(), sic.standby.Close())
	}
	return sic.hot.Close()
}
