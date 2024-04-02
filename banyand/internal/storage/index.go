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
	"os"
	"path/filepath"
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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func (d *database[T, O]) IndexDB() IndexDB {
	return d.indexController.hot
}

func (d *database[T, O]) Lookup(ctx context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
	return d.indexController.searchPrimary(ctx, series)
}

type seriesIndex struct {
	l     *logger.Logger
	store index.SeriesStore
	name  string
}

func newSeriesIndex(ctx context.Context, path, name string, flushTimeoutSeconds int64) (*seriesIndex, error) {
	si := &seriesIndex{
		name: name,
		l:    logger.Fetch(ctx, "series_index"),
	}
	var err error
	if si.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:         filepath.Join(path, name),
		Logger:       si.l,
		BatchWaitSec: flushTimeoutSeconds,
	}); err != nil {
		return nil, err
	}
	return si, nil
}

func (s *seriesIndex) Write(docs index.Documents) error {
	applied := make(chan struct{})
	err := s.store.Batch(index.Batch{
		Documents: docs,
		Applied:   applied,
	})
	if err != nil {
		return err
	}
	<-applied
	return nil
}

var rangeOpts = index.RangeOpts{}

func (s *seriesIndex) searchPrimary(_ context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
	var hasAny, hasWildcard bool
	var prefixIndex int

	for i, tv := range series.EntityValues {
		if tv == nil {
			return nil, errors.New("nil tag value")
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
		var ss []index.Series
		if hasWildcard {
			if err = series.Marshal(); err != nil {
				return nil, err
			}
			ss, err = s.store.SearchWildcard(series.Buffer)
			if err != nil {
				return nil, err
			}
			return convertIndexSeriesToSeriesList(ss)
		}
		series.EntityValues = series.EntityValues[:prefixIndex]
		if err = series.Marshal(); err != nil {
			return nil, err
		}
		ss, err = s.store.SearchPrefix(series.Buffer)
		if err != nil {
			return nil, err
		}
		return convertIndexSeriesToSeriesList(ss)
	}
	if err = series.Marshal(); err != nil {
		return nil, err
	}
	var seriesID common.SeriesID
	seriesID, err = s.store.Search(series.Buffer)
	if err != nil {
		return nil, err
	}
	if seriesID > 0 {
		series.ID = seriesID
		return pbv1.SeriesList{series}, nil
	}
	return nil, nil
}

func convertIndexSeriesToSeriesList(indexSeries []index.Series) (pbv1.SeriesList, error) {
	seriesList := make(pbv1.SeriesList, 0, len(indexSeries))
	for _, s := range indexSeries {
		var series pbv1.Series
		series.ID = s.ID
		if err := series.Unmarshal(s.EntityValues); err != nil {
			return nil, err
		}
		seriesList = append(seriesList, &series)
	}
	return seriesList, nil
}

func (s *seriesIndex) Search(ctx context.Context, series *pbv1.Series, filter index.Filter, order *pbv1.OrderBy, preloadSize int) (pbv1.SeriesList, error) {
	seriesList, err := s.searchPrimary(ctx, series)
	if err != nil {
		return nil, err
	}

	pl := seriesList.ToList()
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

	if order == nil || order.Index == nil {
		return filterSeriesList(seriesList, pl), nil
	}

	fieldKey := index.FieldKey{
		IndexRuleID: order.Index.GetMetadata().Id,
	}
	iter, err := s.store.Iterator(fieldKey, rangeOpts, order.Sort, preloadSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	var sortedSeriesList pbv1.SeriesList
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

func filterSeriesList(seriesList pbv1.SeriesList, filter posting.List) pbv1.SeriesList {
	for i := 0; i < len(seriesList); i++ {
		if !filter.Contains(uint64(seriesList[i].ID)) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			i--
		}
	}
	return seriesList
}

func appendSeriesList(dest, src pbv1.SeriesList, filter posting.List) pbv1.SeriesList {
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

type seriesIndexController[T TSTable, O any] struct {
	ctx     context.Context
	hot     *seriesIndex
	standby *seriesIndex
	timestamp.TimeRange
	l    *logger.Logger
	opts TSDBOpts[T, O]
	sync.RWMutex
}

func standard(t time.Time, unit IntervalUnit) time.Time {
	switch unit {
	case HOUR:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case DAY:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}
	panic("invalid interval unit")
}

func newSeriesIndexController[T TSTable, O any](
	ctx context.Context,
	opts TSDBOpts[T, O],
) (*seriesIndexController[T, O], error) {
	var hpath, spath string
	l := logger.Fetch(ctx, "seriesIndexController")
	startTime := standard(time.Now(), opts.TTL.Unit)
	endTime := startTime.Add(opts.TTL.estimatedDuration())
	timeRange := timestamp.NewSectionTimeRange(startTime, endTime)
	location := filepath.Clean(opts.Location)

	idxName := make([]string, 0)
	if err := walkDir(
		location,
		"idx",
		func(suffix string) error {
			idxName = append(idxName, "idx-"+suffix)
			return nil
		}); err != nil {
		return nil, err
	}
	if len(idxName) != 0 {
		hpath = idxName[0]
	} else {
		hpath = fmt.Sprintf("idx-%016x", time.Now().UnixNano())
	}
	h, err := newSeriesIndex(ctx, location, hpath, opts.SeriesIndexFlushTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	sir := &seriesIndexController[T, O]{
		hot:       h,
		ctx:       ctx,
		opts:      opts,
		TimeRange: timeRange,
		l:         l,
	}
	if len(idxName) == 2 {
		spath = idxName[1]
		sb, err := newSeriesIndex(ctx, location, spath, opts.SeriesIndexFlushTimeoutSeconds)
		if err != nil {
			return nil, err
		}
		sir.standby = sb
	}
	return sir, nil
}

func (sir *seriesIndexController[T, O]) run(deadline time.Time) (err error) {
	sir.l.Info().Time("deadline", deadline).Msg("start to swap series index")
	if sir.End.Before(deadline) {
		sir.Lock()
		defer sir.Unlock()

		sir.hot, sir.standby = sir.standby, sir.hot
		go func() {
			<-time.After(time.Hour)
			sir.Lock()
			defer sir.Unlock()
			err = sir.standby.Close()
			if err != nil {
				sir.l.Error().Msg("fail to close standby series index")
			}
			location := filepath.Clean(sir.opts.Location)
			root := filepath.Join(location, sir.standby.name)
			err = os.RemoveAll(root)
			if err != nil {
				sir.l.Error().Msg("fail to remove expired standby directory")
			}
			sir.standby = nil
		}()

		startTime := standard(time.Now(), sir.opts.TTL.Unit)
		endTime := startTime.Add(sir.opts.TTL.estimatedDuration())
		sir.TimeRange = timestamp.NewSectionTimeRange(startTime, endTime)
	}
	if sir.End.Sub(deadline) < time.Hour {
		location := filepath.Clean(sir.opts.Location)
		path := fmt.Sprintf("idx-%016x", time.Now().UnixNano())
		sir.standby, err = newSeriesIndex(sir.ctx, location, path, sir.opts.SeriesIndexFlushTimeoutSeconds)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sir *seriesIndexController[T, O]) Write(docs index.Documents) error {
	sir.Lock()
	defer sir.Unlock()
	if sir.standby != nil {
		err := sir.standby.Write(docs)
		if err != nil {
			sir.l.Error().Msg("fail to write docs in standby series index")
		}
	}
	return sir.hot.Write(docs)
}

func (sir *seriesIndexController[T, O]) searchPrimary(ctx context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
	sir.RLock()
	defer sir.RUnlock()

	sl, err := sir.hot.searchPrimary(ctx, series)
	if err != nil {
		return nil, err
	}
	if len(sl) > 0 || sir.standby == nil {
		return sl, nil
	}
	return sir.standby.searchPrimary(ctx, series)
}

func (sir *seriesIndexController[T, O]) Search(ctx context.Context, series *pbv1.Series, filter index.Filter, order *pbv1.OrderBy) (pbv1.SeriesList, error) {
	sir.RLock()
	defer sir.RUnlock()

	sl, err := sir.hot.Search(ctx, series, filter, order)
	if err != nil {
		return nil, err
	}
	if len(sl) > 0 || sir.standby == nil {
		return sl, nil
	}
	return sir.standby.Search(ctx, series, filter, order)
}

func (sir *seriesIndexController[T, O]) Close() error {
	sir.Lock()
	defer sir.Unlock()
	if sir.standby != nil {
		err := sir.standby.Close()
		if err != nil {
			return err
		}
	}
	return sir.hot.Close()
}
