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
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/valyala/fastrand"
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
	store index.SeriesStore
	l     *logger.Logger
}

func newSeriesIndex(ctx context.Context, path string, flushTimeoutSeconds int64) (*seriesIndex, error) {
	si := &seriesIndex{
		l: logger.Fetch(ctx, "series_index"),
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

func (s *seriesIndex) Search(ctx context.Context, series *pbv1.Series, filter index.Filter, order *pbv1.OrderBy) (pbv1.SeriesList, error) {
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
	iter, err := s.store.Iterator(fieldKey, rangeOpts, order.Sort)
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
	ctx       context.Context
	hot       *seriesIndex
	standby   *seriesIndex
	scheduler *timestamp.Scheduler
	l         *logger.Logger
	opts      TSDBOpts[T, O]
}

func newSeriesIndexController[T TSTable, O any](
	ctx context.Context,
	opts TSDBOpts[T, O],
) (*seriesIndexController[T, O], error) {
	location := filepath.Clean(opts.Location)
	path := fmt.Sprintf("idx-%v", fastrand.Uint32())
	h, err := newSeriesIndex(ctx, filepath.Join(location, path), opts.SeriesIndexFlushTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	path = fmt.Sprintf("idx-%v", fastrand.Uint32())
	sb, err := newSeriesIndex(ctx, filepath.Join(location, path), opts.SeriesIndexFlushTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	l := logger.Fetch(ctx, "seriesIndexController")

	var expr string
	switch opts.TTL.Unit {
	case HOUR:
		expr = "5 *"
	case DAY:
		expr = "5 0"
	}
	clock, _ := timestamp.GetClock(ctx)
	scheduler := timestamp.NewScheduler(l, clock)
	sir := &seriesIndexController[T, O]{
		hot:       h,
		standby:   sb,
		ctx:       ctx,
		scheduler: scheduler,
		l:         l,
	}
	if err := scheduler.Register("seriesIndexController", cron.Minute|cron.Hour, expr, sir.run); err != nil {
		return nil, err
	}
	return sir, nil
}

func (sir *seriesIndexController[T, O]) run(_ time.Time, l *logger.Logger) bool {
	err := sir.hot.Close()
	if err != nil {
		l.Error().Msg("fail to close old hot series index")
		return false
	}
	sir.hot = sir.standby
	location := filepath.Clean(sir.opts.Location)
	path := fmt.Sprintf("idx-%v", fastrand.Uint32())
	sir.standby, err = newSeriesIndex(sir.ctx, filepath.Join(location, path), sir.opts.SeriesIndexFlushTimeoutSeconds)
	if err != nil {
		l.Error().Msg("fail to create series index")
		return false
	}
	return true
}

func (sir *seriesIndexController[T, O]) Write(docs index.Documents) error {
	return sir.hot.Write(docs)
}

func (sir *seriesIndexController[T, O]) searchPrimary(ctx context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
	return sir.hot.searchPrimary(ctx, series)
}

func (sir *seriesIndexController[T, O]) Search(ctx context.Context, series *pbv1.Series, filter index.Filter, order *pbv1.OrderBy) (pbv1.SeriesList, error) {
	return sir.hot.Search(ctx, series, filter, order)
}

func (sir *seriesIndexController[T, O]) Close() error {
	return sir.hot.Close()
}
