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
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func (d *database[T, O]) IndexDB() IndexDB {
	return d.indexController.hot
}

func (d *database[T, O]) Lookup(ctx context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
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
		seriesID, _ := iter.Val()
		if !pl.Contains(seriesID) {
			continue
		}
		sortedSeriesList = appendSeriesList(sortedSeriesList, seriesList, common.SeriesID(seriesID))
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
		if sic.hot, err = sic.newIdx(ctx); err != nil {
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

func (sic *seriesIndexController[T, O]) newIdx(ctx context.Context) (*seriesIndex, error) {
	return sic.openIdx(ctx, fmt.Sprintf("idx-%016x", time.Now().UnixNano()))
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

func (sic *seriesIndexController[T, O]) run(deadline time.Time) (err error) {
	var standby *seriesIndex
	ctx := context.WithValue(context.Background(), logger.ContextKey, sic.l)
	_, err = sic.loadIdx()
	if err != nil {
		sic.l.Warn().Err(err).Msg("fail to clear redundant series index")
	}
	if sic.hot.startTime.Before(deadline) {
		sic.l.Info().Time("deadline", deadline).Msg("start to swap series index")
		sic.Lock()
		if sic.standby == nil {
			sic.standby, err = sic.newIdx(ctx)
			if err != nil {
				sic.Unlock()
				return err
			}
		}
		standby = sic.hot
		sic.hot = sic.standby
		sic.standby = nil
		sic.Unlock()
		err = standby.Close()
		if err != nil {
			sic.l.Warn().Err(err).Msg("fail to close standby series index")
		}
		lfs.MustRMAll(standby.path)
		sic.l.Info().Str("path", standby.path).Msg("dropped series index")
		lfs.SyncPath(sic.location)
	}

	liveTime := sic.hot.startTime.Sub(deadline)
	if liveTime > 0 && liveTime <= sic.standbyLiveTime {
		sic.l.Info().Time("deadline", deadline).Msg("start to create standby series index")
		standby, err = sic.newIdx(ctx)
		if err != nil {
			return err
		}
		sic.Lock()
		sic.standby = standby
		sic.Unlock()
	}
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

func (sic *seriesIndexController[T, O]) searchPrimary(ctx context.Context, series *pbv1.Series) (pbv1.SeriesList, error) {
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

func (sic *seriesIndexController[T, O]) Search(ctx context.Context, series *pbv1.Series,
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
