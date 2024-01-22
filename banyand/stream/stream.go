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

// Package stream implements a time-series-based storage which is consists of a sequence of element.
// Each element drops in a arbitrary interval. They are immutable, can not be updated or overwritten.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/schema"
)

const (
	maxValuesBlockSize              = 8 * 1024 * 1024
	maxTimestampsBlockSize          = 8 * 1024 * 1024
	maxElementIDsBlockSize          = 8 * 1024 * 1024
	maxTagFamiliesMetadataSize      = 8 * 1024 * 1024
	maxUncompressedBlockSize        = 2 * 1024 * 1024
	maxUncompressedPrimaryBlockSize = 128 * 1024

	maxBlockLength      = 8 * 1024
	defaultFlushTimeout = 5 * time.Second
)

type option struct {
	flushTimeout time.Duration
}

// Query allow to retrieve elements in a series of streams.
type Query interface {
	LoadGroup(name string) (schema.Group, bool)
	Stream(stream *commonv1.Metadata) (Stream, error)
}

// Stream allows inspecting elements' details.
type Stream interface {
	io.Closer
	GetSchema() *databasev1.Stream
	GetIndexRules() []*databasev1.IndexRule
	Query(ctx context.Context, opts pbv1.StreamQueryOptions) (pbv1.StreamQueryResult, error)
	Sort(ctx context.Context, opts pbv1.StreamSortOptions) (pbv1.StreamSortResult, error)
	Filter(ctx context.Context, opts pbv1.StreamFilterOptions) (pbv1.StreamFilterResult, error)
}

var _ Stream = (*stream)(nil)

type stream struct {
	databaseSupplier  schema.Supplier
	l                 *logger.Logger
	schema            *databasev1.Stream
	name              string
	group             string
	indexRules        []*databasev1.IndexRule
	indexRuleLocators []*partition.IndexRuleLocator
	shardNum          uint32
}

func (s *stream) GetSchema() *databasev1.Stream {
	return s.schema
}

func (s *stream) GetIndexRules() []*databasev1.IndexRule {
	return s.indexRules
}

func (s *stream) Close() error {
	return nil
}

func (s *stream) parseSpec() {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	s.indexRuleLocators = partition.ParseIndexRuleLocators(s.schema.GetTagFamilies(), s.indexRules)
}

func (s *stream) ParseElementIDDeprecated(item tsdb.Item) (string, error) {
	rawBytes, err := item.Val()
	if err != nil {
		return "", err
	}
	return string(rawBytes), nil
}

// NewItemIter returns a ItemIterator which mergers several tsdb.Iterator by input sorting order.
func NewItemIter(iters []*searcherIterator, s modelv1.Sort) itersort.Iterator[item] {
	var ii []itersort.Iterator[item]
	for _, iter := range iters {
		ii = append(ii, iter)
	}
	if s == modelv1.Sort_SORT_DESC {
		return itersort.NewItemIter[item](ii, true)
	}
	return itersort.NewItemIter[item](ii, false)
}

func (s *stream) Filter(ctx context.Context, sfo pbv1.StreamFilterOptions) (sfr pbv1.StreamFilterResult, err error) {
	if sfo.TimeRange == nil || sfo.Entities == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sfo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sfo.TimeRange)
	sort.Slice(tabWrappers, func(i, j int) bool {
		return tabWrappers[i].GetTimeRange().Start.Before(tabWrappers[j].GetTimeRange().Start)
	})
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	var seriesList pbv1.SeriesList
	for _, entity := range sfo.Entities {
		sl, lookupErr := tsdb.Lookup(ctx, &pbv1.Series{Subject: sfo.Name, EntityValues: entity})
		if lookupErr != nil {
			return nil, lookupErr
		}
		seriesList = seriesList.Merge(sl)
	}

	ces := newColumnElements()
	for _, tw := range tabWrappers {
		if len(ces.timestamp) >= sfo.MaxElementSize {
			break
		}
		index := tw.Table().Index()
		erl, err := index.Search(ctx, seriesList, sfo.Filter)
		if err != nil {
			return nil, err
		}
		if len(ces.timestamp)+len(erl) > sfo.MaxElementSize {
			erl = erl[:sfo.MaxElementSize-len(ces.timestamp)]
		}
		for _, er := range erl {
			e, err := tw.Table().getElement(er.seriesID, common.ItemID(er.timestamp), sfo.TagProjection)
			if err != nil {
				return nil, err
			}
			ces.BuildFromElement(e, sfo.TagProjection)
		}
	}
	return ces, nil
}

func (s *stream) Sort(ctx context.Context, sso pbv1.StreamSortOptions) (ssr pbv1.StreamSortResult, err error) {
	if sso.TimeRange == nil || sso.Entities == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sso.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sso.TimeRange)
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	var seriesList pbv1.SeriesList
	for _, entity := range sso.Entities {
		sl, lookupErr := tsdb.Lookup(ctx, &pbv1.Series{Subject: sso.Name, EntityValues: entity})
		if lookupErr != nil {
			return nil, lookupErr
		}
		seriesList = seriesList.Merge(sl)
	}

	var iters []*searcherIterator
	for _, series := range seriesList {
		seriesSpan := newSeriesSpan(ctx, sso.TimeRange, tabWrappers, series.ID)
		seekerBuilder := seriesSpan.Build()
		seekerBuilder.enhance(sso.Filter, sso.Order.Index, sso.Order.Sort, sso.TagProjection)
		seriesIters, buildErr := seekerBuilder.buildSeriesByIndex()
		if err != nil {
			return nil, buildErr
		}
		if len(seriesIters) > 0 {
			iters = append(iters, seriesIters...)
		}
	}

	if len(iters) == 0 {
		return ssr, nil
	}

	it := NewItemIter(iters, sso.Order.Sort)
	defer func() {
		err = multierr.Append(err, it.Close())
	}()

	ces := newColumnElements()
	for it.Next() {
		nextItem := it.Val()
		e, err := nextItem.Element()
		if err != nil {
			return nil, err
		}
		ces.BuildFromElement(e, sso.TagProjection)
		if len(ces.timestamp) >= sso.MaxElementSize {
			break
		}
	}
	return ces, nil
}

func (s *stream) Query(ctx context.Context, sqo pbv1.StreamQueryOptions) (pbv1.StreamQueryResult, error) {
	if sqo.TimeRange == nil || sqo.Entity == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sqo.TimeRange)
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()
	sl, err := tsdb.Lookup(ctx, &pbv1.Series{Subject: sqo.Name, EntityValues: sqo.Entity})
	if err != nil {
		return nil, err
	}

	var result queryResult
	if len(sl) < 1 {
		return &result, nil
	}
	var sids []common.SeriesID
	for i := range sl {
		sids = append(sids, sl[i].ID)
	}
	var parts []*part
	qo := queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
		includeMin:         sqo.TimeRange.IncludeStart,
		includeMax:         sqo.TimeRange.IncludeEnd,
	}
	var n int
	for i := range tabWrappers {
		s := tabWrappers[i].Table().currentSnapshot()
		if s == nil {
			continue
		}
		parts, n = s.getParts(parts, qo.minTimestamp, qo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		result.snapshots = append(result.snapshots, s)
	}
	// TODO: cache tstIter
	var tstIter tstIter
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	tstIter.init(parts, sids, qo.minTimestamp, qo.maxTimestamp)
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot init tstIter: %w", tstIter.Error())
	}
	for tstIter.nextBlock() {
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
	}
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}
	if sqo.Order == nil {
		result.orderByTS = true
		result.ascTS = true
		return &result, nil
	}
	if sqo.Order.Index == nil {
		result.orderByTS = true
		if sqo.Order.Sort == modelv1.Sort_SORT_ASC || sqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		return &result, nil
	}

	result.sidToIndex = make(map[common.SeriesID]int)
	for i, si := range originalSids {
		result.sidToIndex[si] = i
	}
	return &result, nil
}

type streamSpec struct {
	schema     *databasev1.Stream
	indexRules []*databasev1.IndexRule
}

func openStream(shardNum uint32, db schema.Supplier, spec streamSpec, l *logger.Logger) *stream {
	s := &stream{
		shardNum:   shardNum,
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
	}
	s.parseSpec()
	if db == nil {
		return s
	}

	s.databaseSupplier = db
	return s
}
