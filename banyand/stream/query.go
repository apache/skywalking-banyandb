// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	logicalstream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const checkDoneEvery = 128

func (s *stream) Query(ctx context.Context, sqo model.StreamQueryOptions) (sqr model.StreamQueryResult, err error) {
	if sqo.TimeRange == nil || len(sqo.Entities) < 1 {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	var tsdb storage.TSDB[*tsTable, option]
	db := s.tsdb.Load()
	if db == nil {
		tsdb, err = s.schemaRepo.loadTSDB(s.group)
		if err != nil {
			return nil, err
		}
		s.tsdb.Store(tsdb)
	} else {
		tsdb = db.(storage.TSDB[*tsTable, option])
	}
	segments := tsdb.SelectSegments(*sqo.TimeRange)
	if len(segments) < 1 {
		return bypassQueryResultInstance, nil
	}
	defer func() {
		if err != nil {
			sqr.Release()
		}
	}()
	series := make([]*pbv1.Series, len(sqo.Entities))
	for i := range sqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      sqo.Name,
			EntityValues: sqo.Entities[i],
		}
	}
	qo := queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
	}

	if sqo.Order == nil || sqo.Order.Index == nil {
		result := &tsResult{
			segments: segments,
			series:   series,
			qo:       qo,
			sm:       s,
			pm:       s.pm,
			l:        s.l,
		}
		if sqo.Order == nil {
			result.asc = true
		} else if sqo.Order.Sort == modelv1.Sort_SORT_ASC || sqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.asc = true
		}
		return result, nil
	}
	var result idxResult
	result.pm = s.pm
	result.segments = segments
	result.sm = s
	result.qo = queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
		seriesToEntity:     make(map[common.SeriesID][]*modelv1.TagValue),
	}
	var sl pbv1.SeriesList
	seriesFilter := roaring.NewPostingList()
	for i := range result.segments {
		sl, err = result.segments[i].Lookup(ctx, series)
		if err != nil {
			return nil, err
		}
		for j := range sl {
			if seriesFilter.Contains(uint64(sl[j].ID)) {
				continue
			}
			seriesFilter.Insert(uint64(sl[j].ID))
			result.qo.seriesToEntity[sl[j].ID] = sl[j].EntityValues
		}
		result.tabs = append(result.tabs, result.segments[i].Tables()...)
	}

	if seriesFilter.IsEmpty() {
		result.Release()
		return nil, nil
	}
	sids := seriesFilter.ToSlice()
	if result.qo.elementFilter, err = indexSearch(ctx, sqo, result.tabs, sids); err != nil {
		return nil, err
	}
	if result.sortingIter, err = s.indexSort(ctx, sqo, result.tabs, sids); err != nil {
		return nil, err
	}
	if sqo.Order.Sort == modelv1.Sort_SORT_ASC || sqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
		result.asc = true
	}
	return &result, nil
}

type queryOptions struct {
	elementFilter  posting.List
	seriesToEntity map[common.SeriesID][]*modelv1.TagValue
	sortedSids     []common.SeriesID
	model.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (qo *queryOptions) reset() {
	qo.StreamQueryOptions.Reset()
	qo.elementFilter = nil
	qo.seriesToEntity = nil
	qo.sortedSids = nil
	qo.minTimestamp = 0
	qo.maxTimestamp = 0
}

func (qo *queryOptions) copyFrom(other *queryOptions) {
	qo.StreamQueryOptions.CopyFrom(&other.StreamQueryOptions)
	qo.elementFilter = other.elementFilter
	qo.seriesToEntity = other.seriesToEntity
	qo.sortedSids = other.sortedSids
	qo.minTimestamp = other.minTimestamp
	qo.maxTimestamp = other.maxTimestamp
}

func indexSearch(ctx context.Context, sqo model.StreamQueryOptions,
	tabs []*tsTable, seriesList []uint64,
) (posting.List, error) {
	if sqo.Filter == nil || sqo.Filter == logicalstream.ENode {
		return nil, nil
	}
	result := roaring.NewPostingList()
	for _, tw := range tabs {
		index := tw.Index()
		pl, err := index.Search(ctx, seriesList, sqo.Filter)
		if err != nil {
			return nil, err
		}
		if pl == nil || pl.IsEmpty() {
			continue
		}
		if err := result.Union(pl); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *stream) indexSort(ctx context.Context, sqo model.StreamQueryOptions, tabs []*tsTable,
	sids []uint64,
) (itersort.Iterator[*index.DocumentResult], error) {
	if sqo.Order == nil || sqo.Order.Index == nil {
		return nil, nil
	}
	seriesList := make([]common.SeriesID, len(sids))
	for i := range sids {
		seriesList[i] = common.SeriesID(sids[i])
	}
	iters, err := s.buildItersByIndex(ctx, tabs, seriesList, sqo)
	if err != nil {
		return nil, err
	}
	desc := sqo.Order != nil && sqo.Order.Index == nil && sqo.Order.Sort == modelv1.Sort_SORT_DESC
	return itersort.NewItemIter[*index.DocumentResult](iters, desc), nil
}

func (s *stream) buildItersByIndex(ctx context.Context, tables []*tsTable,
	sids []common.SeriesID, sqo model.StreamQueryOptions,
) (iters []itersort.Iterator[*index.DocumentResult], err error) {
	indexRuleForSorting := sqo.Order.Index
	if len(indexRuleForSorting.Tags) != 1 {
		return nil, fmt.Errorf("only support one tag for sorting, but got %d", len(indexRuleForSorting.Tags))
	}
	for _, tw := range tables {
		var iter index.FieldIterator[*index.DocumentResult]
		fieldKey := index.FieldKey{
			IndexRuleID: indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    indexRuleForSorting.GetAnalyzer(),
		}
		iter, err = tw.Index().Sort(ctx, sids, fieldKey, sqo.Order.Sort, sqo.TimeRange, sqo.MaxElementSize)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}
	return
}

func mustEncodeTagValue(name string, tagType databasev1.TagType, tagValue *modelv1.TagValue, num int) [][]byte {
	values := make([][]byte, num)
	nv := encodeTagValue(name, tagType, tagValue)
	value := nv.marshal()
	for i := 0; i < num; i++ {
		values[i] = value
	}
	return values
}

func mustDecodeTagValue(valueType pbv1.ValueType, value []byte) *modelv1.TagValue {
	if value == nil {
		return pbv1.NullTagValue
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		return int64TagValue(convert.BytesToInt64(value))
	case pbv1.ValueTypeStr:
		return strTagValue(string(value))
	case pbv1.ValueTypeBinaryData:
		return binaryDataTagValue(value)
	case pbv1.ValueTypeInt64Arr:
		var values []int64
		for i := 0; i < len(value); i += 8 {
			values = append(values, convert.BytesToInt64(value[i:i+8]))
		}
		return int64ArrTagValue(values)
	case pbv1.ValueTypeStrArr:
		var values []string
		bb := bigValuePool.Generate()
		defer bigValuePool.Release(bb)
		var err error
		for len(value) > 0 {
			bb.Buf, value, err = unmarshalVarArray(bb.Buf[:0], value)
			if err != nil {
				logger.Panicf("unmarshalVarArray failed: %v", err)
			}
			values = append(values, string(bb.Buf))
		}
		return strArrTagValue(values)
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func int64TagValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Int{
			Int: &modelv1.Int{
				Value: value,
			},
		},
	}
}

func strTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: value,
			},
		},
	}
}

func binaryDataTagValue(value []byte) *modelv1.TagValue {
	data := make([]byte, len(value))
	copy(data, value)
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_BinaryData{
			BinaryData: data,
		},
	}
}

func int64ArrTagValue(values []int64) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_IntArray{
			IntArray: &modelv1.IntArray{
				Value: values,
			},
		},
	}
}

func strArrTagValue(values []string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_StrArray{
			StrArray: &modelv1.StrArray{
				Value: values,
			},
		},
	}
}
