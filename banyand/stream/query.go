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
	"container/heap"
	"context"
	"fmt"
	"sort"

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
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
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
	db := s.databaseSupplier.SupplyTSDB()
	if db == nil {
		return sqr, nil
	}
	var result queryResult
	tsdb := db.(storage.TSDB[*tsTable, option])
	result.segments = tsdb.SelectSegments(*sqo.TimeRange)
	if len(result.segments) < 1 {
		return &result, nil
	}
	defer func() {
		if err != nil {
			result.Release()
		}
	}()
	series := make([]*pbv1.Series, len(sqo.Entities))
	for i := range sqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      sqo.Name,
			EntityValues: sqo.Entities[i],
		}
	}
	var seriesList, sl pbv1.SeriesList
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
			seriesList = append(seriesList, sl[j])
			seriesFilter.Insert(uint64(sl[j].ID))
		}
		result.tabs = append(result.tabs, result.segments[i].Tables()...)
	}

	if len(seriesList) == 0 {
		return &result, nil
	}
	result.qo = queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
		seriesToEntity:     make(map[common.SeriesID][]*modelv1.TagValue),
	}
	for i := range seriesList {
		result.qo.seriesToEntity[seriesList[i].ID] = seriesList[i].EntityValues
		result.qo.sortedSids = append(result.qo.sortedSids, seriesList[i].ID)
	}
	if result.qo.elementFilter, err = indexSearch(ctx, sqo, result.tabs, seriesList); err != nil {
		return nil, err
	}
	result.tagNameIndex = make(map[string]partition.TagLocator)
	result.schema = s.schema
	for i, tagFamilySpec := range s.schema.GetTagFamilies() {
		for j, tagSpec := range tagFamilySpec.GetTags() {
			result.tagNameIndex[tagSpec.GetName()] = partition.TagLocator{
				FamilyOffset: i,
				TagOffset:    j,
			}
		}
	}
	if sqo.Order == nil {
		result.orderByTS = true
		result.asc = true
		return &result, nil
	}

	if sqo.Order.Index == nil {
		result.orderByTS = true
	} else if result.sortingIter, err = s.indexSort(ctx, sqo, result.tabs, seriesList); err != nil {
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

type queryResult struct {
	sortingIter      itersort.Iterator[*index.DocumentResult]
	tagNameIndex     map[string]partition.TagLocator
	schema           *databasev1.Stream
	tabs             []*tsTable
	elementIDsSorted []uint64
	data             []*blockCursor
	snapshots        []*snapshot
	segments         []storage.Segment[*tsTable, option]
	qo               queryOptions
	loaded           bool
	orderByTS        bool
	asc              bool
}

func (qr *queryResult) Pull(ctx context.Context) *model.StreamResult {
	if qr.sortingIter == nil {
		qo := qr.qo
		sort.Slice(qo.sortedSids, func(i, j int) bool { return qo.sortedSids[i] < qo.sortedSids[j] })
		return qr.load(ctx, qo)
	}
	if !qr.loaded {
		qr.elementIDsSorted = make([]uint64, 0, qr.qo.MaxElementSize)
		return qr.loadSortingData(ctx)
	}
	if v := qr.nextValue(); v != nil {
		return v
	}
	qr.loaded = false
	return qr.loadSortingData(ctx)
}

func (qr *queryResult) scanParts(ctx context.Context, qo queryOptions) error {
	var parts []*part
	var n int
	for i := range qr.tabs {
		s := qr.tabs[i].currentSnapshot()
		if s == nil {
			continue
		}
		parts, n = s.getParts(parts, qo.minTimestamp, qo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		qr.snapshots = append(qr.snapshots, s)
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	defFn := startBlockScanSpan(ctx, len(qo.sortedSids), parts, qr)
	defer defFn()
	ti := generateTstIter()
	defer releaseTstIter(ti)
	sids := qo.sortedSids
	ti.init(bma, parts, sids, qo.minTimestamp, qo.maxTimestamp)
	if ti.Error() != nil {
		return fmt.Errorf("cannot init tstIter: %w", ti.Error())
	}
	var hit int
	for ti.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return errors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan", len(qr.data), len(ti.piHeap), len(ti.piPool))
			default:
			}
		}
		hit++
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		qr.data = append(qr.data, bc)
	}
	if ti.Error() != nil {
		return fmt.Errorf("cannot iterate tstIter: %w", ti.Error())
	}
	return nil
}

func (qr *queryResult) load(ctx context.Context, qo queryOptions) *model.StreamResult {
	if !qr.loaded {
		if err := qr.scanParts(ctx, qo); err != nil {
			return &model.StreamResult{
				Error: err,
			}
		}
		if len(qr.data) == 0 {
			return nil
		}

		cursorChan := make(chan int, len(qr.data))
		for i := 0; i < len(qr.data); i++ {
			go func(i int) {
				select {
				case <-ctx.Done():
					cursorChan <- i
					return
				default:
				}
				tmpBlock := generateBlock()
				defer releaseBlock(tmpBlock)
				if !qr.data[i].loadData(tmpBlock) {
					cursorChan <- i
					return
				}
				if qr.schema.GetEntity() == nil || len(qr.schema.GetEntity().GetTagNames()) == 0 {
					cursorChan <- -1
					return
				}
				entityValues := qo.seriesToEntity[qr.data[i].bm.seriesID]
				entityMap := make(map[string]int)
				tagFamilyMap := make(map[string]int)
				for idx, entity := range qr.schema.GetEntity().GetTagNames() {
					entityMap[entity] = idx + 1
				}
				for idx, tagFamily := range qr.data[i].tagFamilies {
					tagFamilyMap[tagFamily.name] = idx + 1
				}
				for _, tagFamilyProj := range qr.data[i].tagProjection {
					for j, tagProj := range tagFamilyProj.Names {
						offset := qr.tagNameIndex[tagProj]
						tagFamilySpec := qr.schema.GetTagFamilies()[offset.FamilyOffset]
						tagSpec := tagFamilySpec.GetTags()[offset.TagOffset]
						if tagSpec.IndexedOnly {
							continue
						}
						entityPos := entityMap[tagProj]
						tagFamilyPos := tagFamilyMap[tagFamilyProj.Family]
						if entityPos == 0 {
							continue
						}
						if tagFamilyPos == 0 {
							qr.data[i].tagFamilies[tagFamilyPos-1] = tagFamily{
								name: tagFamilyProj.Family,
								tags: make([]tag, 0),
							}
						}
						valueType := pbv1.MustTagValueToValueType(entityValues[entityPos-1])
						qr.data[i].tagFamilies[tagFamilyPos-1].tags[j] = tag{
							name:      tagProj,
							values:    mustEncodeTagValue(tagProj, tagSpec.GetType(), entityValues[entityPos-1], len(qr.data[i].timestamps)),
							valueType: valueType,
						}
					}
				}
				if qr.orderByTimestampDesc() {
					qr.data[i].idx = len(qr.data[i].timestamps) - 1
				}
				cursorChan <- -1
			}(i)
		}

		blankCursorList := []int{}
		for completed := 0; completed < len(qr.data); completed++ {
			result := <-cursorChan
			if result != -1 {
				blankCursorList = append(blankCursorList, result)
			}
		}
		select {
		case <-ctx.Done():
			return &model.StreamResult{
				Error: errors.WithMessagef(ctx.Err(), "interrupt: blank/total=%d/%d", len(blankCursorList), len(qr.data)),
			}
		default:
		}
		sort.Slice(blankCursorList, func(i, j int) bool {
			return blankCursorList[i] > blankCursorList[j]
		})
		for _, index := range blankCursorList {
			releaseBlockCursor(qr.data[index])
			qr.data = append(qr.data[:index], qr.data[index+1:]...)
		}
		qr.loaded = true
		heap.Init(qr)
	}
	return qr.nextValue()
}

func (qr *queryResult) nextValue() *model.StreamResult {
	if len(qr.data) == 0 {
		return nil
	}
	if !qr.orderByTS {
		return qr.mergeByTagValue()
	}
	if len(qr.data) == 1 {
		r := &model.StreamResult{}
		bc := qr.data[0]
		bc.copyAllTo(r, qr.orderByTimestampDesc())
		qr.releaseBlockCursor()
		return r
	}
	return qr.mergeByTimestamp()
}

func (qr *queryResult) loadSortingData(ctx context.Context) *model.StreamResult {
	var qo queryOptions
	qo.StreamQueryOptions = qr.qo.StreamQueryOptions
	qo.elementFilter = roaring.NewPostingList()
	qo.seriesToEntity = qr.qo.seriesToEntity
	qr.elementIDsSorted = qr.elementIDsSorted[:0]
	count, searchedSize := 1, 0
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "load-sorting-data")
		span.Tagf("max_element_size", "%d", qo.MaxElementSize)
		if qr.qo.elementFilter != nil {
			span.Tag("filter_size", fmt.Sprintf("%d", qr.qo.elementFilter.Len()))
		}
		defer func() {
			span.Tagf("searched_size", "%d", searchedSize)
			span.Tagf("count", "%d", count)
			span.Stop()
		}()
	}
	for ; qr.sortingIter.Next(); count++ {
		searchedSize++
		val := qr.sortingIter.Val()
		if qr.qo.elementFilter != nil && !qr.qo.elementFilter.Contains(val.DocID) {
			count--
			continue
		}
		qo.elementFilter.Insert(val.DocID)
		if val.Timestamp > qo.maxTimestamp {
			qo.maxTimestamp = val.Timestamp
		}
		if val.Timestamp < qo.minTimestamp || qo.minTimestamp == 0 {
			qo.minTimestamp = val.Timestamp
		}
		qr.elementIDsSorted = append(qr.elementIDsSorted, val.DocID)

		// Insertion sort
		insertPos, found := -1, false
		for i, sid := range qo.sortedSids {
			if val.SeriesID == sid {
				found = true
				break
			}
			if val.SeriesID < sid {
				insertPos = i
				break
			}
		}

		if !found {
			if insertPos == -1 {
				qo.sortedSids = append(qo.sortedSids, val.SeriesID)
			} else {
				qo.sortedSids = append(qo.sortedSids[:insertPos], append([]common.SeriesID{val.SeriesID}, qo.sortedSids[insertPos:]...)...)
			}
		}
		if count >= qo.MaxElementSize {
			break
		}
	}
	if qo.elementFilter.IsEmpty() {
		return nil
	}
	return qr.load(ctx, qo)
}

func (qr *queryResult) releaseParts() {
	qr.releaseBlockCursor()
	for i := range qr.snapshots {
		qr.snapshots[i].decRef()
	}
	qr.snapshots = qr.snapshots[:0]
}

func (qr *queryResult) releaseBlockCursor() {
	for i, v := range qr.data {
		releaseBlockCursor(v)
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
}

func (qr *queryResult) Release() {
	qr.releaseParts()
	for i := range qr.segments {
		qr.segments[i].DecRef()
	}
}

func (qr queryResult) Len() int {
	return len(qr.data)
}

func (qr queryResult) Less(i, j int) bool {
	leftIdx, rightIdx := qr.data[i].idx, qr.data[j].idx
	leftTS := qr.data[i].timestamps[leftIdx]
	rightTS := qr.data[j].timestamps[rightIdx]
	if qr.asc {
		return leftTS < rightTS
	}
	return leftTS > rightTS
}

func (qr queryResult) Swap(i, j int) {
	qr.data[i], qr.data[j] = qr.data[j], qr.data[i]
}

func (qr *queryResult) Push(x interface{}) {
	qr.data = append(qr.data, x.(*blockCursor))
}

func (qr *queryResult) Pop() interface{} {
	old := qr.data
	n := len(old)
	x := old[n-1]
	qr.data = old[0 : n-1]
	releaseBlockCursor(x)
	return x
}

func (qr *queryResult) orderByTimestampDesc() bool {
	return qr.orderByTS && !qr.asc
}

func (qr *queryResult) mergeByTagValue() *model.StreamResult {
	defer qr.releaseBlockCursor()
	tmp := &model.StreamResult{}
	prevIdx := 0
	elementIDToIdx := make(map[uint64]int)
	for _, data := range qr.data {
		data.copyAllTo(tmp, false)
		var idx int
		for idx = prevIdx; idx < len(tmp.Timestamps); idx++ {
			elementIDToIdx[tmp.ElementIDs[idx]] = idx
		}
		prevIdx = idx
	}

	r := &model.StreamResult{
		TagFamilies: []model.TagFamily{},
	}
	for _, tagFamily := range tmp.TagFamilies {
		tf := model.TagFamily{
			Name: tagFamily.Name,
			Tags: []model.Tag{},
		}
		for _, tag := range tagFamily.Tags {
			t := model.Tag{
				Name:   tag.Name,
				Values: []*modelv1.TagValue{},
			}
			tf.Tags = append(tf.Tags, t)
		}
		r.TagFamilies = append(r.TagFamilies, tf)
	}
	for _, id := range qr.elementIDsSorted {
		idx, ok := elementIDToIdx[id]
		if !ok {
			continue
		}
		r.Timestamps = append(r.Timestamps, tmp.Timestamps[idx])
		r.ElementIDs = append(r.ElementIDs, tmp.ElementIDs[idx])
		for i := 0; i < len(r.TagFamilies); i++ {
			for j := 0; j < len(r.TagFamilies[i].Tags); j++ {
				r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, tmp.TagFamilies[i].Tags[j].Values[idx])
			}
		}
	}
	return r
}

func (qr *queryResult) mergeByTimestamp() *model.StreamResult {
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}
	result := &model.StreamResult{}
	var lastSid common.SeriesID

	for qr.Len() > 0 {
		topBC := qr.data[0]
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			return result
		}
		lastSid = topBC.bm.seriesID

		topBC.copyTo(result)
		topBC.idx += step

		if qr.orderByTimestampDesc() {
			if topBC.idx < 0 {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		} else {
			if topBC.idx >= len(topBC.timestamps) {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		}
	}

	return result
}

func indexSearch(ctx context.Context, sqo model.StreamQueryOptions,
	tabs []*tsTable, seriesList pbv1.SeriesList,
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
	seriesList pbv1.SeriesList,
) (itersort.Iterator[*index.DocumentResult], error) {
	if sqo.Order == nil || sqo.Order.Index == nil {
		return nil, nil
	}
	iters, err := s.buildItersByIndex(ctx, tabs, seriesList, sqo)
	if err != nil {
		return nil, err
	}
	desc := sqo.Order != nil && sqo.Order.Index == nil && sqo.Order.Sort == modelv1.Sort_SORT_DESC
	return itersort.NewItemIter[*index.DocumentResult](iters, desc), nil
}

func (s *stream) buildItersByIndex(ctx context.Context, tables []*tsTable,
	seriesList pbv1.SeriesList, sqo model.StreamQueryOptions,
) (iters []itersort.Iterator[*index.DocumentResult], err error) {
	indexRuleForSorting := sqo.Order.Index
	if len(indexRuleForSorting.Tags) != 1 {
		return nil, fmt.Errorf("only support one tag for sorting, but got %d", len(indexRuleForSorting.Tags))
	}
	sids := seriesList.IDs()
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
