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
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type queryOptions struct {
	filteredRefMap map[common.SeriesID][]int64
	sortedRefMap   map[storage.TSTableWrapper[*tsTable]]map[common.SeriesID][]int64
	pbv1.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
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

type queryResult struct {
	entityMap         map[string]int
	sidToIndex        map[common.SeriesID]int
	tagNameIndex      map[string]partition.TagLocator
	schema            *databasev1.Stream
	sortedTagLocation *tagLocation
	data              []*blockCursor
	snapshots         []*snapshot
	seriesList        pbv1.SeriesList
	loaded            bool
	orderByTS         bool
	asc               bool
}

func (qr *queryResult) Pull() *pbv1.StreamResult {
	if !qr.loaded {
		if len(qr.data) == 0 {
			return nil
		}

		cursorChan := make(chan int, len(qr.data))
		for i := 0; i < len(qr.data); i++ {
			go func(i int) {
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
				sidIndex := qr.sidToIndex[qr.data[i].bm.seriesID]
				series := qr.seriesList[sidIndex]
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
						valueType := pbv1.MustTagValueToValueType(series.EntityValues[entityPos-1])
						qr.data[i].tagFamilies[tagFamilyPos-1].tags[j] = tag{
							name:      tagProj,
							values:    mustEncodeTagValue(tagProj, tagSpec.GetType(), series.EntityValues[entityPos-1], len(qr.data[i].timestamps)),
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
		sort.Slice(blankCursorList, func(i, j int) bool {
			return blankCursorList[i] > blankCursorList[j]
		})
		for _, index := range blankCursorList {
			qr.data = append(qr.data[:index], qr.data[index+1:]...)
		}
		qr.loaded = true
		heap.Init(qr)
	}
	if len(qr.data) == 0 {
		return nil
	}
	if len(qr.data) == 1 {
		r := &pbv1.StreamResult{}
		bc := qr.data[0]
		bc.copyAllTo(r, qr.orderByTimestampDesc())
		qr.data = qr.data[:0]
		return r
	}
	return qr.merge()
}

func (qr *queryResult) Release() {
	for i, v := range qr.data {
		releaseBlockCursor(v)
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
	for i := range qr.snapshots {
		qr.snapshots[i].decRef()
	}
	qr.snapshots = qr.snapshots[:0]
}

func (qr queryResult) Len() int {
	return len(qr.data)
}

func (qr queryResult) Less(i, j int) bool {
	leftIdx, rightIdx := qr.data[i].idx, qr.data[j].idx
	if qr.orderByTS {
		leftTS := qr.data[i].timestamps[leftIdx]
		rightTS := qr.data[j].timestamps[rightIdx]
		if qr.asc {
			return leftTS < rightTS
		}
		return leftTS > rightTS
	}
	stl := qr.sortedTagLocation
	leftTagValue := qr.data[i].tagFamilies[stl.familyIndex].tags[stl.tagIndex].values[leftIdx]
	rightTagValue := qr.data[j].tagFamilies[stl.familyIndex].tags[stl.tagIndex].values[rightIdx]
	if qr.asc {
		return bytes.Compare(leftTagValue, rightTagValue) < 0
	}
	return bytes.Compare(leftTagValue, rightTagValue) > 0
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
	return x
}

func (qr *queryResult) orderByTimestampDesc() bool {
	return qr.orderByTS && !qr.asc
}

func (qr *queryResult) merge() *pbv1.StreamResult {
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}
	result := &pbv1.StreamResult{}
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

func (s *stream) Query(ctx context.Context, sqo pbv1.StreamQueryOptions) (sqr pbv1.StreamQueryResult, err error) {
	if sqo.TimeRange == nil || len(sqo.Entities) < 1 {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	db := s.databaseSupplier.SupplyTSDB()
	var result queryResult
	if db == nil {
		return sqr, nil
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sqo.TimeRange)
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	series := make([]*pbv1.Series, len(sqo.Entities))
	for i := range sqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      sqo.Name,
			EntityValues: sqo.Entities[i],
		}
	}
	seriesList, err := tsdb.Lookup(ctx, series)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return sqr, nil
	}

	filteredRefMap, err := indexSearch(ctx, sqo, tabWrappers, seriesList)
	if err != nil {
		return nil, err
	}
	sortedRefMap, sortedTagLocation, err := indexSort(s, sqo, tabWrappers, seriesList, filteredRefMap)
	if err != nil {
		return nil, err
	}

	qo := queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
		filteredRefMap:     filteredRefMap,
		sortedRefMap:       sortedRefMap,
	}
	var parts []*part
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
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	// TODO: cache tstIter
	var ti tstIter
	defer ti.reset()
	var sids []common.SeriesID
	for i := 0; i < len(seriesList); i++ {
		sid := seriesList[i].ID
		if (filteredRefMap != nil || sortedRefMap != nil) && !findSeriesID(sid, filteredRefMap, sortedRefMap) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			i--
			continue
		}
		sids = append(sids, sid)
	}
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	ti.init(bma, parts, sids, qo.minTimestamp, qo.maxTimestamp)
	if ti.Error() != nil {
		return nil, fmt.Errorf("cannot init tstIter: %w", ti.Error())
	}
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
	}
	if ti.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", ti.Error())
	}

	entityMap, sidToIndex := s.genIndex(seriesList)
	result.entityMap = entityMap
	result.sidToIndex = sidToIndex
	result.tagNameIndex = make(map[string]partition.TagLocator)
	result.schema = s.schema
	result.seriesList = seriesList
	for i, si := range originalSids {
		result.sidToIndex[si] = i
	}
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
	} else {
		result.sortedTagLocation = sortedTagLocation
	}
	if sqo.Order.Sort == modelv1.Sort_SORT_ASC || sqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
		result.asc = true
	}
	return &result, nil
}

func indexSearch(ctx context.Context, sqo pbv1.StreamQueryOptions,
	tabWrappers []storage.TSTableWrapper[*tsTable], seriesList pbv1.SeriesList,
) (map[common.SeriesID][]int64, error) {
	if sqo.Filter == nil || sqo.Filter == logical.ENode {
		return nil, nil
	}
	var filteredRefList []elementRef
	for _, tw := range tabWrappers {
		index := tw.Table().Index()
		erl, err := index.Search(ctx, seriesList, sqo.Filter, sqo.TimeRange)
		if err != nil {
			return nil, err
		}
		filteredRefList = append(filteredRefList, erl...)
	}
	filteredRefMap := make(map[common.SeriesID][]int64)
	if len(filteredRefList) != 0 {
		for _, ref := range filteredRefList {
			if _, ok := filteredRefMap[ref.seriesID]; !ok {
				filteredRefMap[ref.seriesID] = []int64{ref.timestamp}
			} else {
				filteredRefMap[ref.seriesID] = append(filteredRefMap[ref.seriesID], ref.timestamp)
			}
		}
	}
	return filteredRefMap, nil
}

func indexSort(s *stream, sqo pbv1.StreamQueryOptions, tabWrappers []storage.TSTableWrapper[*tsTable],
	seriesList pbv1.SeriesList, filteredRefMap map[common.SeriesID][]int64,
) (map[storage.TSTableWrapper[*tsTable]]map[common.SeriesID][]int64, *tagLocation, error) {
	if sqo.Order == nil || sqo.Order.Index == nil {
		return nil, nil, nil
	}
	elementRefCount := 0
	sortedRefMap := make(map[storage.TSTableWrapper[*tsTable]]map[common.SeriesID][]int64)
	iters, stl, err := s.buildItersByIndex(tabWrappers, seriesList, sqo)
	if err != nil {
		return nil, nil, err
	}
	for {
		for i := 0; i < len(iters); i++ {
			if _, ok := sortedRefMap[tabWrappers[i]]; !ok {
				sortedRefMap[tabWrappers[i]] = make(map[common.SeriesID][]int64)
			}
			srm := sortedRefMap[tabWrappers[i]]
			var hasNext bool
			for j := 1; j <= sqo.MaxElementSize; j++ {
				hasNext = iters[i].Next()
				if !hasNext {
					break
				}
				ts, sid := iters[i].Val()
				if filteredRefMap != nil && (filteredRefMap[sid] == nil || timestamp.Find(filteredRefMap[sid], int64(ts)) == -1) {
					continue
				}
				if _, ok := srm[sid]; !ok {
					srm[sid] = []int64{int64(ts)}
				} else {
					srm[sid] = append(srm[sid], int64(ts))
				}
				elementRefCount++
			}
			sortedRefMap[tabWrappers[i]] = srm
			if !hasNext {
				iters = append(iters[:i], iters[i+1:]...)
				i--
			}
		}
		if elementRefCount >= sqo.MaxElementSize || len(iters) == 0 {
			break
		}
	}
	return sortedRefMap, stl, nil
}

func findSeriesID(sid common.SeriesID, filteredRefMap map[common.SeriesID][]int64, sortedRefMap map[storage.TSTableWrapper[*tsTable]]map[common.SeriesID][]int64) bool {
	if len(sortedRefMap) > 0 {
		for _, refMap := range sortedRefMap {
			if _, ok := refMap[sid]; ok {
				return true
			}
		}
		return false
	}
	_, ok := filteredRefMap[sid]
	return ok
}

func (s *stream) genIndex(seriesList pbv1.SeriesList) (map[string]int, map[common.SeriesID]int) {
	entityMap := make(map[string]int)
	for idx, entity := range s.schema.GetEntity().GetTagNames() {
		entityMap[entity] = idx + 1
	}
	sidToIndex := make(map[common.SeriesID]int)
	for idx, series := range seriesList {
		sidToIndex[series.ID] = idx
	}
	return entityMap, sidToIndex
}
