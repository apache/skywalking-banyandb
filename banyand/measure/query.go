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

package measure

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	preloadSize    = 100
	checkDoneEvery = 128
)

var nilResult = model.MeasureQueryResult(nil)

// Query allow to retrieve measure data points.
type Query interface {
	LoadGroup(name string) (resourceSchema.Group, bool)
	Measure(measure *commonv1.Metadata) (Measure, error)
	GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange
}

// Measure allows inspecting measure data points' details.
type Measure interface {
	Query(ctx context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error)
	GetSchema() *databasev1.Measure
	GetIndexRules() []*databasev1.IndexRule
}

var _ Measure = (*measure)(nil)

type queryOptions struct {
	schemaTagTypes map[string]pbv1.ValueType
	model.MeasureQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

type topNQueryOptions struct {
	sortDirection modelv1.Sort
	number        int32
}

func (m *measure) Query(ctx context.Context, mqo model.MeasureQueryOptions) (mqr model.MeasureQueryResult, err error) {
	if mqo.TimeRange == nil {
		return nil, errors.New("invalid query options: timeRange are required")
	}
	if len(mqo.TagProjection) == 0 && len(mqo.FieldProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection or fieldProjection is required")
	}

	var tsdb storage.TSDB[*tsTable, option]
	db := m.tsdb.Load()
	if db == nil {
		tsdb, err = m.schemaRepo.loadTSDB(m.group)
		if err != nil {
			return nil, err
		}
		m.tsdb.Store(tsdb)
	} else {
		tsdb = db.(storage.TSDB[*tsTable, option])
	}

	segments, err := tsdb.SelectSegments(*mqo.TimeRange)
	if err != nil {
		return nil, err
	}
	if len(segments) < 1 {
		return nilResult, nil
	}

	if m.schema.IndexMode {
		return m.buildIndexQueryResult(ctx, mqo, segments)
	}

	if len(mqo.Entities) < 1 {
		return nil, errors.New("invalid query options: series is required")
	}

	series := make([]*pbv1.Series, len(mqo.Entities))
	for i := range mqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      mqo.Name,
			EntityValues: mqo.Entities[i],
		}
	}

	sids, tables, tableShardIDs, caches, storedIndexValue, newTagProjection, err := m.searchSeriesList(ctx, series, mqo, segments)
	if err != nil {
		return nil, err
	}
	if len(sids) < 1 {
		for i := range segments {
			segments[i].DecRef()
		}
		return nilResult, nil
	}
	result := queryResult{
		ctx:              ctx,
		segments:         segments,
		tagProjection:    mqo.TagProjection,
		storedIndexValue: storedIndexValue,
	}
	defer func() {
		if err != nil {
			result.Release()
		}
	}()
	mqo.TagProjection = newTagProjection

	schemaTagTypes := make(map[string]pbv1.ValueType)
	for _, tf := range m.schema.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			vt := pbv1.TagValueSpecToValueType(tag.GetType())
			if vt != pbv1.ValueTypeUnknown {
				schemaTagTypes[tag.GetName()] = vt
			}
		}
	}

	var parts []*part
	qo := queryOptions{
		MeasureQueryOptions: mqo,
		schemaTagTypes:      schemaTagTypes,
		minTimestamp:        mqo.TimeRange.Start.UnixNano(),
		maxTimestamp:        mqo.TimeRange.End.UnixNano(),
	}
	var n int
	for i := range tables {
		s := tables[i].currentSnapshot()
		if s == nil {
			continue
		}
		oldLen := len(parts)
		parts, n = s.getParts(parts, caches[i], qo.minTimestamp, qo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		// Set shard ID for newly added parts
		for j := oldLen; j < len(parts); j++ {
			parts[j].shardID = tableShardIDs[i]
		}
		result.snapshots = append(result.snapshots, s)
	}

	if err = m.searchBlocks(ctx, &result, sids, parts, qo); err != nil {
		return nil, err
	}

	if mqo.Order == nil {
		result.ascTS = true
		result.orderByTS = true
	} else {
		if mqo.Order.Sort == modelv1.Sort_SORT_ASC || mqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		switch mqo.Order.Type {
		case index.OrderByTypeTime:
			result.orderByTS = true
		case index.OrderByTypeIndex:
			result.orderByTS = false
		case index.OrderByTypeSeries:
			result.orderByTS = false
		}
	}

	if mqo.Name == "_top_n_result" {
		result.topNQueryOptions = &topNQueryOptions{
			sortDirection: mqo.Sort,
			number:        mqo.Number,
		}
	}

	return &result, nil
}

type tagNameWithType struct {
	fieldName string
	typ       pbv1.ValueType
}

func (m *measure) searchSeriesList(ctx context.Context, series []*pbv1.Series, mqo model.MeasureQueryOptions,
	segments []storage.Segment[*tsTable, option],
) (sl []common.SeriesID, tables []*tsTable, tableShardIDs []common.ShardID, caches []storage.Cache, storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	newTagProjection []model.TagProjection, err error,
) {
	var indexProjection []index.FieldKey
	fieldToValueType := make(map[string]tagNameWithType)
	var projectedEntityOffsets map[string]int
	newTagProjection = make([]model.TagProjection, 0)
	is := m.indexSchema.Load().(indexSchema)
	for _, tp := range mqo.TagProjection {
		var tagProjection model.TagProjection
	TAG:
		for _, n := range tp.Names {
			for i := range m.schema.GetEntity().GetTagNames() {
				if n == m.schema.GetEntity().GetTagNames()[i] {
					if projectedEntityOffsets == nil {
						projectedEntityOffsets = make(map[string]int)
					}
					projectedEntityOffsets[n] = i
					continue TAG
				}
			}
			if is.fieldIndexLocation != nil {
				if fields, ok := is.fieldIndexLocation[tp.Family]; ok {
					if field, ok := fields[n]; ok {
						indexProjection = append(indexProjection, field.Key)
						fieldToValueType[field.Key.Marshal()] = tagNameWithType{
							fieldName: n,
							typ:       field.Type,
						}
						continue TAG
					}
				}
			}
			tagProjection.Family = tp.Family
			tagProjection.Names = append(tagProjection.Names, n)
		}
		if tagProjection.Family != "" {
			newTagProjection = append(newTagProjection, tagProjection)
		}
	}

	// Collect all search results first
	var segResults []*segResult
	var needsSorting bool
	seriesFilter := roaring.NewPostingList()

	for i := range segments {
		sd, sortedValues, err := segments[i].IndexDB().Search(ctx, series, storage.IndexSearchOpts{
			Query:       mqo.Query,
			Order:       mqo.Order,
			PreloadSize: preloadSize,
			Projection:  indexProjection,
		})
		if err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}
		if len(sd.SeriesList) > 0 {
			tt, shardIDs, cc := segments[i].TablesWithShardIDs()
			tables = append(tables, tt...)
			tableShardIDs = append(tableShardIDs, shardIDs...)
			caches = append(caches, cc...)

			// Create segResult for this segment
			sr := &segResult{
				SeriesData:   sd,
				sortedValues: sortedValues,
				i:            0,
			}
			segResults = append(segResults, sr)

			// Check if we need sorting
			if mqo.Order != nil && sortedValues != nil {
				needsSorting = true
			}
		}
	}

	// Sort if needed, otherwise use original order
	if needsSorting && len(segResults) > 0 {
		// Use segResultHeap to sort
		segHeap := &segResultHeap{
			results:  segResults,
			sortDesc: mqo.Order.Sort == modelv1.Sort_SORT_DESC,
		}
		heap.Init(segHeap)

		// Extract sorted series IDs
		for segHeap.Len() > 0 {
			top := heap.Pop(segHeap).(*segResult)
			series := top.SeriesList[top.i]

			if seriesFilter.Contains(uint64(series.ID)) {
				// Move to next in this segment
				top.i++
				if top.i < len(top.SeriesList) {
					heap.Push(segHeap, top)
				}
				continue
			}

			seriesFilter.Insert(uint64(series.ID))
			sl = append(sl, series.ID)

			// Build storedIndexValue for this series
			var fieldResult map[string][]byte
			if top.Fields != nil && top.i < len(top.Fields) {
				fieldResult = top.Fields[top.i]
			}
			storedIndexValue = m.buildStoredIndexValue(
				series.ID,
				series.EntityValues,
				fieldResult,
				projectedEntityOffsets,
				fieldToValueType,
				storedIndexValue,
			)

			// Move to next in this segment
			top.i++
			if top.i < len(top.SeriesList) {
				heap.Push(segHeap, top)
			}
		}
	} else {
		// Original logic when no sorting is needed
		for _, sr := range segResults {
			for j := range sr.SeriesList {
				if seriesFilter.Contains(uint64(sr.SeriesList[j].ID)) {
					continue
				}
				seriesFilter.Insert(uint64(sr.SeriesList[j].ID))
				sl = append(sl, sr.SeriesList[j].ID)

				var fieldResult map[string][]byte
				if sr.Fields != nil && j < len(sr.Fields) {
					fieldResult = sr.Fields[j]
				}
				storedIndexValue = m.buildStoredIndexValue(
					sr.SeriesList[j].ID,
					sr.SeriesList[j].EntityValues,
					fieldResult,
					projectedEntityOffsets,
					fieldToValueType,
					storedIndexValue,
				)
			}
		}
	}

	return sl, tables, tableShardIDs, caches, storedIndexValue, newTagProjection, nil
}

func (m *measure) buildStoredIndexValue(
	seriesID common.SeriesID,
	entityValues []*modelv1.TagValue,
	fieldResult map[string][]byte,
	projectedEntityOffsets map[string]int,
	fieldToValueType map[string]tagNameWithType,
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
) map[common.SeriesID]map[string]*modelv1.TagValue {
	if projectedEntityOffsets == nil && fieldResult == nil {
		return storedIndexValue
	}

	if storedIndexValue == nil {
		storedIndexValue = make(map[common.SeriesID]map[string]*modelv1.TagValue)
	}
	tagValues := make(map[string]*modelv1.TagValue)
	storedIndexValue[seriesID] = tagValues

	for name, offset := range projectedEntityOffsets {
		if offset < 0 || offset >= len(entityValues) {
			logger.Warningf("offset %d for tag %s is out of range for series ID %v", offset, name, seriesID)
			tagValues[name] = pbv1.NullTagValue
			continue
		}
		tagValues[name] = entityValues[offset]
	}

	for f, v := range fieldResult {
		if tnt, ok := fieldToValueType[f]; ok {
			tagValues[tnt.fieldName] = mustDecodeTagValue(tnt.typ, v)
		} else {
			logger.Panicf("unknown field %s not found in fieldToValueType", f)
		}
	}

	return storedIndexValue
}

func (m *measure) buildIndexQueryResult(ctx context.Context, mqo model.MeasureQueryOptions,
	segments []storage.Segment[*tsTable, option],
) (model.MeasureQueryResult, error) {
	defer func() {
		for i := range segments {
			segments[i].DecRef()
		}
	}()
	is := m.indexSchema.Load().(indexSchema)
	r := &indexSortResult{}
	var indexProjection []index.FieldKey
	for _, tp := range mqo.TagProjection {
		tagFamilyLocation := tagFamilyLocation{
			name:                   tp.Family,
			fieldToValueType:       make(map[string]tagNameWithType),
			projectedEntityOffsets: make(map[string]int),
		}
	TAG:
		for _, n := range tp.Names {
			tagFamilyLocation.tagNames = append(tagFamilyLocation.tagNames, n)
			for i := range m.schema.GetEntity().GetTagNames() {
				if n == m.schema.GetEntity().GetTagNames()[i] {
					tagFamilyLocation.projectedEntityOffsets[n] = i
					continue TAG
				}
			}
			if is.fieldIndexLocation != nil {
				if fields, ok := is.fieldIndexLocation[tp.Family]; ok {
					if field, ok := fields[n]; ok {
						indexProjection = append(indexProjection, field.Key)
						tagFamilyLocation.fieldToValueType[n] = tagNameWithType{
							fieldName: field.Key.Marshal(),
							typ:       field.Type,
						}
						continue TAG
					}
				}
			}
			return nil, fmt.Errorf("tag %s not found in schema", n)
		}
		r.tfl = append(r.tfl, tagFamilyLocation)
	}
	var err error
	opts := storage.IndexSearchOpts{
		Query:       mqo.Query,
		Order:       mqo.Order,
		PreloadSize: preloadSize,
		Projection:  indexProjection,
	}
	seriesFilter := roaring.NewPostingList()
	for i := range segments {
		if mqo.TimeRange.Include(segments[i].GetTimeRange()) {
			opts.TimeRange = nil
		} else {
			opts.TimeRange = mqo.TimeRange
		}
		sr := &segResult{}
		sr.SeriesData, sr.sortedValues, err = segments[i].IndexDB().SearchWithoutSeries(ctx, opts)
		if err != nil {
			return nil, err
		}
		for j := 0; j < len(sr.SeriesList); j++ {
			if seriesFilter.Contains(uint64(sr.SeriesList[j].ID)) {
				sr.remove(j)
				j--
				continue
			}
			seriesFilter.Insert(uint64(sr.SeriesList[j].ID))
		}
		if len(sr.SeriesList) < 1 {
			continue
		}
		r.segResults.results = append(r.segResults.results, sr)
	}
	if len(r.segResults.results) < 1 {
		return nilResult, nil
	}

	// Set sort order based on mqo.Order.Sort
	if mqo.Order != nil && mqo.Order.Sort == modelv1.Sort_SORT_DESC {
		r.segResults.sortDesc = true
	}

	heap.Init(&r.segResults)
	return r, nil
}

func (m *measure) searchBlocks(ctx context.Context, result *queryResult, sids []common.SeriesID, parts []*part, qo queryOptions) error {
	defFn := startBlockScanSpan(ctx, len(sids), parts, result)
	defer defFn()
	tstIter := generateTstIter()
	defer releaseTstIter(tstIter)
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	tstIter.init(parts, sids, qo.minTimestamp, qo.maxTimestamp)
	if tstIter.Error() != nil {
		return fmt.Errorf("cannot init tstIter: %w", tstIter.Error())
	}
	var hit int
	var totalBlockBytes uint64
	quota := m.pm.AvailableBytes()
	for tstIter.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return errors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan", len(result.data), len(tstIter.piHeap), len(tstIter.piPool))
			default:
			}
		}
		hit++
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		bc.shardID = p.p.shardID
		result.data = append(result.data, bc)
		totalBlockBytes += bc.bm.uncompressedSizeBytes
		if quota >= 0 && totalBlockBytes > uint64(quota) {
			return fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", totalBlockBytes, quota)
		}
	}
	if tstIter.Error() != nil {
		return fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}
	if err := m.pm.AcquireResource(ctx, totalBlockBytes); err != nil {
		return err
	}
	result.sidToIndex = make(map[common.SeriesID]int)
	for i, si := range originalSids {
		result.sidToIndex[si] = i
	}
	return nil
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

func mustDecodeFieldValue(valueType pbv1.ValueType, value []byte) *modelv1.FieldValue {
	if value == nil {
		switch valueType {
		case pbv1.ValueTypeStr:
			return pbv1.EmptyStrFieldValue
		case pbv1.ValueTypeBinaryData:
			return pbv1.EmptyBinaryFieldValue
		default:
			return pbv1.NullFieldValue
		}
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		return int64FieldValue(convert.BytesToInt64(value))
	case pbv1.ValueTypeFloat64:
		return float64FieldValue(convert.BytesToFloat64(value))
	case pbv1.ValueTypeStr:
		return strFieldValue(string(value))
	case pbv1.ValueTypeBinaryData:
		return binaryDataFieldValue(value)
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func int64FieldValue(value int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Int{
			Int: &modelv1.Int{
				Value: value,
			},
		},
	}
}

func float64FieldValue(value float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Float{
			Float: &modelv1.Float{
				Value: value,
			},
		},
	}
}

func strFieldValue(value string) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Str{
			Str: &modelv1.Str{
				Value: value,
			},
		},
	}
}

func binaryDataFieldValue(value []byte) *modelv1.FieldValue {
	data := make([]byte, len(value))
	copy(data, value)
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_BinaryData{
			BinaryData: data,
		},
	}
}

type queryResult struct {
	ctx              context.Context
	topNQueryOptions *topNQueryOptions
	sidToIndex       map[common.SeriesID]int
	storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue
	tagProjection    []model.TagProjection
	data             []*blockCursor
	snapshots        []*snapshot
	segments         []storage.Segment[*tsTable, option]
	hit              int
	loaded           bool
	orderByTS        bool
	ascTS            bool
}

func (qr *queryResult) Pull() *model.MeasureResult {
	select {
	case <-qr.ctx.Done():
		return &model.MeasureResult{
			Error: errors.WithMessagef(qr.ctx.Err(), "interrupt: hit %d", qr.hit),
		}
	default:
	}
	if !qr.loaded {
		if len(qr.data) == 0 {
			return nil
		}

		cursorChan := make(chan int, len(qr.data))
		for i := 0; i < len(qr.data); i++ {
			go func(i int) {
				select {
				case <-qr.ctx.Done():
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
		case <-qr.ctx.Done():
			return &model.MeasureResult{
				Error: errors.WithMessagef(qr.ctx.Err(), "interrupt: blank/total=%d/%d", len(blankCursorList), len(qr.data)),
			}
		default:
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
		r := &model.MeasureResult{}
		bc := qr.data[0]
		bc.copyAllTo(r, qr.storedIndexValue, qr.tagProjection, qr.orderByTimestampDesc())
		qr.data = qr.data[:0]
		releaseBlockCursor(bc)
		return r
	}
	return qr.merge(qr.storedIndexValue, qr.tagProjection)
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
	for i := range qr.segments {
		qr.segments[i].DecRef()
	}
}

func (qr queryResult) Len() int {
	return len(qr.data)
}

func (qr queryResult) Less(i, j int) bool {
	leftTS := qr.data[i].timestamps[qr.data[i].idx]
	rightTS := qr.data[j].timestamps[qr.data[j].idx]
	leftVersion := qr.data[i].versions[qr.data[i].idx]
	rightVersion := qr.data[j].versions[qr.data[j].idx]
	if qr.orderByTS {
		if leftTS == rightTS {
			if qr.data[i].bm.seriesID == qr.data[j].bm.seriesID {
				// sort version in descending order if timestamps and seriesID are equal
				return leftVersion > rightVersion
			}
			// sort seriesID in ascending order if timestamps are equal
			return qr.data[i].bm.seriesID < qr.data[j].bm.seriesID
		}
		if qr.ascTS {
			return leftTS < rightTS
		}
		return leftTS > rightTS
	}
	leftSIDIndex := qr.sidToIndex[qr.data[i].bm.seriesID]
	rightSIDIndex := qr.sidToIndex[qr.data[j].bm.seriesID]
	if leftSIDIndex == rightSIDIndex {
		if leftTS == rightTS {
			// sort version in descending order if timestamps and seriesID are equal
			return leftVersion > rightVersion
		}
		// sort timestamps in ascending order if seriesID are equal
		return leftTS < rightTS
	}
	return leftSIDIndex < rightSIDIndex
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
	return qr.orderByTS && !qr.ascTS
}

func (qr *queryResult) merge(storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	tagProjection []model.TagProjection,
) *model.MeasureResult {
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}
	result := &model.MeasureResult{}
	var lastVersion int64
	var lastSid common.SeriesID

	var topNPostAggregator PostProcessor

	if qr.topNQueryOptions != nil {
		topNPostAggregator = CreateTopNPostProcessor(qr.topNQueryOptions.number, modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED,
			qr.topNQueryOptions.sortDirection)
	}

	for qr.Len() > 0 {
		topBC := qr.data[0]
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			return result
		}
		lastSid = topBC.bm.seriesID

		if len(result.Timestamps) > 0 &&
			topBC.timestamps[topBC.idx] == result.Timestamps[len(result.Timestamps)-1] {
			if topNPostAggregator != nil {
				topBC.mergeTopNResult(result, storedIndexValue, topNPostAggregator)
			} else if topBC.versions[topBC.idx] > lastVersion {
				topBC.replace(result, storedIndexValue)
			}
		} else {
			topBC.copyTo(result, storedIndexValue, tagProjection)
			lastVersion = topBC.versions[topBC.idx]
		}

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

type indexSortResult struct {
	tfl        []tagFamilyLocation
	segResults segResultHeap
}

// Pull implements model.MeasureQueryResult.
func (iqr *indexSortResult) Pull() *model.MeasureResult {
	if len(iqr.segResults.results) < 1 {
		return nil
	}
	if len(iqr.segResults.results) == 1 {
		if iqr.segResults.results[0].i >= len(iqr.segResults.results[0].SeriesList) {
			return nil
		}
		sr := iqr.segResults.results[0]
		r := iqr.copyTo(sr)
		sr.i++
		if sr.i >= len(sr.SeriesList) {
			iqr.segResults.results = iqr.segResults.results[:0]
		}
		return r
	}
	top := heap.Pop(&iqr.segResults)
	sr := top.(*segResult)
	r := iqr.copyTo(sr)
	sr.i++
	if sr.i < len(sr.SeriesList) {
		heap.Push(&iqr.segResults, sr)
	}
	return r
}

func (iqr *indexSortResult) Release() {}

func (iqr *indexSortResult) copyTo(src *segResult) (dest *model.MeasureResult) {
	index := src.i
	dest = &model.MeasureResult{
		SID:        src.SeriesList[index].ID,
		Timestamps: []int64{src.Timestamps[index]},
		Versions:   []int64{src.Versions[index]},
	}
	for i := range iqr.tfl {
		tagFamily := model.TagFamily{Name: iqr.tfl[i].name}
		peo := iqr.tfl[i].projectedEntityOffsets
		var fr storage.FieldResult
		if src.Fields != nil {
			fr = src.Fields[index]
		}
		for _, n := range iqr.tfl[i].tagNames {
			if offset, ok := peo[n]; ok {
				tagFamily.Tags = append(tagFamily.Tags, model.Tag{
					Name:   n,
					Values: []*modelv1.TagValue{src.SeriesList[index].EntityValues[offset]},
				})
				continue
			}
			if fr == nil {
				continue
			}
			if tnt, ok := iqr.tfl[i].fieldToValueType[n]; ok {
				tagFamily.Tags = append(tagFamily.Tags, model.Tag{
					Name:   n,
					Values: []*modelv1.TagValue{mustDecodeTagValue(tnt.typ, fr[tnt.fieldName])},
				})
			} else {
				logger.Panicf("unknown field %s not found in fieldToValueType", n)
			}
		}
		dest.TagFamilies = append(dest.TagFamilies, tagFamily)
	}
	return dest
}

type tagFamilyLocation struct {
	fieldToValueType       map[string]tagNameWithType
	projectedEntityOffsets map[string]int
	name                   string
	tagNames               []string
}

type segResult struct {
	storage.SeriesData
	sortedValues [][]byte
	i            int
}

func (sr *segResult) remove(i int) {
	sr.SeriesList = append(sr.SeriesList[:i], sr.SeriesList[i+1:]...)
	if sr.Fields != nil {
		sr.Fields = append(sr.Fields[:i], sr.Fields[i+1:]...)
	}
	sr.Timestamps = append(sr.Timestamps[:i], sr.Timestamps[i+1:]...)
	sr.Versions = append(sr.Versions[:i], sr.Versions[i+1:]...)
	if sr.sortedValues != nil {
		sr.sortedValues = append(sr.sortedValues[:i], sr.sortedValues[i+1:]...)
	}
}

type segResultHeap struct {
	results  []*segResult
	sortDesc bool
}

func (h *segResultHeap) Len() int { return len(h.results) }
func (h *segResultHeap) Less(i, j int) bool {
	// Handle NPE - check for nil results or invalid indices
	if i >= len(h.results) || j >= len(h.results) {
		return false
	}
	if h.results[i] == nil || h.results[j] == nil {
		return false
	}
	if h.results[i].i >= len(h.results[i].SeriesList) || h.results[j].i >= len(h.results[j].SeriesList) {
		return false
	}

	// If no sortedValues, compare by SeriesID
	if h.results[i].sortedValues == nil || h.results[j].sortedValues == nil {
		if h.sortDesc {
			return h.results[i].SeriesList[h.results[i].i].ID > h.results[j].SeriesList[h.results[j].i].ID
		}
		return h.results[i].SeriesList[h.results[i].i].ID < h.results[j].SeriesList[h.results[j].i].ID
	}

	// Handle potential index out of bounds for sortedValues
	if h.results[i].i >= len(h.results[i].sortedValues) || h.results[j].i >= len(h.results[j].sortedValues) {
		if h.sortDesc {
			return h.results[i].SeriesList[h.results[i].i].ID > h.results[j].SeriesList[h.results[j].i].ID
		}
		return h.results[i].SeriesList[h.results[i].i].ID < h.results[j].SeriesList[h.results[j].i].ID
	}

	cmp := bytes.Compare(h.results[i].sortedValues[h.results[i].i], h.results[j].sortedValues[h.results[j].i])
	if h.sortDesc {
		return cmp > 0
	}
	return cmp < 0
}
func (h *segResultHeap) Swap(i, j int) { h.results[i], h.results[j] = h.results[j], h.results[i] }

func (h *segResultHeap) Push(x interface{}) {
	h.results = append(h.results, x.(*segResult))
}

func (h *segResultHeap) Pop() interface{} {
	old := h.results
	n := len(old)
	x := old[n-1]
	h.results = old[0 : n-1]
	return x
}
