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

package trace

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const checkDoneEvery = 128

var nilResult = model.TraceQueryResult(nil)

type queryOptions struct {
	seriesToEntity map[common.SeriesID][]*modelv1.TagValue
	traceIDs       []string
	model.TraceQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (t *trace) Query(ctx context.Context, tqo model.TraceQueryOptions) (model.TraceQueryResult, error) {
	if tqo.TimeRange == nil {
		return nil, errors.New("invalid query options: timeRange are required")
	}
	var tsdb storage.TSDB[*tsTable, option]
	var err error
	db := t.tsdb.Load()
	if db == nil {
		tsdb, err = t.schemaRepo.loadTSDB(t.group)
		if err != nil {
			return nil, err
		}
		t.tsdb.Store(tsdb)
	} else {
		tsdb = db.(storage.TSDB[*tsTable, option])
	}

	segments, err := tsdb.SelectSegments(*tqo.TimeRange)
	if err != nil {
		return nil, err
	}
	if len(segments) < 1 {
		return nilResult, nil
	}

	result := queryResult{
		ctx:           ctx,
		segments:      segments,
		tagProjection: tqo.TagProjection,
	}
	defer func() {
		if err != nil {
			result.Release()
		}
	}()
	sort.Strings(tqo.TraceIDs)
	var parts []*part
	qo := queryOptions{
		TraceQueryOptions: tqo,
		traceIDs:          tqo.TraceIDs,
		minTimestamp:      tqo.TimeRange.Start.UnixNano(),
		maxTimestamp:      tqo.TimeRange.End.UnixNano(),
	}

	// Process entities to get series IDs for sidx queries
	if len(tqo.Entities) > 0 {
		// Create series from entities for lookup
		series := make([]*pbv1.Series, len(tqo.Entities))
		for i, entityValues := range tqo.Entities {
			series[i] = &pbv1.Series{
				Subject:      tqo.Name,
				EntityValues: entityValues,
			}
		}

		// Use segment lookup to find actual series that exist in the data
		qo.seriesToEntity = make(map[common.SeriesID][]*modelv1.TagValue)
		for _, segment := range segments {
			sl, lookupErr := segment.Lookup(ctx, series)
			if lookupErr != nil {
				return nil, fmt.Errorf("cannot lookup series: %w", lookupErr)
			}
			for _, s := range sl {
				qo.seriesToEntity[s.ID] = s.EntityValues
			}
		}
	}
	var n int
	tables := make([]*tsTable, 0)
	for _, segment := range segments {
		tt, _ := segment.Tables()
		tables = append(tables, tt...)
	}

	// Check if we need to use sidx for ordering when no trace IDs are provided
	if len(tqo.TraceIDs) == 0 && tqo.Order != nil {
		// Extract sidx name from index rule
		sidxName := "default" // fallback to default sidx
		if tqo.Order.Index != nil {
			sidxName = tqo.Order.Index.GetMetadata().GetName()
		}

		// Collect sidx instances from all tables
		var sidxInstances []sidx.SIDX
		for _, table := range tables {
			if sidxInstance, exists := table.getSidx(sidxName); exists {
				sidxInstances = append(sidxInstances, sidxInstance)
			}
		}

		if len(sidxInstances) > 0 {
			// Query sidx for trace IDs
			var seriesIDs []common.SeriesID
			for seriesID := range qo.seriesToEntity {
				seriesIDs = append(seriesIDs, seriesID)
			}
			traceIDs, sidxErr := t.querySidxForTraceIDs(ctx, sidxInstances, tqo, seriesIDs)
			if sidxErr != nil {
				t.l.Warn().Err(sidxErr).Str("sidx", sidxName).Msg("sidx query failed, falling back to normal query")
			} else if len(traceIDs) > 0 {
				// Store original sidx order before sorting for partIter
				result.originalSidxOrder = make([]string, len(traceIDs))
				copy(result.originalSidxOrder, traceIDs)

				// Create sidx order map for efficient heap operations
				result.sidxOrderMap = make(map[string]int)
				for i, traceID := range result.originalSidxOrder {
					result.sidxOrderMap[traceID] = i
				}

				qo.traceIDs = traceIDs
				sort.Strings(qo.traceIDs) // Sort for partIter efficiency
			}
		}
	}
	if len(qo.traceIDs) == 0 {
		return nilResult, nil
	}
	for i := range tables {
		s := tables[i].currentSnapshot()
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

	if err = t.searchBlocks(ctx, &result, parts, qo); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *trace) searchBlocks(ctx context.Context, result *queryResult, parts []*part, qo queryOptions) error {
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	defFn := startBlockScanSpan(ctx, qo.traceIDs, parts, result)
	defer defFn()
	tstIter := generateTstIter()
	defer releaseTstIter(tstIter)
	tstIter.init(bma, parts, qo.traceIDs)
	if tstIter.Error() != nil {
		return fmt.Errorf("cannot init tstIter: %w", tstIter.Error())
	}
	var hit int
	var spanBlockBytes uint64
	quota := t.pm.AvailableBytes()
	for tstIter.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				return errors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan",
					len(result.data), len(tstIter.piPool)-tstIter.idx, len(tstIter.piPool))
			default:
			}
		}
		hit++
		bc := generateBlockCursor()
		p := tstIter.piPool[tstIter.idx]
		bc.init(p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
		spanBlockBytes += bc.bm.uncompressedSpanSizeBytes
		if quota >= 0 && spanBlockBytes > uint64(quota) {
			return fmt.Errorf("block scan quota exceeded: used %d bytes, quota is %d bytes", spanBlockBytes, quota)
		}
	}
	if tstIter.Error() != nil {
		return fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}
	return t.pm.AcquireResource(ctx, spanBlockBytes)
}

type queryResult struct {
	ctx               context.Context
	tagProjection     *model.TagProjection
	sidxOrderMap      map[string]int
	data              []*blockCursor
	snapshots         []*snapshot
	segments          []storage.Segment[*tsTable, option]
	originalSidxOrder []string
	hit               int
	loaded            bool
}

func (qr *queryResult) Pull() *model.TraceResult {
	select {
	case <-qr.ctx.Done():
		return &model.TraceResult{
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
			return &model.TraceResult{
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

		// Resort data according to original sidx order if available
		if len(qr.originalSidxOrder) > 0 {
			qr.resortDataBySidxOrder()
		}

		heap.Init(qr)
	}
	if len(qr.data) == 0 {
		return nil
	}
	if len(qr.data) == 1 {
		r := &model.TraceResult{}
		bc := qr.data[0]
		bc.copyAllTo(r, false)
		qr.data = qr.data[:0]
		releaseBlockCursor(bc)
		return r
	}
	return qr.merge()
}

// resortDataBySidxOrder reorders the data to match the original sidx order.
func (qr *queryResult) resortDataBySidxOrder() {
	if len(qr.originalSidxOrder) == 0 || len(qr.data) == 0 {
		return
	}

	// Sort data according to the original sidx order using the pre-computed map
	sort.Slice(qr.data, func(i, j int) bool {
		traceIDi := qr.data[i].bm.traceID
		traceIDj := qr.data[j].bm.traceID

		orderi, existi := qr.sidxOrderMap[traceIDi]
		orderj, existj := qr.sidxOrderMap[traceIDj]

		// If both trace IDs are in the original order, use that ordering
		if existi && existj {
			return orderi < orderj
		}
		// If only one is in the original order, prioritize it
		if existi {
			return true
		}
		if existj {
			return false
		}
		// If neither is in the original order, use alphabetical ordering
		return traceIDi < traceIDj
	})
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
	// If no original sidx order, return natural order (no sorted merge)
	if len(qr.originalSidxOrder) == 0 {
		return false
	}

	traceIDi := qr.data[i].bm.traceID
	traceIDj := qr.data[j].bm.traceID

	orderi, existi := qr.sidxOrderMap[traceIDi]
	orderj, existj := qr.sidxOrderMap[traceIDj]

	// If both trace IDs are in the original order, use that ordering
	if existi && existj {
		return orderi < orderj
	}
	// If only one is in the original order, prioritize it
	if existi {
		return true
	}
	if existj {
		return false
	}
	// If neither is in the original order, use alphabetical ordering as fallback
	return traceIDi < traceIDj
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

func (qr *queryResult) merge() *model.TraceResult {
	result := &model.TraceResult{}
	var lastTraceID string

	for qr.Len() > 0 {
		topBC := qr.data[0]
		if lastTraceID != "" && topBC.bm.traceID != lastTraceID {
			return result
		}
		lastTraceID = topBC.bm.traceID

		topBC.copyTo(result)
		topBC.idx++

		if topBC.idx >= len(topBC.spans) {
			heap.Pop(qr)
		}
	}

	return result
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
	case pbv1.ValueTypeTimestamp:
		// Convert 64-bit nanoseconds since epoch back to protobuf timestamp
		epochNanos := convert.BytesToInt64(value)
		seconds := epochNanos / 1e9
		nanos := int32(epochNanos % 1e9)
		return timestampTagValue(seconds, nanos)
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

func timestampTagValue(seconds int64, nanos int32) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Timestamp{
			Timestamp: &timestamppb.Timestamp{
				Seconds: seconds,
				Nanos:   nanos,
			},
		},
	}
}
