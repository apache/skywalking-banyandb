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
	if err := validateTraceQueryOptions(tqo); err != nil {
		return nil, err
	}

	var err error
	tsdb, err := t.ensureTSDB()
	if err != nil {
		return nil, err
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

	qo := queryOptions{
		TraceQueryOptions: tqo,
		traceIDs:          tqo.TraceIDs,
		minTimestamp:      tqo.TimeRange.Start.UnixNano(),
		maxTimestamp:      tqo.TimeRange.End.UnixNano(),
	}

	if err = t.resolveSeriesEntities(ctx, segments, &qo, tqo.Name, tqo.Entities); err != nil {
		return nil, err
	}

	tables := collectTables(segments)

	sidxInstances, sidxQueryRequest, useSIDXStreaming := t.prepareSIDXStreaming(tqo, qo, tables)
	if len(qo.traceIDs) == 0 && !useSIDXStreaming {
		return nilResult, nil
	}

	parts := t.attachSnapshots(&result, tables, qo.minTimestamp, qo.maxTimestamp)

	pipelineCtx, cancel := context.WithCancel(ctx)
	result.ctx = pipelineCtx
	result.cancel = cancel

	if result.keys == nil {
		result.keys = make(map[string]int64)
	}

	var traceBatchCh <-chan traceBatch
	switch {
	case len(qo.traceIDs) > 0:
		traceBatchCh = staticTraceBatchSource(pipelineCtx, qo.traceIDs, tqo.MaxTraceSize, result.keys)
	case useSIDXStreaming:
		traceBatchCh = t.streamSIDXTraceBatches(pipelineCtx, sidxInstances, sidxQueryRequest, tqo.MaxTraceSize)
	default:
		return nilResult, errors.New("invalid query options: either traceIDs or order must be specified")
	}

	result.cursorBatchCh = t.startBlockScanStage(pipelineCtx, parts, qo, traceBatchCh, tqo.MaxTraceSize)

	return &result, nil
}

func validateTraceQueryOptions(tqo model.TraceQueryOptions) error {
	if tqo.TimeRange == nil {
		return errors.New("invalid query options: timeRange are required")
	}
	if len(tqo.TraceIDs) == 0 && tqo.Order == nil {
		return errors.New("invalid query options: either traceIDs or order must be specified")
	}
	return nil
}

func (t *trace) ensureTSDB() (storage.TSDB[*tsTable, option], error) {
	if db := t.tsdb.Load(); db != nil {
		return db.(storage.TSDB[*tsTable, option]), nil
	}

	tsdb, err := t.schemaRepo.loadTSDB(t.group)
	if err != nil {
		return nil, err
	}
	t.tsdb.Store(tsdb)
	return tsdb, nil
}

func (t *trace) resolveSeriesEntities(
	ctx context.Context,
	segments []storage.Segment[*tsTable, option],
	qo *queryOptions,
	name string,
	entities [][]*modelv1.TagValue,
) error {
	if len(entities) == 0 {
		return nil
	}

	series := make([]*pbv1.Series, len(entities))
	for i, entityValues := range entities {
		series[i] = &pbv1.Series{
			Subject:      name,
			EntityValues: entityValues,
		}
	}

	qo.seriesToEntity = make(map[common.SeriesID][]*modelv1.TagValue)
	for _, segment := range segments {
		sl, err := segment.Lookup(ctx, series)
		if err != nil {
			return fmt.Errorf("cannot lookup series: %w", err)
		}
		for _, s := range sl {
			qo.seriesToEntity[s.ID] = s.EntityValues
		}
	}

	return nil
}

func collectTables(segments []storage.Segment[*tsTable, option]) []*tsTable {
	tables := make([]*tsTable, 0)
	for _, segment := range segments {
		if tt, _ := segment.Tables(); len(tt) > 0 {
			tables = append(tables, tt...)
		}
	}
	return tables
}

func (t *trace) prepareSIDXStreaming(
	tqo model.TraceQueryOptions,
	qo queryOptions,
	tables []*tsTable,
) ([]sidx.SIDX, sidx.QueryRequest, bool) {
	if len(tqo.TraceIDs) > 0 || tqo.Order == nil {
		return nil, sidx.QueryRequest{}, false
	}

	sidxName := "default"
	if tqo.Order.Index != nil {
		sidxName = tqo.Order.Index.GetMetadata().GetName()
	}

	sidxInstances := make([]sidx.SIDX, 0, len(tables))
	for _, table := range tables {
		if instance, exists := table.getSidx(sidxName); exists {
			sidxInstances = append(sidxInstances, instance)
		}
	}
	if len(sidxInstances) == 0 {
		return nil, sidx.QueryRequest{}, false
	}

	seriesIDs := make([]common.SeriesID, 0, len(qo.seriesToEntity))
	for seriesID := range qo.seriesToEntity {
		seriesIDs = append(seriesIDs, seriesID)
	}
	if len(seriesIDs) == 0 {
		seriesIDs = []common.SeriesID{1}
	}

	req := sidx.QueryRequest{
		Filter:         tqo.SkippingFilter,
		Order:          tqo.Order,
		MaxElementSize: tqo.MaxTraceSize,
		MinKey:         &tqo.MinVal,
		MaxKey:         &tqo.MaxVal,
		SeriesIDs:      seriesIDs,
	}
	if tqo.TagProjection != nil {
		req.TagProjection = []model.TagProjection{*tqo.TagProjection}
	}

	return sidxInstances, req, true
}

func (t *trace) attachSnapshots(
	qr *queryResult,
	tables []*tsTable,
	minTimestamp int64,
	maxTimestamp int64,
) []*part {
	parts := make([]*part, 0)
	for i := range tables {
		s := tables[i].currentSnapshot()
		if s == nil {
			continue
		}

		var count int
		parts, count = s.getParts(parts, minTimestamp, maxTimestamp)
		if count < 1 {
			s.decRef()
			continue
		}

		qr.snapshots = append(qr.snapshots, s)
	}
	return parts
}

type queryResult struct {
	ctx                 context.Context
	err                 error
	cancel              context.CancelFunc
	tagProjection       *model.TagProjection
	keys                map[string]int64
	cursorBatchCh       <-chan *scanBatch
	currentBatch        *scanBatch
	currentCursorGroups map[string][]*blockCursor
	snapshots           []*snapshot
	segments            []storage.Segment[*tsTable, option]
	currentIndex        int
	hit                 int
}

func (qr *queryResult) Pull() *model.TraceResult {
	for {
		select {
		case <-qr.ctx.Done():
			return &model.TraceResult{
				Error: errors.WithMessagef(qr.ctx.Err(), "interrupt: hit %d", qr.hit),
			}
		default:
		}

		// Ensure we have a batch ready or surface any pending error.
		if qr.err != nil {
			return &model.TraceResult{Error: qr.err}
		}

		if !qr.ensureCurrentBatch() {
			return nil
		}

		if qr.currentBatch == nil || qr.currentIndex >= len(qr.currentBatch.traceIDs) {
			qr.releaseCurrentBatch()
			continue
		}

		traceID := qr.currentBatch.traceIDs[qr.currentIndex]
		cursors := qr.currentCursorGroups[traceID]

		if len(cursors) == 0 {
			qr.currentIndex++
			delete(qr.currentCursorGroups, traceID)
			continue
		}

		filtered, err := qr.loadTraceCursors(cursors)
		if err != nil {
			qr.err = err
			return &model.TraceResult{Error: err}
		}
		if len(filtered) == 0 {
			qr.currentIndex++
			delete(qr.currentCursorGroups, traceID)
			continue
		}

		result := &model.TraceResult{}
		for _, bc := range filtered {
			bc.copyAllTo(result)
			releaseBlockCursor(bc)
		}
		result.Key = qr.keys[traceID]
		result.TID = traceID

		qr.hit++
		qr.currentIndex++
		delete(qr.currentCursorGroups, traceID)

		return result
	}
}

func (qr *queryResult) ensureCurrentBatch() bool {
	if qr.currentBatch != nil && qr.currentIndex < len(qr.currentBatch.traceIDs) {
		return true
	}

	qr.releaseCurrentBatch()

	for {
		select {
		case batch, ok := <-qr.cursorBatchCh:
			if !ok {
				qr.cursorBatchCh = nil
				return false
			}
			if batch == nil {
				continue
			}

			if batch.err != nil {
				qr.err = batch.err
				if batch.cursorCh != nil {
					// Drain and release any cursors from the channel
					for result := range batch.cursorCh {
						if result.cursor != nil {
							releaseBlockCursor(result.cursor)
						}
					}
				}
				return true
			}

			qr.currentBatch = batch
			qr.currentIndex = 0
			qr.currentCursorGroups = make(map[string][]*blockCursor, len(batch.traceIDs))

			// Stream cursors from channel and group by traceID
			if batch.cursorCh != nil {
				for result := range batch.cursorCh {
					if result.err != nil {
						qr.err = result.err
						// Release any cursors we've already collected
						for _, cursors := range qr.currentCursorGroups {
							for _, bc := range cursors {
								releaseBlockCursor(bc)
							}
						}
						qr.currentCursorGroups = nil
						return true
					}
					if result.cursor != nil {
						traceID := result.cursor.bm.traceID
						qr.currentCursorGroups[traceID] = append(qr.currentCursorGroups[traceID], result.cursor)
					}
				}
			}

			if len(batch.keys) > 0 {
				if qr.keys == nil {
					qr.keys = make(map[string]int64, len(batch.keys))
				}
				for k, v := range batch.keys {
					qr.keys[k] = v
				}
			}

			return true

		case <-qr.ctx.Done():
			qr.err = errors.WithMessagef(qr.ctx.Err(), "interrupt: hit %d", qr.hit)
			return true
		}
	}
}

func (qr *queryResult) loadTraceCursors(cursors []*blockCursor) ([]*blockCursor, error) {
	if len(cursors) == 0 {
		return nil, nil
	}

	cursorChan := make(chan int, len(cursors))
	for i := range cursors {
		go func(idx int) {
			select {
			case <-qr.ctx.Done():
				cursorChan <- idx
				return
			default:
			}
			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)
			if !cursors[idx].loadData(tmpBlock) {
				cursorChan <- idx
				return
			}
			cursorChan <- -1
		}(i)
	}

	var blankCursorIdx []int
	for completed := 0; completed < len(cursors); completed++ {
		select {
		case <-qr.ctx.Done():
			return nil, errors.WithMessagef(qr.ctx.Err(), "interrupt while loading trace data")
		case idx := <-cursorChan:
			if idx != -1 {
				blankCursorIdx = append(blankCursorIdx, idx)
			}
		}
	}

	if len(blankCursorIdx) > 0 {
		sort.Slice(blankCursorIdx, func(i, j int) bool {
			return blankCursorIdx[i] > blankCursorIdx[j]
		})
		for _, idx := range blankCursorIdx {
			releaseBlockCursor(cursors[idx])
			cursors = append(cursors[:idx], cursors[idx+1:]...)
		}
	}

	return cursors, nil
}

func (qr *queryResult) releaseCurrentBatch() {
	if qr.currentCursorGroups != nil {
		for _, group := range qr.currentCursorGroups {
			for _, bc := range group {
				releaseBlockCursor(bc)
			}
		}
		qr.currentCursorGroups = nil
	}
	if qr.currentBatch != nil {
		// Note: cursorCh should be fully consumed by ensureCurrentBatch
		// so no need to drain it here
		qr.currentBatch = nil
	}
	qr.currentIndex = 0
}

func (qr *queryResult) Release() {
	if qr.cancel != nil {
		qr.cancel()
	}

	qr.releaseCurrentBatch()

	if qr.cursorBatchCh != nil {
		for {
			select {
			case batch, ok := <-qr.cursorBatchCh:
				if !ok {
					qr.cursorBatchCh = nil
					goto done
				}
				if batch == nil {
					continue
				}
				// Drain and release cursors from the channel
				if batch.cursorCh != nil {
					for result := range batch.cursorCh {
						if result.cursor != nil {
							releaseBlockCursor(result.cursor)
						}
					}
				}
			default:
				goto done
			}
		}
	}

done:
	for i := range qr.snapshots {
		qr.snapshots[i].decRef()
	}
	qr.snapshots = qr.snapshots[:0]
	for i := range qr.segments {
		qr.segments[i].DecRef()
	}
	qr.segments = qr.segments[:0]
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
