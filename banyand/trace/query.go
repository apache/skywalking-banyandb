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
	"maps"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var traceQueryResultTracker = pool.RegisterTracker("trace.queryResult")

const (
	checkDoneEvery = 128
	queryTimeout   = 20 * time.Second
)

var nilResult = model.TraceQueryResult(nil)

type timestampTagFilterMatcher struct {
	delegate  model.TagFilterMatcher
	decoder   model.TagValueDecoder
	timeRange timestamp.TimeRange
	tagName   string
	empty     bool
}

func newTimestampTagFilterMatcher(timeRange timestamp.TimeRange, tagName string, delegate model.TagFilterMatcher) model.TagFilterMatcher {
	matcher := &timestampTagFilterMatcher{
		timeRange: timeRange,
		tagName:   tagName,
		delegate:  delegate,
		decoder:   mustDecodeTagValueAndArray,
		empty:     traceTimeRangeEmpty(timeRange),
	}
	if delegate != nil && delegate.GetDecoder() != nil {
		matcher.decoder = delegate.GetDecoder()
	}
	return matcher
}

func (ttfm *timestampTagFilterMatcher) Match(tags []*modelv1.Tag) (bool, error) {
	if ttfm.empty {
		return false, nil
	}
	for _, tag := range tags {
		if tag.GetKey() != ttfm.tagName {
			continue
		}
		timestampValue := tag.GetValue().GetTimestamp()
		if timestampValue == nil || !ttfm.timeRange.Contains(timestampValue.AsTime().UnixNano()) {
			return false, nil
		}
		if ttfm.delegate == nil {
			return true, nil
		}
		return ttfm.delegate.Match(tags)
	}
	return false, nil
}

func (ttfm *timestampTagFilterMatcher) GetDecoder() model.TagValueDecoder {
	return ttfm.decoder
}

func traceTimeRangeEmpty(timeRange timestamp.TimeRange) bool {
	return timeRange.Start.After(timeRange.End) ||
		timeRange.Start.Equal(timeRange.End) && (!timeRange.IncludeStart || !timeRange.IncludeEnd)
}

type queryOptions struct {
	seriesToEntity map[common.SeriesID][]*modelv1.TagValue
	schemaTagTypes map[string]pbv1.ValueType
	traceIDs       []string
	model.TraceQueryOptions
	QueryMemoryMiB int
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

	segments, err := tsdb.SelectSegments(*tqo.TimeRange, true)
	if err != nil {
		return nil, err
	}
	if len(segments) < 1 {
		return nilResult, nil
	}
	segmentsNeedRelease := true
	defer func() {
		if !segmentsNeedRelease {
			return
		}
		for i := range segments {
			segments[i].DecRef()
		}
	}()

	storageTagProjection := omitIdentityTagProjection(tqo.TagProjection, t.schema.GetTraceIdTagName(), t.schema.GetSpanIdTagName())
	storageTQO := tqo
	storageTQO.TagProjection = storageTagProjection
	result := queryResult{
		ctx:           ctx,
		segments:      segments,
		tagProjection: storageTagProjection,
	}
	segmentsNeedRelease = false
	defer func() {
		if err != nil {
			result.Release()
		}
	}()

	sort.Strings(tqo.TraceIDs)

	schemaTagTypes := make(map[string]pbv1.ValueType)
	for _, tag := range t.schema.GetTags() {
		vt := pbv1.TagValueSpecToValueType(tag.GetType())
		if vt != pbv1.ValueTypeUnknown {
			schemaTagTypes[tag.GetName()] = vt
		}
	}

	qo := queryOptions{
		TraceQueryOptions: storageTQO,
		traceIDs:          tqo.TraceIDs,
		schemaTagTypes:    schemaTagTypes,
		QueryMemoryMiB:    t.vectorized.QueryMemoryMiB,
	}

	// resolveSeriesEntities runs an O(series-cardinality) wildcard series-index lookup
	// whose only consumer is prepareSIDXStreaming, which early-returns for trace-id
	// queries. Skip the lookup when TraceIDs are set: qo.seriesToEntity is unused there.
	if len(tqo.TraceIDs) == 0 {
		if err = t.resolveSeriesEntities(ctx, segments, &qo, tqo.Name, tqo.Entities); err != nil {
			return nil, err
		}
	}

	tables := collectTables(segments)

	sidxInstances, sidxQueryRequest, useSIDXStreaming, prepareErr := t.prepareSIDXStreaming(storageTQO, qo, tables)
	if prepareErr != nil {
		// Assign the outer error so the deferred result cleanup releases segments.
		err = prepareErr
		return nil, err
	}
	if len(qo.traceIDs) == 0 && !useSIDXStreaming {
		result.Release()
		return nilResult, nil
	}

	pipelineCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	result.ctx = pipelineCtx
	result.cancel = cancel
	_, result.recordResult, result.finishResultSpan = startQueryResultSpan(pipelineCtx)

	// Assign errors to err so the deferred result.Release() fires on failure,
	// releasing segments, the timeout context, and the tracing span.
	var vectorizedScanBatch *scanBatch
	if vectorizedScanBatch, err = t.buildConsistentVectorizedScanBatch(
		pipelineCtx, tables, qo, sidxInstances, sidxQueryRequest, useSIDXStreaming, tqo.MaxTraceSize,
	); err != nil {
		return nil, err
	}
	var vectorizedResult *vectorizedTraceQueryResult
	vectorizedResult, err = newVectorizedTraceQueryResult(
		pipelineCtx, vectorizedScanBatch, qo, segments, cancel, result.finishResultSpan, result.recordResult)
	if err != nil {
		return nil, err
	}
	vtrace.IncrQueryCount()
	traceQueryResultTracker.Acquire(vectorizedResult)
	return vectorizedResult, nil
}

func omitIdentityTagProjection(projection *model.TagProjection, traceIDTagName, spanIDTagName string) *model.TagProjection {
	if projection == nil {
		return nil
	}
	filtered := &model.TagProjection{
		Family: projection.Family,
		Names:  make([]string, 0, len(projection.Names)),
	}
	for _, name := range projection.Names {
		if name == traceIDTagName || name == spanIDTagName {
			continue
		}
		filtered.Names = append(filtered.Names, name)
	}
	return filtered
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

func (t *trace) GetTagValueDecoder() model.TagValueDecoder {
	return mustDecodeTagValueAndArray
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
) ([]sidx.SIDX, sidx.QueryRequest, bool, error) {
	if len(tqo.TraceIDs) > 0 || tqo.Order == nil {
		return nil, sidx.QueryRequest{}, false, nil
	}
	if traceTimeRangeEmpty(*tqo.TimeRange) {
		return nil, sidx.QueryRequest{}, false, nil
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
		return nil, sidx.QueryRequest{}, false, nil
	}

	selectedRule := t.selectedIndexRule(sidxName)
	if selectedRule == nil && sidxName != "default" {
		return nil, sidx.QueryRequest{}, false, fmt.Errorf("selected trace index %q has no schema rule", sidxName)
	}
	timestampRepresentation := t.selectedIndexTimestampRepresentation(sidxName)
	seriesIDs := make([]common.SeriesID, 0, len(qo.seriesToEntity))
	if timestampRepresentation == timestampEntity {
		var filterErr error
		seriesIDs, filterErr = filterTimestampEntitySeries(qo.seriesToEntity, selectedRule, t.schema.GetTimestampTagName(), *tqo.TimeRange)
		if filterErr != nil {
			return nil, sidx.QueryRequest{}, false, filterErr
		}
		if len(seriesIDs) == 0 {
			return nil, sidx.QueryRequest{}, false, nil
		}
	} else {
		for seriesID := range qo.seriesToEntity {
			seriesIDs = append(seriesIDs, seriesID)
		}
		if len(seriesIDs) == 0 {
			seriesIDs = []common.SeriesID{1}
		}
	}

	minTimestamp := tqo.TimeRange.Start.UnixNano()
	maxTimestamp := tqo.TimeRange.End.UnixNano()
	minKey, maxKey := tqo.MinVal, tqo.MaxVal
	if timestampRepresentation == timestampOrderingKey {
		if !tqo.TimeRange.IncludeStart {
			if minTimestamp == math.MaxInt64 {
				return nil, sidx.QueryRequest{}, false, nil
			}
			minTimestamp++
		}
		if !tqo.TimeRange.IncludeEnd {
			if maxTimestamp == math.MinInt64 {
				return nil, sidx.QueryRequest{}, false, nil
			}
			maxTimestamp--
		}
		if minTimestamp > maxTimestamp {
			return nil, sidx.QueryRequest{}, false, nil
		}
		if minKey < minTimestamp {
			minKey = minTimestamp
		}
		if maxKey > maxTimestamp {
			maxKey = maxTimestamp
		}
		if minKey > maxKey {
			return nil, sidx.QueryRequest{}, false, nil
		}
	}
	req := sidx.QueryRequest{
		Filter:           tqo.SkippingFilter,
		TagFilter:        tqo.TagFilter,
		Order:            tqo.Order,
		MaxBatchSize:     tqo.MaxTraceSize,
		MinKey:           &minKey,
		MaxKey:           &maxKey,
		MinTimestamp:     &minTimestamp,
		MaxTimestamp:     &maxTimestamp,
		TimeIncludeStart: tqo.TimeRange.IncludeStart,
		TimeIncludeEnd:   tqo.TimeRange.IncludeEnd,
		SeriesIDs:        seriesIDs,
		SchemaTagTypes:   qo.schemaTagTypes,
	}
	if timestampRepresentation == timestampStoredTag {
		req.TimeTagName = t.schema.GetTimestampTagName()
		req.TimeTagFilter = newTimestampTagFilterMatcher(*tqo.TimeRange, req.TimeTagName, tqo.TagFilter)
	}
	if tqo.TagProjection != nil {
		req.TagProjection = []model.TagProjection{*tqo.TagProjection}
	}

	return sidxInstances, req, true, nil
}

// selectedIndexRule returns the configured rule for an SIDX name.
func (t *trace) selectedIndexRule(sidxName string) *databasev1.IndexRule {
	for _, indexRule := range t.GetIndexRules() {
		if indexRule.GetMetadata().GetName() == sidxName {
			return indexRule
		}
	}
	return nil
}

// filterTimestampEntitySeries applies the time range to timestamp values carried
// by the selected index's series prefix. The logical planner derives the same
// selected-rule layout before resolving these series.
func filterTimestampEntitySeries(seriesToEntity map[common.SeriesID][]*modelv1.TagValue,
	indexRule *databasev1.IndexRule, timestampTagName string, timeRange timestamp.TimeRange,
) ([]common.SeriesID, error) {
	if indexRule == nil {
		return nil, fmt.Errorf("timestamp entity index rule is missing")
	}
	if traceTimeRangeEmpty(timeRange) {
		return nil, nil
	}
	timestampPosition := -1
	for index, tagName := range indexRule.GetTags()[:len(indexRule.GetTags())-1] {
		if tagName == "" {
			return nil, fmt.Errorf("timestamp entity index has an empty tag name")
		}
		if tagName == timestampTagName {
			timestampPosition = index
			break
		}
	}
	if timestampPosition < 0 {
		return nil, fmt.Errorf("timestamp entity is absent from the selected index prefix")
	}
	seriesIDs := make([]common.SeriesID, 0, len(seriesToEntity))
	for seriesID, entityValues := range seriesToEntity {
		if len(entityValues) != len(indexRule.GetTags())-1 {
			return nil, fmt.Errorf("series %d has %d entities, selected index requires %d", seriesID, len(entityValues), len(indexRule.GetTags())-1)
		}
		timestampValue := entityValues[timestampPosition].GetTimestamp()
		if timestampValue != nil && timeRange.Contains(timestampValue.AsTime().UnixNano()) {
			seriesIDs = append(seriesIDs, seriesID)
		}
	}
	return seriesIDs, nil
}

type timestampIndexRepresentation uint8

const (
	timestampOrderingKey timestampIndexRepresentation = iota
	timestampStoredTag
	timestampEntity
)

// selectedIndexTimestampRepresentation identifies where the selected SIDX keeps
// timestamp. Every index-rule tag is removed from stored tags; only its final tag
// is the ordering key.
func (t *trace) selectedIndexTimestampRepresentation(sidxName string) timestampIndexRepresentation {
	timestampTagName := t.schema.GetTimestampTagName()
	for _, indexRule := range t.GetIndexRules() {
		if indexRule.GetMetadata().GetName() != sidxName {
			continue
		}
		tags := indexRule.GetTags()
		for idx, tagName := range tags {
			if tagName != timestampTagName {
				continue
			}
			if idx == len(tags)-1 {
				return timestampOrderingKey
			}
			return timestampEntity
		}
		return timestampStoredTag
	}
	// A default index is timestamp ordered by the logical planner. Unknown named
	// indexes must not infer a stored timestamp tag and silently filter all rows.
	return timestampOrderingKey
}

type queryResult struct {
	ctx                 context.Context
	err                 error
	streamDone          <-chan struct{}
	recordCursor        func(*blockCursor)
	keys                map[string]int64
	cursorBatchCh       <-chan *scanBatch
	cancel              context.CancelFunc
	currentCursorGroups map[string][]*blockCursor
	currentBatch        *scanBatch
	tagProjection       *model.TagProjection
	finishResultSpan    func(int, error)
	recordResult        func(*model.TraceResult)
	currentTraceIDs     []string
	segments            []storage.Segment[*tsTable, option]
	hit                 int
	currentIndex        int
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

		if qr.currentBatch == nil || qr.currentIndex >= len(qr.currentTraceIDs) {
			qr.releaseCurrentBatch()
			continue
		}

		traceID := qr.currentTraceIDs[qr.currentIndex]
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

		if qr.recordResult != nil {
			qr.recordResult(result)
		}

		return result
	}
}

func (qr *queryResult) ensureCurrentBatch() bool {
	if qr.currentBatch != nil && qr.currentIndex < len(qr.currentTraceIDs) {
		return true
	}

	qr.releaseCurrentBatch()

	for {
		if qr.cursorBatchCh == nil {
			return false
		}
		select {
		case batch, ok := <-qr.cursorBatchCh:
			if !ok {
				return false
			}
			if batch == nil {
				continue
			}
			return qr.acceptScanBatch(batch)

		case <-qr.ctx.Done():
			qr.err = errors.WithMessagef(qr.ctx.Err(), "interrupt: hit %d", qr.hit)
			return true
		}
	}
}

func (qr *queryResult) acceptScanBatch(batch *scanBatch) bool {
	if batch.err != nil {
		qr.err = batch.err
		return true
	}

	qr.currentBatch = batch
	qr.currentIndex = 0
	qr.currentTraceIDs = batch.traceIDsOrder
	qr.currentCursorGroups = make(map[string][]*blockCursor, len(qr.currentTraceIDs))

	for _, cursor := range batch.cursors {
		if qr.recordCursor != nil {
			qr.recordCursor(cursor)
		}
		traceID := cursor.bm.traceID
		qr.currentCursorGroups[traceID] = append(qr.currentCursorGroups[traceID], cursor)
	}
	batch.cursors = nil

	if batch.cursorCh != nil {
		for result := range batch.cursorCh {
			if result.err != nil {
				qr.err = result.err
				for _, cursors := range qr.currentCursorGroups {
					for _, bc := range cursors {
						releaseBlockCursor(bc)
					}
				}
				qr.currentCursorGroups = nil
				for _, s := range batch.snapshots {
					s.decRef()
				}
				qr.currentBatch = nil
				return true
			}
			if result.cursor != nil {
				if qr.recordCursor != nil {
					qr.recordCursor(result.cursor)
				}
				traceID := result.cursor.bm.traceID
				qr.currentCursorGroups[traceID] = append(qr.currentCursorGroups[traceID], result.cursor)
			}
		}
	}

	if len(batch.keys) > 0 {
		if qr.keys == nil {
			qr.keys = make(map[string]int64, len(batch.keys))
		}
		maps.Copy(qr.keys, batch.keys)
	}

	return true
}

func (qr *queryResult) loadTraceCursors(cursors []*blockCursor) ([]*blockCursor, error) {
	if len(cursors) == 0 {
		return nil, nil
	}

	cursorChan := make(chan int, len(cursors))
	traceBlockLogger := logger.GetLogger("trace-query-block-loader")
	for i := range cursors {
		idx := i
		go func() {
			defer func() {
				if r := recover(); r != nil {
					traceBlockLogger.Error().Interface("panic", r).Msg("panic in parallel block loader")
					cursorChan <- idx
				}
			}()
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
		}()
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
		// Release snapshots from this batch
		for _, s := range qr.currentBatch.snapshots {
			s.decRef()
		}
		// Note: cursorCh should be fully consumed by ensureCurrentBatch
		// so no need to drain it here
		qr.currentBatch = nil
	}
	qr.currentTraceIDs = nil
	qr.currentIndex = 0
}

func (qr *queryResult) Release() {
	traceQueryResultTracker.Release(qr)
	if qr.cancel != nil {
		qr.cancel()
	}

	// Wait for streamSIDXTraceBatches goroutine to exit if it was used
	if qr.streamDone != nil {
		<-qr.streamDone
	}

	// Drain all batches and their cursor channels to ensure scanTraceIDsInline completes
	if qr.cursorBatchCh != nil {
		for batch := range qr.cursorBatchCh {
			qr.releaseScanBatch(batch)
		}
		qr.cursorBatchCh = nil
	}

	qr.releaseCurrentBatch()

	// Release segments
	for i := range qr.segments {
		qr.segments[i].DecRef()
	}
	qr.segments = qr.segments[:0]

	qr.finishTracing(qr.err)
}

func (qr *queryResult) releaseScanBatch(batch *scanBatch) {
	if batch == nil {
		return
	}
	for _, cursor := range batch.cursors {
		if cursor != nil {
			releaseBlockCursor(cursor)
		}
	}
	batch.cursors = nil
	if batch.cursorCh != nil {
		for result := range batch.cursorCh {
			if result.cursor != nil {
				releaseBlockCursor(result.cursor)
			}
		}
	}
	for _, s := range batch.snapshots {
		s.decRef()
	}
	batch.snapshots = nil
}

func (qr *queryResult) finishTracing(err error) {
	if qr.finishResultSpan == nil {
		return
	}
	qr.finishResultSpan(qr.hit, err)
	qr.finishResultSpan = nil
}

func mustDecodeTagValue(valueType pbv1.ValueType, value []byte) *modelv1.TagValue {
	return mustDecodeTagValueAndArray(valueType, value, nil)
}

func mustDecodeTagValueAndArray(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
	if value == nil && valueArr == nil {
		return pbv1.NullTagValue
	}
	if value == nil &&
		valueType != pbv1.ValueTypeInt64Arr &&
		valueType != pbv1.ValueTypeStrArr {
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
		if valueArr != nil {
			for _, v := range valueArr {
				values = append(values, convert.BytesToInt64(v))
			}
			return int64ArrTagValue(values)
		}
		for i := 0; i < len(value); i += 8 {
			values = append(values, convert.BytesToInt64(value[i:i+8]))
		}
		return int64ArrTagValue(values)
	case pbv1.ValueTypeStrArr:
		var values []string
		if valueArr != nil {
			for _, v := range valueArr {
				values = append(values, string(v))
			}
			return strArrTagValue(values)
		}
		var (
			end  int
			next int
			err  error
		)
		for idx := 0; idx < len(value); idx = next {
			end, next, err = encoding.UnmarshalVarArray(value, idx)
			if err != nil {
				logger.Panicf("UnmarshalVarArray failed: %v", err)
			}
			values = append(values, string(value[idx:end]))
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
