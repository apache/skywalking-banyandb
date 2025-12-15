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

package stream

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type idxResult struct {
	sortingIter      itersort.Iterator[*index.DocumentResult]
	sm               *stream
	pm               protector.Memory
	tabs             []*tsTable
	elementIDsSorted []uint64
	data             []*blockCursor
	snapshots        []*snapshot
	segments         []storage.Segment[*tsTable, option]
	qo               queryOptions
	loaded           bool
	asc              bool
}

func (qr *idxResult) Pull(ctx context.Context) *model.StreamResult {
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

func (qr *idxResult) scanParts(ctx context.Context, qo queryOptions) error {
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
	ti.init(bma, parts, sids, qo.minTimestamp, qo.maxTimestamp, qo.SkippingFilter)
	if ti.Error() != nil {
		return fmt.Errorf("cannot init tstIter: %w", ti.Error())
	}
	var hit int
	var totalBlockBytes uint64
	quota := qr.pm.AvailableBytes()
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
		totalBlockBytes += bc.bm.uncompressedSizeBytes
		if quota >= 0 && totalBlockBytes > uint64(quota) {
			return fmt.Errorf("parts scan quota exceeded: used %d bytes, quota is %d bytes", totalBlockBytes, quota)
		}
	}
	if ti.Error() != nil {
		return fmt.Errorf("cannot iterate tstIter: %w", ti.Error())
	}
	if err := qr.pm.AcquireResource(ctx, totalBlockBytes); err != nil {
		return fmt.Errorf("cannot acquire resource: %w", err)
	}
	return nil
}

func (qr *idxResult) load(ctx context.Context, qo queryOptions) *model.StreamResult {
	if qr.loaded {
		qr.nextValue()
	}
	if err := qr.scanParts(ctx, qo); err != nil {
		return &model.StreamResult{
			Error: err,
		}
	}
	if len(qr.data) == 0 {
		return nil
	}

	cursorChan := make(chan int, len(qr.data))
	is := qr.sm.indexSchema.Load().(indexSchema)
	for i := 0; i < len(qr.data); i++ {
		go func(i int) {
			select {
			case <-ctx.Done():
				releaseBlockCursor(qr.data[i])
				cursorChan <- i
				return
			default:
			}
			if qr.sm.schema.GetEntity() == nil || len(qr.sm.schema.GetEntity().GetTagNames()) == 0 {
				cursorChan <- -1
				return
			}
			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)
			if loadBlockCursor(qr.data[i], tmpBlock, qo, is) {
				cursorChan <- -1
				return
			}
			cursorChan <- i
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
		qr.data = append(qr.data[:index], qr.data[index+1:]...)
	}
	qr.loaded = true
	return qr.nextValue()
}

func (qr *idxResult) nextValue() *model.StreamResult {
	if len(qr.data) == 0 {
		return nil
	}
	return qr.mergeByTagValue()
}

func (qr *idxResult) loadSortingData(ctx context.Context) *model.StreamResult {
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

func (qr *idxResult) releaseParts() {
	qr.releaseBlockCursor()
	for i := range qr.snapshots {
		qr.snapshots[i].decRef()
	}
	qr.snapshots = qr.snapshots[:0]
}

func (qr *idxResult) releaseBlockCursor() {
	for i, v := range qr.data {
		releaseBlockCursor(v)
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
}

func (qr *idxResult) Release() {
	qr.releaseParts()
	for i := range qr.segments {
		qr.segments[i].DecRef()
	}
}

func (qr *idxResult) mergeByTagValue() *model.StreamResult {
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

var bypassQueryResultInstance = &bypassQueryResult{}

type bypassQueryResult struct{}

func (bypassQueryResult) Pull(context.Context) *model.StreamResult {
	return nil
}

func (bypassQueryResult) Release() {}
