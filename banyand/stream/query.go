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
	"errors"
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type queryOptions struct {
	pbv1.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

type queryResult struct {
	entityMap    map[string]int
	sidToIndex   map[common.SeriesID]int
	tagNameIndex map[string]partition.TagLocator
	schema       *databasev1.Stream
	data         []*blockCursor
	snapshots    []*snapshot
	seriesList   pbv1.SeriesList
	loaded       bool
	orderByTS    bool
	ascTS        bool
}

func (qr *queryResult) Pull() *pbv1.StreamResult {
	if !qr.loaded {
		if len(qr.data) == 0 {
			return nil
		}
		// TODO:// Parallel load
		tmpBlock := generateBlock()
		defer releaseBlock(tmpBlock)
		for i := 0; i < len(qr.data); i++ {
			if !qr.data[i].loadData(tmpBlock) {
				qr.data = append(qr.data[:i], qr.data[i+1:]...)
				i--
			}
			if qr.schema.GetEntity() == nil || len(qr.schema.GetEntity().GetTagNames()) == 0 {
				continue
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
	leftTS := qr.data[i].timestamps[qr.data[i].idx]
	rightTS := qr.data[j].timestamps[qr.data[j].idx]
	if qr.orderByTS {
		if qr.ascTS {
			return leftTS < rightTS
		}
		return leftTS > rightTS
	}
	leftSIDIndex := qr.sidToIndex[qr.data[i].bm.seriesID]
	rightSIDIndex := qr.sidToIndex[qr.data[j].bm.seriesID]
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
	return x
}

func (qr *queryResult) orderByTimestampDesc() bool {
	return qr.orderByTS && !qr.ascTS
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

func (s *stream) Query(ctx context.Context, sqo pbv1.StreamQueryOptions) (pbv1.StreamResultPuller, error) {
	if sqo.TimeRange == nil || sqo.Entities == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	db := s.databaseSupplier.SupplyTSDB()
	var result queryResult
	if db == nil {
		return &result, nil
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
	sl, err := tsdb.Lookup(ctx, series)
	if err != nil {
		return nil, err
	}

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
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	// TODO: cache ti
	var ti tstIter
	defer ti.reset()
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
		initBlockCursor(bc, p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
	}
	if ti.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", ti.Error())
	}

	entityMap, _, _, sidToIndex := s.genIndex(sqo.TagProjection, sl)
	result.entityMap = entityMap
	result.sidToIndex = sidToIndex
	result.tagNameIndex = make(map[string]partition.TagLocator)
	result.schema = s.schema
	result.seriesList = sl
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

	return &result, nil
}
