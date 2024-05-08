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

type filterOptions struct {
	elementRefMap map[common.SeriesID][]int64
	pbv1.StreamFilterOptions
	minTimestamp int64
	maxTimestamp int64
}

type filterResult struct {
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

func (fr *filterResult) Pull() *pbv1.StreamResult {
	if !fr.loaded {
		if len(fr.data) == 0 {
			return nil
		}
		n := len(fr.data)
		for i := 0; i < n; i++ {
			// TODO: fetching data concurrently
			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)
			if !fr.data[i].searchData(tmpBlock) {
				continue
			}
			if fr.schema.GetEntity() == nil || len(fr.schema.GetEntity().GetTagNames()) == 0 {
				continue
			}
			sidIndex := fr.sidToIndex[fr.data[i].bm.seriesID]
			series := fr.seriesList[sidIndex]
			entityMap := make(map[string]int)
			tagFamilyMap := make(map[string]int)
			for idx, entity := range fr.schema.GetEntity().GetTagNames() {
				entityMap[entity] = idx + 1
			}
			for idx, tagFamily := range fr.data[i].tagFamilies {
				tagFamilyMap[tagFamily.name] = idx + 1
			}
			for _, tagFamilyProj := range fr.data[i].tagProjection {
				for j, tagProj := range tagFamilyProj.Names {
					offset := fr.tagNameIndex[tagProj]
					tagFamilySpec := fr.schema.GetTagFamilies()[offset.FamilyOffset]
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
						fr.data[i].tagFamilies[tagFamilyPos-1] = tagFamily{
							name: tagFamilyProj.Family,
							tags: make([]tag, 0),
						}
					}
					valueType := pbv1.MustTagValueToValueType(series.EntityValues[entityPos-1])
					fr.data[i].tagFamilies[tagFamilyPos-1].tags[j] = tag{
						name:      tagProj,
						values:    mustEncodeTagValue(tagProj, tagSpec.GetType(), series.EntityValues[entityPos-1], len(fr.data[i].timestamps)),
						valueType: valueType,
					}
				}
			}
			if fr.orderByTimestampDesc() {
				fr.data[i].idx = len(fr.data[i].timestamps) - 1
			}
			fr.data = append(fr.data, fr.data[i])
		}
		fr.data = fr.data[n:]
		fr.loaded = true
		heap.Init(fr)
	}
	if len(fr.data) == 0 {
		return nil
	}
	if len(fr.data) == 1 {
		r := &pbv1.StreamResult{}
		bc := fr.data[0]
		bc.copyAllTo(r, fr.orderByTimestampDesc())
		fr.data = fr.data[:0]
		return r
	}
	return fr.merge()
}

func (fr *filterResult) Release() {
	for i, v := range fr.data {
		releaseBlockCursor(v)
		fr.data[i] = nil
	}
	fr.data = fr.data[:0]
	for i := range fr.snapshots {
		fr.snapshots[i].decRef()
	}
	fr.snapshots = fr.snapshots[:0]
}

func (fr filterResult) Len() int {
	return len(fr.data)
}

func (fr filterResult) Less(i, j int) bool {
	leftTS := fr.data[i].timestamps[fr.data[i].idx]
	rightTS := fr.data[j].timestamps[fr.data[j].idx]
	if fr.orderByTS {
		if fr.ascTS {
			return leftTS < rightTS
		}
		return leftTS > rightTS
	}
	leftSIDIndex := fr.sidToIndex[fr.data[i].bm.seriesID]
	rightSIDIndex := fr.sidToIndex[fr.data[j].bm.seriesID]
	return leftSIDIndex < rightSIDIndex
}

func (fr filterResult) Swap(i, j int) {
	fr.data[i], fr.data[j] = fr.data[j], fr.data[i]
}

func (fr *filterResult) Push(x interface{}) {
	fr.data = append(fr.data, x.(*blockCursor))
}

func (fr *filterResult) Pop() interface{} {
	old := fr.data
	n := len(old)
	x := old[n-1]
	fr.data = old[0 : n-1]
	return x
}

func (fr *filterResult) orderByTimestampDesc() bool {
	return fr.orderByTS && !fr.ascTS
}

func (fr *filterResult) merge() *pbv1.StreamResult {
	step := 1
	if fr.orderByTimestampDesc() {
		step = -1
	}
	result := &pbv1.StreamResult{}
	var lastSid common.SeriesID

	for fr.Len() > 0 {
		topBC := fr.data[0]
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			return result
		}
		lastSid = topBC.bm.seriesID

		topBC.copyTo(result)
		topBC.idx += step

		if fr.orderByTimestampDesc() {
			if topBC.idx < 0 {
				heap.Pop(fr)
			} else {
				heap.Fix(fr, 0)
			}
		} else {
			if topBC.idx >= len(topBC.timestamps) {
				heap.Pop(fr)
			} else {
				heap.Fix(fr, 0)
			}
		}
	}

	return result
}

func (s *stream) Filter(ctx context.Context, sfo pbv1.StreamFilterOptions) (srp pbv1.StreamResultPuller, err error) {
	if sfo.TimeRange == nil || len(sfo.Entities) < 1 {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sfo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection is required")
	}
	db := s.databaseSupplier.SupplyTSDB()
	var result filterResult
	if db == nil {
		return srp, nil
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*sfo.TimeRange)
	sort.Slice(tabWrappers, func(i, j int) bool {
		return tabWrappers[i].GetTimeRange().Start.Before(tabWrappers[j].GetTimeRange().Start)
	})
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	series := make([]*pbv1.Series, len(sfo.Entities))
	for i := range sfo.Entities {
		series[i] = &pbv1.Series{
			Subject:      sfo.Name,
			EntityValues: sfo.Entities[i],
		}
	}
	seriesList, err := tsdb.Lookup(ctx, series)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return srp, nil
	}

	var sids []common.SeriesID
	for i := range seriesList {
		sids = append(sids, seriesList[i].ID)
	}
	var elementRefList []elementRef
	for _, tw := range tabWrappers {
		index := tw.Table().Index()
		erl, err := index.Search(ctx, seriesList, sfo.Filter)
		if err != nil {
			return nil, err
		}
		elementRefList = append(elementRefList, erl...)
		if len(elementRefList) > sfo.MaxElementSize {
			elementRefList = elementRefList[:sfo.MaxElementSize]
			break
		}
	}
	elementRefMap := make(map[common.SeriesID][]int64)
	for _, ref := range elementRefList {
		if _, ok := elementRefMap[ref.seriesID]; !ok {
			elementRefMap[ref.seriesID] = []int64{ref.timestamp}
		} else {
			elementRefMap[ref.seriesID] = append(elementRefMap[ref.seriesID], ref.timestamp)
		}
	}
	fo := filterOptions{
		StreamFilterOptions: sfo,
		minTimestamp:        sfo.TimeRange.Start.UnixNano(),
		maxTimestamp:        sfo.TimeRange.End.UnixNano(),
		elementRefMap:       elementRefMap,
	}
	var parts []*part
	var n int
	for i := range tabWrappers {
		s := tabWrappers[i].Table().currentSnapshot()
		if s == nil {
			continue
		}
		parts, n = s.getParts(parts, fo.minTimestamp, fo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		result.snapshots = append(result.snapshots, s)
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	// TODO: cache tstIter
	var tstIter tstIter
	defer tstIter.reset()
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	tstIter.init(bma, parts, sids, fo.minTimestamp, fo.maxTimestamp)
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot init tstIter: %w", tstIter.Error())
	}
	for tstIter.nextBlock() {
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]
		initBlockCursor(bc, p.p, p.curBlock, fo)
		result.data = append(result.data, bc)
	}
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}

	entityMap, _, _, sidToIndex := s.genIndex(sfo.TagProjection, seriesList)
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
	if sfo.Order == nil {
		result.orderByTS = true
		result.ascTS = true
		return &result, nil
	}
	if sfo.Order.Index == nil {
		result.orderByTS = true
		if sfo.Order.Sort == modelv1.Sort_SORT_ASC || sfo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		return &result, nil
	}

	return &result, nil
}
