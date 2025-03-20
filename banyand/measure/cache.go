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
	"math"
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type cache struct {
	data sync.Map
	bh   bucketHeap
	size int
}

func newCache() *cache {
	return &cache{
		data: sync.Map{},
	}
}

func initBlockCursorsFromPart(p *part, sids []common.SeriesID, schema *databasev1.Measure) []*blockCursor {
	pi := &partIter{}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	pi.init(bma, p, sids, 0, math.MaxInt64)
	bcs := make([]*blockCursor, 0)
	for pi.nextBlock() {
		bc := generateBlockCursor()
		mqo := model.MeasureQueryOptions{
			FieldProjection: []string{},
		}
		for _, field := range schema.Fields {
			mqo.FieldProjection = append(mqo.FieldProjection, field.Name)
		}
		qo := queryOptions{
			minTimestamp:        0,
			maxTimestamp:        math.MaxInt64,
			MeasureQueryOptions: mqo,
		}
		bc.init(p, pi.curBlock, qo)
		bcs = append(bcs, bc)
	}

	if len(bcs) == 0 {
		return nil
	}

	cursorChan := make(chan int, len(bcs))
	for i := 0; i < len(bcs); i++ {
		go func(i int) {
			tmpBlock := generateBlock()
			defer releaseBlock(tmpBlock)
			if !bcs[i].loadDataFromParts(tmpBlock) {
				cursorChan <- i
				return
			}
			cursorChan <- -1
		}(i)
	}

	blankCursorList := []int{}
	for completed := 0; completed < len(bcs); completed++ {
		result := <-cursorChan
		if result != -1 {
			blankCursorList = append(blankCursorList, result)
		}
	}
	sort.Slice(blankCursorList, func(i, j int) bool {
		return blankCursorList[i] > blankCursorList[j]
	})
	for _, index := range blankCursorList {
		bcs = append(bcs[:index], bcs[index+1:]...)
	}
	return bcs
}

func (c *cache) addAndMerge(bc *blockCursor) {
	val, ok := c.data.Load(bc.bm.seriesID)
	var buckets []*bucket
	if !ok {
		// TODO: eliminate bucket
		buckets = make([]*bucket, 0)
		buckets = append(buckets, generateBucket(bc.bm.seriesID))
		c.data.Store(bc.bm.seriesID, buckets)
	} else {
		buckets = val.([]*bucket)
	}

	bucketsToMerge := make(map[int]struct{})
	for i, t := range bc.timestamps {
		idx := sort.Search(len(buckets), func(i int) bool {
			return buckets[i].timestampsBuf.minTimestamp > t
		})
		if idx-1 < 0 {
			continue
		}
		bucketsToMerge[idx-1] = struct{}{}
		if idx < len(buckets) {
			buckets[idx-1].addOne(bc, i)
		} else {
			// TODO: create a new bucket when reaching maxSize
			buckets[idx-1].addRange(bc, i)
			break
		}
	}

	for i, bucket := range buckets {
		if _, ok := bucketsToMerge[i]; !ok {
			continue
		}
		bucket.merge()
	}
}

func (c *cache) put(p *part, sids []common.SeriesID, schema *databasev1.Measure) {
	bcs := initBlockCursorsFromPart(p, sids, schema)
	for _, bc := range bcs {
		c.addAndMerge(bc)
	}
}
