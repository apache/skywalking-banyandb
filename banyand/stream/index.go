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
	"path"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type elementIndex struct {
	store index.Store
	l     *logger.Logger
}

func newElementIndex(ctx context.Context, root string, flushTimeoutSeconds int64) (*elementIndex, error) {
	ei := &elementIndex{
		l: logger.Fetch(ctx, "element_index"),
	}
	var err error
	if ei.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:         path.Join(root, elementIndexFilename),
		Logger:       ei.l,
		BatchWaitSec: flushTimeoutSeconds,
	}); err != nil {
		return nil, err
	}
	return ei, nil
}

func (e *elementIndex) Sort(sids []common.SeriesID, fieldKey index.FieldKey, order modelv1.Sort, preloadSize int) (index.FieldIterator, error) {
	iter, err := e.store.Sort(sids, fieldKey, order, preloadSize)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (e *elementIndex) Write(docs index.Documents) error {
	applied := make(chan struct{})
	err := e.store.Batch(index.Batch{
		Documents: docs,
		Applied:   applied,
	})
	if err != nil {
		return err
	}
	<-applied
	return nil
}

func (e *elementIndex) Search(_ context.Context, seriesList pbv1.SeriesList, filter index.Filter, timeRange *timestamp.TimeRange) ([]elementRef, error) {
	pm := make(map[common.SeriesID][]uint64)
	for _, series := range seriesList {
		pl, err := filter.Execute(func(_ databasev1.IndexRule_Type) (index.Searcher, error) {
			return e.store, nil
		}, series.ID)
		if err != nil {
			return nil, err
		}
		if pl == nil {
			pl = roaring.DummyPostingList
		}
		if pl.IsEmpty() {
			continue
		}
		timestamps := pl.ToSlice()
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})
		start, end, ok := timestamp.FindRange(timestamps, uint64(timeRange.Start.UnixNano()), uint64(timeRange.End.UnixNano()))
		if !ok {
			pm[series.ID] = []uint64{}
		} else {
			pm[series.ID] = timestamps[start : end+1]
		}
	}
	return merge(pm), nil
}

func (e *elementIndex) Close() error {
	return e.store.Close()
}

type elementRef struct {
	seriesID  common.SeriesID
	timestamp int64
}

type indexedElementRef struct {
	elementRef
	elemIdx int
}

type priorityQueue []*indexedElementRef

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].timestamp < pq[j].timestamp
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*indexedElementRef)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func merge(postingMap map[common.SeriesID][]uint64) []elementRef {
	var result []elementRef
	pq := make(priorityQueue, 0)
	heap.Init(&pq)

	for seriesID, timestamps := range postingMap {
		if len(timestamps) > 0 {
			er := elementRef{seriesID: seriesID, timestamp: int64(timestamps[0])}
			item := &indexedElementRef{elementRef: er, elemIdx: 0}
			heap.Push(&pq, item)
		}
	}
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*indexedElementRef)
		result = append(result, item.elementRef)

		if item.elemIdx+1 < len(postingMap[item.seriesID]) {
			nextTS := postingMap[item.seriesID][item.elemIdx+1]
			nextEr := elementRef{seriesID: item.seriesID, timestamp: int64(nextTS)}
			nextItem := &indexedElementRef{elementRef: nextEr, elemIdx: item.elemIdx + 1}
			heap.Push(&pq, nextItem)
		}
	}

	return result
}
