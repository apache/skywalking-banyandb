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

package trace

import (
	"context"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// traceGroupBudgetShare is the share of the free query budget one group of traces may use: the rest is left for what the
// caller does with the group before it asks for the next one (encoding it, and the copies of that).
const traceGroupBudgetShare = 4

// traceGroupLoader decodes the spans of the traces of a scan batch a group at a time. A group is as many whole traces, in the
// order of the batch, as the query budget has room for (at least one trace), so the spans held at once follow the budget and
// not the limit of the query. The group is given up when the next one is asked for, and its charge goes back to the lease.
type traceGroupLoader struct {
	byTrace        map[string][]*blockCursor
	keys           map[string]int64
	schemaTagTypes map[string]pbv1.ValueType
	order          []string
	snapshots      []*snapshot
	tagCols        []string
	budgetBytes    int64
	charged        uint64
	next           int
}

// newTraceGroupLoader takes the cursors and the snapshots of batch. It keeps the cursors of the traces the batch selects
// and releases the others.
func newTraceGroupLoader(batch *scanBatch, qo queryOptions) *traceGroupLoader {
	loader := &traceGroupLoader{schemaTagTypes: qo.schemaTagTypes}
	if qo.QueryMemoryMiB > 0 {
		loader.budgetBytes = int64(qo.QueryMemoryMiB) * 1024 * 1024
	}
	if qo.TagProjection != nil && len(qo.TagProjection.Names) > 0 {
		loader.tagCols = append([]string(nil), qo.TagProjection.Names...)
	}
	if batch == nil {
		return loader
	}
	loader.snapshots, batch.snapshots = batch.snapshots, nil
	loader.keys = batch.keys
	selected := make(map[string]struct{}, len(batch.traceIDsOrder))
	for _, traceID := range batch.traceIDsOrder {
		selected[traceID] = struct{}{}
	}
	loader.byTrace = make(map[string][]*blockCursor)
	for _, cursor := range batch.cursors {
		if cursor == nil {
			continue
		}
		traceID := cursor.bm.traceID
		if _, ok := selected[traceID]; !ok {
			releaseBlockCursor(cursor)
			continue
		}
		loader.byTrace[traceID] = append(loader.byTrace[traceID], cursor)
	}
	batch.cursors = nil
	// A trace listed twice is loaded once, at its first place.
	queued := make(map[string]struct{}, len(loader.byTrace))
	for _, traceID := range batch.traceIDsOrder {
		if _, found := loader.byTrace[traceID]; !found {
			continue
		}
		if _, dup := queued[traceID]; dup {
			continue
		}
		queued[traceID] = struct{}{}
		loader.order = append(loader.order, traceID)
	}
	return loader
}

// exhausted reports that every trace has been loaded.
func (l *traceGroupLoader) exhausted() bool {
	return l.next >= len(l.order)
}

// groupLimit is how many span bytes the next group may hold, or 0 for no limit. It is a share of what the query budget has
// left, and never more than the soft limit of the query; with neither, one group holds every trace, as before.
func (l *traceGroupLoader) groupLimit(ctx context.Context) uint64 {
	var limit uint64
	if lease, ok := query.BudgetLeaseFromContext(ctx); ok {
		free := uint64(0)
		if lease.Limit() > lease.Used() {
			free = lease.Limit() - lease.Used()
		}
		limit = max(free/traceGroupBudgetShare, 1)
	}
	if l.budgetBytes > 0 && (limit == 0 || uint64(l.budgetBytes) < limit) {
		limit = uint64(l.budgetBytes)
	}
	return limit
}

// load returns the next group of traces, after it has given the budget of the previous group back. It returns nil once every
// trace has been loaded.
func (l *traceGroupLoader) load(ctx context.Context) ([]*model.TraceResult, error) {
	l.refund(ctx)
	if l.exhausted() {
		return nil, nil
	}
	limit := l.groupLimit(ctx)
	var (
		ids     []string
		cursors []*blockCursor
		size    uint64
	)
	for !l.exhausted() {
		traceID := l.order[l.next]
		var traceBytes uint64
		for _, cursor := range l.byTrace[traceID] {
			traceBytes += cursor.bm.uncompressedSpanSizeBytes
		}
		if len(ids) > 0 && limit > 0 && size+traceBytes > limit {
			break
		}
		ids = append(ids, traceID)
		cursors = append(cursors, l.byTrace[traceID]...)
		size += traceBytes
		delete(l.byTrace, traceID)
		l.next++
	}
	group := &scanBatch{traceBatch: traceBatch{traceIDsOrder: ids, keys: l.keys}, cursors: cursors}
	results, charged, err := assembleVectorizedTraceResultsCharged(ctx, group, l.tagCols, l.schemaTagTypes, l.budgetBytes)
	l.charged = charged
	return results, err
}

// refund gives the budget of the group in hand back.
func (l *traceGroupLoader) refund(ctx context.Context) {
	if l.charged > 0 {
		query.Refund(ctx, l.charged)
		l.charged = 0
	}
}

// release drops the cursors that were not loaded, refunds the group in hand and releases the snapshots.
func (l *traceGroupLoader) release(ctx context.Context) {
	l.refund(ctx)
	for traceID, cursors := range l.byTrace {
		for _, cursor := range cursors {
			releaseBlockCursor(cursor)
		}
		delete(l.byTrace, traceID)
	}
	l.next = len(l.order)
	for _, snapshot := range l.snapshots {
		snapshot.decRef()
	}
	l.snapshots = nil
}
