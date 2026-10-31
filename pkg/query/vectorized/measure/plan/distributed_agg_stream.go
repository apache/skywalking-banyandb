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

package plan

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// bucketReduceSource reduces the partials of a time-bucketed group-by on a part-scan measure while the data nodes
// still stream them. Every data node emits its buckets in time order, so a bucket is complete once every node has
// emitted a bucket after it or has ended; the source then finishes that bucket's reduction and emits it. Only the
// groups of the buckets still open are held, however long the time range is.
type bucketReduceSource struct {
	ctx        context.Context
	err        error
	outSchema  *vectorized.BatchSchema
	reducers   map[int64]*vmeasure.PartialReducer
	template   func() *vmeasure.PartialReducer
	set        *scanSet
	marks      []int64
	order      []int64
	ready      []*vectorized.RecordBatch
	ended      []bool
	buckets    int
	bucketsOut int
	closed     bool
}

// newBucketReduceSource reads the first chunk of every node, which gives the layout of the partials.
func newBucketReduceSource(ctx context.Context, set *scanSet, template func() *vmeasure.PartialReducer) (*bucketReduceSource, error) {
	s := &bucketReduceSource{
		ctx: ctx, set: set, template: template,
		reducers: make(map[int64]*vmeasure.PartialReducer),
		marks:    make([]int64, len(set.sources)),
		ended:    make([]bool, len(set.sources)),
	}
	for i := range s.marks {
		s.marks[i] = math.MinInt64
	}
	for i := range set.sources {
		if err := s.pull(ctx, i); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

// pull reads the next chunk of source i and reduces it, or marks the source ended.
func (s *bucketReduceSource) pull(ctx context.Context, i int) error {
	batch, err := s.set.sources[i].next(ctx)
	if err != nil {
		return err
	}
	if batch == nil {
		s.ended[i] = true
		return nil
	}
	defer batch.ReleaseOwners()
	// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
	return s.ingest(i, batch)
}

// ingest splits a chunk into runs of one bucket and folds each run into its bucket's reducer.
func (s *bucketReduceSource) ingest(src int, batch *vectorized.RecordBatch) error {
	tsIdx := batch.Schema.TimestampIndex()
	if tsIdx < 0 {
		return fmt.Errorf("bucketed query expected a RoleTimestamp column in the partial schema (an older data node may have ignored time_bucket)")
	}
	tsCol, ok := batch.Columns[tsIdx].(*vectorized.TypedColumn[int64])
	if !ok {
		return fmt.Errorf("bucketed partial timestamp column is %T, want an int64 column", batch.Columns[tsIdx])
	}
	ts := tsCol.Data()
	active, activeErr := activeBatchRows(batch)
	if activeErr != nil {
		return activeErr
	}
	start := 0
	for i := 1; i <= len(active); i++ {
		if i < len(active) && ts[active[i]] == ts[active[start]] {
			continue
		}
		if foldErr := s.fold(src, batch, active[start:i], ts[active[start]]); foldErr != nil {
			return foldErr
		}
		start = i
	}
	return nil
}

// fold reduces the rows of one bucket of a chunk.
func (s *bucketReduceSource) fold(src int, batch *vectorized.RecordBatch, rows []uint16, bucket int64) error {
	if bucket < s.marks[src] {
		return fmt.Errorf("data node %s sent bucket %d after bucket %d: a streamed time-bucketed partial must be time ordered",
			s.set.sources[src].stream.Node(), bucket, s.marks[src])
	}
	reducer, ok := s.reducers[bucket]
	if !ok {
		reducer = s.template()
		s.reducers[bucket] = reducer
		idx, _ := slices.BinarySearch(s.order, bucket)
		s.order = slices.Insert(s.order, idx, bucket)
		s.buckets++
	}
	part := &vectorized.RecordBatch{Schema: batch.Schema, Columns: batch.Columns, Len: batch.Len, Selection: rows}
	if err := reducer.Consume(part); err != nil {
		return err
	}
	if s.outSchema == nil {
		s.outSchema = reducer.OutputSchema()
	}
	s.marks[src] = bucket
	return nil
}

// frontier is the first bucket that some node may still add rows to.
func (s *bucketReduceSource) frontier() int64 {
	frontier := int64(math.MaxInt64)
	for i, mark := range s.marks {
		if !s.ended[i] {
			frontier = min(frontier, mark)
		}
	}
	return frontier
}

// release finishes every bucket before the frontier, or every bucket when final is set, and queues its rows.
func (s *bucketReduceSource) release(final bool) error {
	frontier := s.frontier()
	for len(s.order) > 0 && (final || s.order[0] < frontier) {
		bucket := s.order[0]
		s.order = s.order[1:]
		reducer := s.reducers[bucket]
		delete(s.reducers, bucket)
		batches, _, err := reducer.Finish()
		reducer.Close()
		if err != nil {
			releaseBatches(batches)
			return err
		}
		s.ready = append(s.ready, batches...)
		s.bucketsOut++
	}
	return nil
}

// laggard returns the node that holds the frontier back, or -1 when every node has ended.
func (s *bucketReduceSource) laggard() int {
	lag := -1
	for i, mark := range s.marks {
		if s.ended[i] {
			continue
		}
		if lag < 0 || mark < s.marks[lag] {
			lag = i
		}
	}
	return lag
}

// Init implements vectorized.BatchOperator.
func (s *bucketReduceSource) Init(context.Context) error { return nil }

// OutputSchema implements vectorized.BatchOperator.
func (s *bucketReduceSource) OutputSchema() *vectorized.BatchSchema { return s.outSchema }

// NextBatch returns the rows of the next complete bucket, or nil once every bucket is out.
func (s *bucketReduceSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	for {
		if len(s.ready) > 0 {
			out := s.ready[0]
			s.ready[0] = nil
			s.ready = s.ready[1:]
			return out, nil
		}
		if s.err != nil {
			return nil, s.err
		}
		if s.closed {
			return nil, nil
		}
		lag := s.laggard()
		if lag < 0 {
			if len(s.order) == 0 {
				return nil, nil
			}
			// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
			s.err = s.release(true)
			continue
		}
		if err := errors.Join(ctx.Err(), s.pull(ctx, lag)); err != nil {
			s.err = err
			continue
		}
		// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
		s.err = s.release(false)
	}
}

// Close drops the reductions still open and the batches not yet handed out. The node streams belong to the scan set.
func (s *bucketReduceSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for bucket, reducer := range s.reducers {
		reducer.Close()
		delete(s.reducers, bucket)
	}
	releaseBatches(s.ready)
	s.ready = nil
	return nil
}

// activeBatchRows returns the active row indexes of a batch.
func activeBatchRows(batch *vectorized.RecordBatch) ([]uint16, error) {
	if batch.Selection != nil {
		return batch.Selection, nil
	}
	if batch.Len > math.MaxUint16+1 {
		return nil, fmt.Errorf("batch of %d rows is larger than a selection can address", batch.Len)
	}
	rows := make([]uint16, batch.Len)
	for i := range rows {
		rows[i] = uint16(i)
	}
	return rows, nil
}
