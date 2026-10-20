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
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// consumeRuns feeds b to the open bucket aggregator run by run when b describes
// where its grouping key changes, instead of flooring and visiting every row. It
// reports false, having changed nothing, when it does not apply.
//
// It applies when the batch knows its series and its timestamps are ordered within
// each series run (RecordBatch.Hints.TimeOrder, asc or desc), the timestamp column
// has no null, and
// every tag key is constant within a series run (a ConstColumn, or an entity tag of
// a batch that knows its series). The batch is cut into series runs, and each series
// run into bucket runs: the end of a bucket run is found by galloping on the
// timestamps. Each bucket run then goes through exactly the state machine the per-row
// path applies to its first row (open the first bucket, stay, advance and flush, or
// fail on a regression), and is folded into its group with one lookup.
//
// The timestamp column is not floored in place, so a lent column is not copied.
func (bt *BatchTimeBucket) consumeRuns(ctx context.Context, b *vectorized.RecordBatch) (bool, error) {
	if runsDisabled.Load() || bt.mode == AggModeReduce || !b.Hints.SeriesKnown || b.Hints.TimeOrder == vectorized.TimeOrderUnknown {
		return false, nil
	}
	if b.Selection == nil && b.Len > maxNilSelectionLen {
		// Let the per-row path report the oversized batch.
		return false, nil
	}
	tsCol, ok := b.Columns[bt.timestampIdx].(*vectorized.TypedColumn[int64])
	if !ok || tsCol.MayHaveNulls() {
		return false, nil
	}
	if _, ok = keyConstancy(b, bt.aggKeyIndices, bt.keyEntity, bt.timestampIdx); !ok || tooFragmented(b) {
		return false, nil
	}
	n := b.ActiveLen()
	if n == 0 {
		return true, nil
	}
	// Timestamps are ordered within a series run, not across the batch, so the bucket
	// runs are found inside each series run even when the key does not depend on the
	// series: a group simply receives the runs of every series that falls into it.
	runs, _ := b.SeriesRuns(bt.runScratch[:0])
	bt.runScratch = runs
	bt.runBatches++
	b = expandConstColumns(b, bt.aggs, nil)
	ts, sel := tsCol.Data(), b.Selection
	verify := verifyRuns.Load()
	pos := 0
	for _, seriesEnd := range runs {
		for pos < seriesEnd {
			bucket := bucketStart(ts[rowAt(sel, pos)], bt.widthNanos)
			end := bucketRunEnd(ts, sel, pos, seriesEnd, bucket, bt.widthNanos)
			if verify {
				checkBucketRun(ts, sel, pos, end, seriesEnd, bucket, bt.widthNanos)
			}
			if advanceErr := bt.enterBucket(ctx, bucket); advanceErr != nil {
				return true, advanceErr
			}
			if foldErr := bt.currentAgg.foldRun(b, pos, end, bucket); foldErr != nil {
				return true, foldErr
			}
			pos = end
		}
	}
	return true, nil
}

// enterBucket applies to the run of bucket the state machine consumeBatch applies
// to a row of that bucket. Map mode keeps one aggregator for the whole scan.
func (bt *BatchTimeBucket) enterBucket(ctx context.Context, bucket int64) error {
	if !bt.streaming {
		if !bt.hasBucket {
			bt.hasBucket = true
			return bt.openBucket(ctx)
		}
		return nil
	}
	switch {
	case !bt.hasBucket:
		bt.hasBucket = true
		bt.currentBucket = bucket
		return bt.openBucket(ctx)
	case bucket == bt.currentBucket:
		return nil
	case bucket > bt.currentBucket:
		if flushErr := bt.flushCurrentBucket(ctx); flushErr != nil {
			return flushErr
		}
		bt.currentBucket = bucket
		return bt.openBucket(ctx)
	default:
		return fmt.Errorf("vectorized.measure: BatchTimeBucket: input is not time-ascending — row bucket %d precedes the current bucket %d", bucket, bt.currentBucket)
	}
}
