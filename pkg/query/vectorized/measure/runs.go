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
	"fmt"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Runs.
//
// A run is a maximal stretch of consecutive active rows that share a grouping key.
// An aggregation that knows the runs of a batch looks its group up once per run and
// folds the run's values in a typed loop, instead of encoding a key, hashing it and
// folding one row at a time. A run list is just the positions, in the active order,
// where the key changes, so the same list serves every way the key can be constant:
//
//   - every key column is a vectorized.ConstColumn: one run, the whole batch;
//   - the key columns are entity tags and the batch says where each series ends
//     (vectorized.RecordBatch.SeriesRuns): one run per series run;
//   - the key includes a time bucket and the timestamps are ordered within a series
//     run (RecordBatch.Hints.TimeOrder): the run's edges are found by searching for
//     the next bucket, not by visiting every row.
//
// A run changes only how a group is looked up and how its values are added. Rows are
// still folded in active order, group by group in the order the groups first
// appear, so the result, including every float sum, is that of the per-row path.

// runsDisabled switches every runs fast path off, leaving the per-row path. It
// exists so equivalence tests can compare the two.
var runsDisabled atomic.Bool

// verifyRuns makes the run fast paths re-derive each run row by row and panic when
// the producer's hints were wrong. Tests turn it on; production never does.
var verifyRuns atomic.Bool

// SetRunsDisabled switches the runs fast paths of BatchAggregation and
// BatchTimeBucket off (true) or on (false) and returns the previous setting. It is
// for tests that compare the fast paths with the per-row path.
func SetRunsDisabled(disabled bool) (previous bool) { return runsDisabled.Swap(disabled) }

// SetRunsVerification makes the runs fast paths cross-check every run against the
// rows it covers (true) and returns the previous setting. It is for tests.
func SetRunsVerification(verify bool) (previous bool) { return verifyRuns.Swap(verify) }

// rowAt returns the column row of the pos-th active row.
func rowAt(sel []uint16, pos int) int {
	if sel == nil {
		return pos
	}
	return int(sel[pos])
}

// keyConstancy reports whether every key column of a batch is constant within the
// runs the batch can describe, and whether those runs are the batch's series runs.
// A key column is constant when it is a ConstColumn, or when it is an entity tag and
// the batch knows its series. skip is a key column the caller treats separately (the
// time bucket) or -1. ok is false when some key column is neither.
func keyConstancy(b *vectorized.RecordBatch, keyIndices []int, keyEntity []bool, skip int) (series, ok bool) {
	for i, kIdx := range keyIndices {
		if kIdx == skip {
			continue
		}
		if _, isConst := vectorized.AsConst(b.Columns[kIdx]); isConst {
			continue
		}
		if keyEntity[i] && b.Hints.SeriesKnown {
			series = true
			continue
		}
		return false, false
	}
	return series, true
}

// minRunRows is the average run length below which the runs paths are not worth it:
// a run costs about what a row does on the per-row path (a key, a lookup, a fold
// call), so a batch that interleaves series row by row is better folded row by row.
const minRunRows = 3

// tooFragmented reports whether b is known to split into so many series runs that
// the runs paths would cost more than the per-row path. It looks only at the number
// of series ends the producer listed, an upper bound on the runs of the batch.
func tooFragmented(b *vectorized.RecordBatch) bool {
	return len(b.Hints.SeriesEnds)*minRunRows > b.ActiveLen()
}

// keyEntityFlags lists, per key column, whether the input schema marks it as an
// entity tag.
func keyEntityFlags(input *vectorized.BatchSchema, keyIndices []int) []bool {
	flags := make([]bool, len(keyIndices))
	for i, kIdx := range keyIndices {
		flags[i] = input.Columns[kIdx].Entity
	}
	return flags
}

// bucketRunEnd returns the end, exclusive, of the run that starts at position from
// and ends no later than to: the rows whose timestamp floors to bucket. The
// timestamps of the active rows in [from, to) must be monotone, so rows of one
// bucket are contiguous; the edge is then found by galloping from the start of the
// run and bisecting the last step, which costs O(log run length) probes, not one per
// row.
func bucketRunEnd(ts []int64, sel []uint16, from, to int, bucket, width int64) int {
	inRun := func(pos int) bool { return bucketStart(ts[rowAt(sel, pos)], width) == bucket }
	lo := from + 1 // every position below lo is in the run
	hi := lo
	step := 1
	for hi < to && inRun(hi) {
		lo = hi + 1
		hi += step
		step <<= 1
	}
	hi = min(hi, to)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if inRun(mid) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// checkBucketRun panics unless every row in positions [from, end) of the active
// order floors to bucket and, when more rows follow before limit, the next row does
// not. It is the verification half of SetRunsVerification.
func checkBucketRun(ts []int64, sel []uint16, from, end, limit int, bucket, width int64) {
	for pos := from; pos < end; pos++ {
		if got := bucketStart(ts[rowAt(sel, pos)], width); got != bucket {
			panic(fmt.Sprintf("vectorized.measure: bucket run [%d,%d) of bucket %d holds position %d of bucket %d: the batch's TimeOrder hint is wrong",
				from, end, bucket, pos, got))
		}
	}
	if end < limit {
		if got := bucketStart(ts[rowAt(sel, end)], width); got == bucket {
			panic(fmt.Sprintf("vectorized.measure: bucket run [%d,%d) of bucket %d stops before position %d of the same bucket: the batch's TimeOrder hint is wrong",
				from, end, bucket, end))
		}
	}
}
