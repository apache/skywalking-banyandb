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

package lifecycle

import (
	"sort"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// skippedSourceTracker records source segments that must be retained (excluded
// from the delete-after-migration suffix set) because row-replay skipped
// unresolved rows in them. Keyed by segment start instant (UnixNano), the same
// value a segment suffix decodes to. Embedded by the measure and stream
// migration visitors so the retention bookkeeping lives in one place.
type skippedSourceTracker struct {
	starts map[int64]struct{}
}

func newSkippedSourceTracker() skippedSourceTracker {
	return skippedSourceTracker{starts: make(map[int64]struct{})}
}

// recordSkippedSource marks a source segment (by its start instant) as holding
// rows the row-replay could not republish, so it must be retained rather than
// deleted.
func (t *skippedSourceTracker) recordSkippedSource(segmentTR *timestamp.TimeRange) {
	t.starts[segmentTR.Start.UnixNano()] = struct{}{}
}

// SkippedSourceSegmentStarts returns the start instants (UnixNano) of source
// segments that must be retained because row-replay skipped unresolved rows in
// them. The migration excludes these from the delete-after-migration suffix set.
func (t *skippedSourceTracker) SkippedSourceSegmentStarts() []int64 {
	if len(t.starts) == 0 {
		return nil
	}
	out := make([]int64, 0, len(t.starts))
	for start := range t.starts {
		out = append(out, start)
	}
	// Sorted so the retained-suffix log line is reproducible run to run.
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
