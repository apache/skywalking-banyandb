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

package native

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

const (
	// trieLevelFanout is the number of buckets of one _timestamp level that a bucket of the next coarser level holds.
	trieLevelFanout = 16
	// trieLevelStep is the shift between two _timestamp levels.
	trieLevelStep = 4
	// trieTopShift is the coarsest _timestamp level the writer emits.
	trieTopShift = 60
	// topTimeKeepFactor bounds the candidate set a descent keeps: a crossing bucket that holds at most this many times the
	// limit is taken whole instead of being refined further.
	topTimeKeepFactor = 2
)

// TopTimeRequest asks for the first Limit documents, in timestamp order, among the candidates of one term-set request.
// The request's Scope.TimeRange is required: documents without a timestamp never match a time range, which is what
// makes the _timestamp trie a complete index of the candidates.
type TopTimeRequest struct {
	Candidates TermSetRequest
	Limit      int
	Desc       bool
}

// TopTimeHits returns at most Limit candidates in timestamp order (newest first with Desc), ties by identifier, segment
// and document, the order SortHits gives the _timestamp field. It does not read every candidate: per segment it walks the
// _timestamp trie from the coarsest useful level, takes the buckets that rank inside the first Limit whole, refines only
// the bucket that holds the Limit-th document, and decodes the few documents that remain. A segment whose trie or time
// bounds cannot be trusted, or whose candidates are few, is read in full instead.
//
//nolint:gocyclo // one segment loop with the fallback and threshold decisions kept explicit.
func (v *ReadView) TopTimeHits(ctx context.Context, request TopTimeRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.Limit <= 0 {
		return nil, fmt.Errorf("top time requires a positive limit: %w", ErrInvalidQuery)
	}
	if request.Candidates.Scope.TimeRange == nil {
		return nil, fmt.Errorf("top time requires a time range: %w", ErrInvalidQuery)
	}
	if termSetRequestEmpty(request.Candidates) {
		return nil, nil
	}
	if err := validateTermSetRequest(request.Candidates); err != nil {
		return nil, err
	}
	patterns, patternErr := compileTermPatterns(ctx, request.Candidates, nil)
	if patternErr != nil {
		return nil, patternErr
	}
	timeRange := request.Candidates.Scope.TimeRange
	best := make([]QueryHit, 0, request.Limit)
	for _, segmentIndex := range v.topTimeSegmentOrder(request.Desc) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := v.root.segments[segmentIndex].(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("top time segment %d has type %T", segmentIndex, v.root.segments[segmentIndex])
		}
		threshold, hasThreshold := int64(0), len(best) >= request.Limit
		if hasThreshold {
			threshold = best[len(best)-1].Timestamp
			if topTimeSegmentCannotBeat(segment.handle, threshold, request.Desc) {
				continue
			}
		}
		candidates, err := exactCandidates(ctx, segment, request.Candidates, patterns)
		if err != nil {
			return nil, err
		}
		narrowed, projection, skip, err := narrowSegmentForTime(ctx, v.owner, segment, candidates, timeRange)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		selected, err := topTimeSelection(ctx, segment, narrowed, projection, request, threshold, hasThreshold)
		if err != nil {
			return nil, err
		}
		hits, err := projectCandidates(ctx, uint64(segmentIndex), segment, selected, timeRange, projection)
		if err != nil {
			return nil, err
		}
		best = mergeTopTime(best, hits, request.Limit, request.Desc)
	}
	return best, nil
}

// topTimeSegmentOrder returns the segment ordinals in the order that ranks first, so that the Limit-th timestamp found so
// far prunes the later segments: newest first by upper bound for Desc, oldest first by lower bound otherwise. A segment
// without trustworthy bounds cannot be pruned and goes first.
func (v *ReadView) topTimeSegmentOrder(desc bool) []int {
	type bound struct {
		key     int64
		ordinal int
		known   bool
	}
	bounds := make([]bound, 0, len(v.root.segments))
	for ordinal, current := range v.root.segments {
		entry := bound{ordinal: ordinal}
		if segment, ok := current.(*memorySegment); ok {
			if minBound, maxBound, known := segmentTimeBounds(segment.handle); known {
				entry.known = true
				entry.key = minBound
				if desc {
					entry.key = maxBound
				}
			}
		}
		bounds = append(bounds, entry)
	}
	sort.SliceStable(bounds, func(i, j int) bool {
		if bounds[i].known != bounds[j].known {
			return !bounds[i].known
		}
		if desc {
			return bounds[i].key > bounds[j].key
		}
		return bounds[i].key < bounds[j].key
	})
	order := make([]int, len(bounds))
	for i := range bounds {
		order[i] = bounds[i].ordinal
	}
	return order
}

// segmentTimeBounds returns a segment's timestamp bounds, or false when they are unknown or cannot be ordered (the merge
// folds them with an unsigned comparison, so a sign change makes them untrustworthy; classifyTime treats them the same).
func segmentTimeBounds(handle *segmentHandle) (minBound, maxBound int64, known bool) {
	if !handle.hasTime {
		return 0, 0, false
	}
	minBound, maxBound = int64(handle.timeMin), int64(handle.timeMax)
	if minBound > maxBound {
		return 0, 0, false
	}
	return minBound, maxBound, true
}

// topTimeSegmentCannotBeat reports that no document of the segment can rank before the current Limit-th one. A tie at the
// threshold may still displace it by identifier, so only a strictly worse range is pruned.
func topTimeSegmentCannotBeat(handle *segmentHandle, threshold int64, desc bool) bool {
	minBound, maxBound, known := segmentTimeBounds(handle)
	if !known {
		return false
	}
	if desc {
		return maxBound < threshold
	}
	return minBound > threshold
}

// topTimeSelection chooses the documents of one segment that can be among the first Limit: the trie descent when the
// narrowing is exact and the candidates are many, every candidate otherwise.
func topTimeSelection(
	ctx context.Context, segment *memorySegment, narrowed *roaringpkg.Bitmap, projection timeProjection,
	request TopTimeRequest, threshold int64, hasThreshold bool,
) (*roaringpkg.Bitmap, error) {
	if narrowed == nil || narrowed.IsEmpty() {
		return roaringpkg.New(), nil
	}
	minBound, maxBound, known := segmentTimeBounds(segment.handle)
	keep := uint64(request.Limit) * topTimeKeepFactor
	if projection != timeProjectionContained || !known || !segment.handle.trieUsable() || narrowed.GetCardinality() <= keep {
		return narrowed, nil
	}
	live := narrowed
	if len(segment.deleted) > 0 {
		live = narrowed.Clone()
		for number := range segment.deleted {
			live.Remove(uint32(number))
		}
	}
	lower, upper := sortableInt64(minBound), sortableInt64(maxBound)
	if hasThreshold {
		if request.Desc {
			lower = max(lower, sortableInt64(threshold))
		} else {
			upper = min(upper, sortableInt64(threshold))
		}
	}
	if lower > upper {
		return roaringpkg.New(), nil
	}
	return trieDescent(ctx, segment, timestampField, live, lower, upper, request.Desc, uint64(request.Limit))
}

// trieDescent returns a set of documents of live, whose keys on the ladder of field are in [lower, upper] (sortable units),
// that contains the first limit documents in key order and at most about topTimeKeepFactor*limit more. The field is
// _timestamp for TopTimeHits and a companion ladder field for TopFieldHits. It reads the posting of one
// bucket at a time, from the best bucket on, so its cost depends on limit and the number of levels, not on the
// number of documents.
//
// Every document of live that has the field is on its ladder at every level (the writer emits all sixteen), so a bucket's
// posting restricted to live is exactly the live documents of that bucket. A document without the ladder is never selected;
// the caller decides whether a short result needs a fallback. A bucket taken whole ranks before every bucket after it;
// the bucket that holds the limit-th document is refined one level down, or taken whole when it is small or at level 0, so
// documents tied with the limit-th are never cut.
func trieDescent(
	ctx context.Context, segment *memorySegment, field string, live *roaringpkg.Bitmap, lower, upper uint64, desc bool, limit uint64,
) (*roaringpkg.Bitmap, error) {
	shift := uint(0)
	for shift < trieTopShift && (upper>>shift)-(lower>>shift) >= trieLevelFanout {
		shift += trieLevelStep
	}
	selected := roaringpkg.New()
	need := limit
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		first, last := lower>>shift, upper>>shift
		crossing, crossingID, err := trieWalkLevel(ctx, segment, field, live, selected, &need, first, last, shift, desc)
		if err != nil {
			return nil, err
		}
		if crossing == nil {
			return selected, nil
		}
		if shift == 0 || crossing.GetCardinality() <= limit*topTimeKeepFactor {
			if reserveErr := reserveBitmapMerge(ctx, selected, crossing); reserveErr != nil {
				return nil, reserveErr
			}
			selected.Or(crossing)
			return selected, nil
		}
		shift -= trieLevelStep
		bucketLower, bucketUpper := crossingID<<(shift+trieLevelStep), crossingID<<(shift+trieLevelStep)|(uint64(1)<<(shift+trieLevelStep)-1)
		lower, upper = max(lower, bucketLower), min(upper, bucketUpper)
	}
}

// trieWalkLevel visits the buckets of one level between first and last in rank order. A bucket with fewer live documents
// than still needed is added to selected whole; the first bucket that holds at least that many is returned (restricted to
// live) as the crossing bucket. A nil crossing means the level ran out before the limit was reached.
func trieWalkLevel(
	ctx context.Context, segment *memorySegment, field string, live, selected *roaringpkg.Bitmap, need *uint64, first, last uint64, shift uint, desc bool,
) (crossing *roaringpkg.Bitmap, crossingID uint64, err error) {
	for step := uint64(0); step <= last-first; step++ {
		id := first + step
		if desc {
			id = last - step
		}
		if err = ctx.Err(); err != nil {
			return nil, 0, err
		}
		posting, found, postingErr := segment.handle.reader.TermPostingBitmapContext(ctx, field, bucketTerm(id, shift))
		if postingErr != nil {
			return nil, 0, fmt.Errorf("read _timestamp bucket posting: %w", postingErr)
		}
		if !found {
			continue
		}
		inBucket := roaringpkg.And(posting, live)
		count := inBucket.GetCardinality()
		if count == 0 {
			continue
		}
		if count >= *need {
			return inBucket, id, nil
		}
		if reserveErr := reserveBitmapMerge(ctx, selected, inBucket); reserveErr != nil {
			return nil, 0, reserveErr
		}
		selected.Or(inBucket)
		*need -= count
	}
	return nil, 0, nil
}

// bucketTerm is the ladder term of bucket id (the sortable value shifted right by shift) at the shift level.
func bucketTerm(id uint64, shift uint) []byte {
	return nativeice.EncodePrefixCodedInt64Shift(int64((id<<shift)^0x8000000000000000), shift)
}

// mergeTopTime folds hits into best, both in timestamp order, and keeps the first limit.
func mergeTopTime(best, hits []QueryHit, limit int, desc bool) []QueryHit {
	if len(hits) == 0 {
		return best
	}
	best = append(best, hits...)
	sort.SliceStable(best, func(i, j int) bool { return topTimeLess(best[i], best[j], desc) })
	if len(best) > limit {
		best = best[:limit]
	}
	return best
}

// topTimeLess is the order SortHits gives the _timestamp field: by timestamp, then identifier, segment and document.
func topTimeLess(left, right QueryHit, desc bool) bool {
	if left.Timestamp != right.Timestamp {
		if desc {
			return left.Timestamp > right.Timestamp
		}
		return left.Timestamp < right.Timestamp
	}
	if c := bytes.Compare(left.Identifier, right.Identifier); c != 0 {
		return c < 0
	}
	if left.Segment != right.Segment {
		return left.Segment < right.Segment
	}
	return left.DocumentNumber < right.DocumentNumber
}
