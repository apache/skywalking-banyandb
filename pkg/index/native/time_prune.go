// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"fmt"
	"math"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// trieMinCandidates is the smallest candidate set worth narrowing with the
// _timestamp trie. Below it, decoding the few candidates costs less than
// OR-ing the cover's postings, so the per-document time check stays. The
// "all documents" path has no such candidate set and always builds the trie.
const trieMinCandidates = 256

// timeClass is what a query's time range means for one segment.
type timeClass uint8

const (
	// timeDisjoint: no document in the segment can match, so the segment is
	// skipped whole.
	timeDisjoint timeClass = iota
	// timeContained: every timestamped document matches, so the per-document
	// range comparison is redundant.
	timeContained
	// timeOverlap: the segment must be narrowed, either with the trie or with
	// the per-document check.
	timeOverlap
)

// stepInt64 returns value one step away from the range it bounds, saturating at
// the int64 limits rather than wrapping.
//
// An exclusive lower bound has to move up by one and an exclusive upper bound
// down by one, so the direction is named rather than implied.
func stepInt64(value int64, increase bool) int64 {
	if increase {
		if value == -1<<63 {
			return value
		}
		return value + 1
	}
	if value == 1<<63-1 {
		return value
	}
	if value == -1<<63 {
		return value
	}
	return value - 1
}

// emptyTimeRange reports whether a range admits no int64 at all, which has to
// be decided before the bounds are stepped. Saturating a step at the limits
// widens the range rather than narrowing it, so an exclusive lower of MaxInt64,
// an exclusive upper of MinInt64, or a single value excluded at both ends are
// all empty even though the saturated interval would look non-empty.
func emptyTimeRange(timeRange *TimeRange) bool {
	if timeRange.IncludesLower && timeRange.IncludesUpper {
		return timeRange.Lower > timeRange.Upper
	}
	if !timeRange.IncludesLower && timeRange.Lower == math.MaxInt64 {
		return true
	}
	if !timeRange.IncludesUpper && timeRange.Upper == math.MinInt64 {
		return true
	}
	if !timeRange.IncludesLower && !timeRange.IncludesUpper && timeRange.Lower == timeRange.Upper {
		return true
	}
	return timeRange.Lower > timeRange.Upper
}

// classifyTime reports what a query range means for a segment's documents.
//
// Unknown bounds classify as overlap, never as disjoint. That is the safety
// property everything else rests on: a segment whose bounds are missing, or
// whose bounds folded across a sign change so int64(min) > int64(max), keeps
// exactly today's behaviour instead of being pruned on a guess.
func classifyTime(handle *segmentHandle, timeRange *TimeRange) timeClass {
	if timeRange == nil {
		// No restriction at all, so there is nothing to compare and nothing to
		// narrow: the segment is fully covered.
		return timeContained
	}
	if !handle.hasTime {
		return timeOverlap
	}
	if emptyTimeRange(timeRange) {
		// Nothing matches, whatever the segment holds.
		return timeDisjoint
	}
	minBound := int64(handle.timeMin)
	maxBound := int64(handle.timeMax)
	if minBound > maxBound {
		// The bounds were folded with an unsigned comparison across a sign
		// change, which the merge does deliberately to stay byte-identical with
		// the flush path. They still describe the right set, but they cannot be
		// ordered, so they are treated as unknown rather than trusted.
		return timeOverlap
	}
	lower := timeRange.Lower
	if !timeRange.IncludesLower {
		lower = stepInt64(lower, true)
	}
	upper := timeRange.Upper
	if !timeRange.IncludesUpper {
		upper = stepInt64(upper, false)
	}
	if lower > upper {
		// An empty range matches nothing, whatever the segment holds.
		return timeDisjoint
	}
	if maxBound < lower || minBound > upper {
		return timeDisjoint
	}
	if lower <= minBound && maxBound <= upper {
		return timeContained
	}
	return timeOverlap
}

// segmentHasTimestampField reports whether the segment carries a _timestamp
// field at all. A segment without one has no documents a time range can match,
// so the trie is trivially usable.
func segmentHasTimestampField(handle *segmentHandle) (bool, error) {
	fields, fieldsErr := handle.reader.Fields()
	if fieldsErr != nil {
		return false, fmt.Errorf("enumerate segment fields: %w", fieldsErr)
	}
	for _, field := range fields {
		if field == timestampField {
			return true, nil
		}
	}
	return false, nil
}

// segmentTrieUsable reports whether the segment's _timestamp dictionary carries
// the coarse term levels the trie cover relies on.
//
// Every timestamped document is indexed at each precision level, so a segment
// with timestamps that is missing either the shift-4 or the shift-60 level was
// written without the trie, or with a different precision step. Trusting the
// cover there would silently drop documents, so the segment falls back to the
// per-document check instead.
//
//nolint:contextcheck // the nativeice iterator has no context-capable variant.
func segmentTrieUsable(handle *segmentHandle) (bool, error) {
	hasTimestampField, fieldsErr := segmentHasTimestampField(handle)
	if fieldsErr != nil {
		return false, fieldsErr
	}
	if !hasTimestampField {
		return true, nil
	}
	// Probe both ends of the level range with single-key dictionary ranges:
	// [0x24, 0x25) is shift 4 and [0x5C, 0x5D) is shift 60.
	levels := [][2]byte{
		{prefixCodedShiftStart + 4, prefixCodedShiftStart + 5},
		{prefixCodedShiftStart + 60, prefixCodedShiftStart + 61},
	}
	for _, level := range levels {
		iterator, iteratorErr := handle.reader.NewDictionaryTermIterator(timestampField, nil, []byte{level[0]}, []byte{level[1]})
		if iteratorErr != nil {
			return false, fmt.Errorf("probe _timestamp level 0x%x: %w", level[0], iteratorErr)
		}
		term, termErr := iterator.NextTerm()
		closeErr := iterator.Close()
		if termErr != nil {
			return false, fmt.Errorf("read _timestamp level 0x%x: %w", level[0], termErr)
		}
		if closeErr != nil {
			return false, fmt.Errorf("close _timestamp level 0x%x: %w", level[0], closeErr)
		}
		// NextTerm yields a nil term only once the range is exhausted, so a nil
		// here means the level is absent and the cover cannot be trusted.
		if term == nil {
			return false, nil
		}
	}
	return true, nil
}

// prefixCodedShiftStart mirrors nativeice's shift header byte, which leads every
// _timestamp term and therefore sorts all terms of one level together.
const prefixCodedShiftStart = 0x20

// trieUsable returns the segment's cached trie capability, computing it once.
// The probe reads only the pinned reader and immutable handle fields, so it is
// safe to run concurrently with queries on the same view.
func (h *segmentHandle) trieUsable() (bool, error) {
	h.trieOnce.Do(func() {
		h.trieUsableValue, h.trieUsableErr = segmentTrieUsable(h)
	})
	if h.trieUsableErr != nil {
		// A segment that cannot be probed is treated as unusable, which routes
		// it to the per-document check. A wrong answer here costs speed; the
		// other direction would cost results.
		return false, nil
	}
	return h.trieUsableValue, nil
}

// trieCandidates returns the exact set of documents in a segment whose
// timestamp falls in the query range.
//
// It turns the range into a cover of _timestamp dictionary term ranges and ORs
// their postings. Because the writer indexes every document under one term per
// precision level, and the cover partitions the range into disjoint buckets, a
// document is in the result exactly when its timestamp is in range.
//
// Documents without a timestamp carry no _timestamp terms and so are excluded,
// which matches the contract that a time range never returns them. Deleted
// documents may appear and are still filtered where they are today.
//
//nolint:contextcheck // the nativeice dictionary iterator has no context-capable variant.
func trieCandidates(ctx context.Context, segment *memorySegment, timeRange *TimeRange) (*roaringpkg.Bitmap, error) {
	result := roaringpkg.New()
	lower := timeRange.Lower
	if !timeRange.IncludesLower {
		lower = stepInt64(lower, true)
	}
	upper := timeRange.Upper
	if !timeRange.IncludesUpper {
		upper = stepInt64(upper, false)
	}
	if lower > upper {
		return result, nil
	}
	handle := segment.handle
	covers := nativeice.TimestampCoverKeys(sortableInt64(lower), sortableInt64(upper))
	for _, cover := range covers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		iterator, iteratorErr := handle.reader.NewDictionaryTermIterator(timestampField, nil, cover.Start, cover.End)
		if iteratorErr != nil {
			return nil, fmt.Errorf("iterate _timestamp cover: %w", iteratorErr)
		}
		for {
			if err := ctx.Err(); err != nil {
				_ = iterator.Close()
				return nil, err
			}
			term, termErr := iterator.NextTerm()
			if termErr != nil {
				_ = iterator.Close()
				return nil, fmt.Errorf("read _timestamp cover term: %w", termErr)
			}
			if term == nil {
				break
			}
			posting, found, postingErr := handle.reader.TermPostingBitmap(timestampField, term)
			if postingErr != nil {
				_ = iterator.Close()
				return nil, fmt.Errorf("read _timestamp posting: %w", postingErr)
			}
			if found {
				result.Or(posting)
			}
		}
		if closeErr := iterator.Close(); closeErr != nil {
			return nil, fmt.Errorf("close _timestamp cover iterator: %w", closeErr)
		}
	}
	return result, nil
}

// sortableInt64 maps a signed int64 into unsigned order that matches signed
// order, which is the transform every _timestamp term is built on.
func sortableInt64(value int64) uint64 {
	return uint64(value) ^ 0x8000000000000000
}

// narrowCandidatesToRange intersects a candidate set with the segment's
// in-range documents, and reports whether the result is exact.
//
// exact is true when the caller may stop comparing each candidate's timestamp
// against the range: either the segment is wholly contained, or the trie
// already selected precisely the in-range documents. It is false when the
// caller must keep the per-document check.
//
// candidates may be nil, meaning "every document in the segment"; the result is
// then the trie set itself.
func narrowCandidatesToRange(
	ctx context.Context,
	segment *memorySegment,
	candidates *roaringpkg.Bitmap,
	class timeClass,
	timeRange *TimeRange,
) (narrowed *roaringpkg.Bitmap, exact bool, err error) {
	switch class {
	case timeDisjoint:
		return roaringpkg.New(), true, nil
	case timeContained:
		if candidates == nil {
			// Every document, and every timestamped one is in range. Documents
			// without a timestamp still have to be dropped, which the caller's
			// hasTimestamp check does.
			return nil, true, nil
		}
		return candidates, true, nil
	default:
		usable, usableErr := segment.handle.trieUsable()
		if usableErr != nil {
			return candidates, false, nil
		}
		if !usable {
			return candidates, false, nil
		}
		if candidates == nil {
			trie, trieErr := trieCandidates(ctx, segment, timeRange)
			if trieErr != nil {
				return nil, false, trieErr
			}
			return trie, true, nil
		}
		// With few candidates the per-document check is cheaper than OR-ing the
		// cover's postings.
		if candidates.GetCardinality() < trieMinCandidates {
			return candidates, false, nil
		}
		trie, trieErr := trieCandidates(ctx, segment, timeRange)
		if trieErr != nil {
			return nil, false, trieErr
		}
		return roaringpkg.And(candidates, trie), true, nil
	}
}
