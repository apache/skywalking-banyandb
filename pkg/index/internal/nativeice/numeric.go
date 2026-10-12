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
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import "fmt"

const prefixCodedInt64ShiftStart byte = 0x20

// EncodePrefixCodedInt64 encodes a full-precision signed integer in the
// shift-zero format used by the established date/time stored-field format.
// The returned bytes are owned by the caller.
func EncodePrefixCodedInt64(value int64) []byte {
	return EncodePrefixCodedInt64Shift(value, 0)
}

// EncodePrefixCodedInt64Shift encodes an int64 numeric term at the requested
// precision shift. It matches the legacy numeric/date term format, where each
// byte carries seven sortable bits and the header records the shift.
func EncodePrefixCodedInt64Shift(value int64, shift uint) []byte {
	if shift > 63 {
		return nil
	}
	return encodePrefixCodedSortableShift(uint64(value)^0x8000000000000000, shift)
}

// encodePrefixCodedSortableShift is EncodePrefixCodedInt64Shift without the
// sign flip: it encodes a value already in sign-flipped sortable order, so
// unsigned order matches signed order.
//
// Callers that hold a sign-flipped value need this rather than re-deriving it,
// because a range cover is computed in sortable units and then has to be
// encoded per shift without flipping again.
func encodePrefixCodedSortableShift(sortable uint64, shift uint) []byte {
	if shift > 63 {
		return nil
	}
	nChars := ((63 - shift) / 7) + 1
	encoded := make([]byte, nChars+1)
	encoded[0] = prefixCodedInt64ShiftStart + byte(shift)
	sortable >>= shift
	for index := len(encoded) - 1; index > 0; index-- {
		encoded[index] = byte(sortable & 0x7f)
		sortable >>= 7
	}
	return encoded
}

// sortableInt64 maps a signed int64 into unsigned order that matches signed
// order, which is the transform every _timestamp term is built on.
func sortableInt64(value int64) uint64 {
	return uint64(value) ^ 0x8000000000000000
}

// sortableRangeBounds is the widest trie cover SplitSortableRange can produce:
// two full lower and upper staircases of 15 buckets each, plus the 16 buckets
// of one aligned run at a single level.
const sortableRangeMaxTerms = 2*15*15 + 16

// TimestampCover runs covers the closed sortable interval [lo, hi] with
// dictionary term ranges instead of bucket indices, so a caller can iterate a
// segment's _timestamp dictionary directly.
type TimestampCover struct {
	// Start is the first dictionary key in the run, inclusive.
	Start []byte
	// End is the exclusive dictionary key one past the run. It is nil only
	// where the run reaches the top shift, where no larger key exists at this
	// level.
	End []byte
}

// TimestampCoverKeys runs SplitSortableRange over [lo, hi] and turns each
// emitted bucket run into the dictionary bounds that select it. Keys are in
// sortable units, so a caller passes sortableInt64 of its own range.
//
// The end key is the encoding of the bucket after the run, because dictionary
// iterators take an exclusive bound. Where the run reaches the largest bucket
// at its level there is no such key, so the end becomes the first key of the
// next shift level: the shift byte leads each key, so every key of this level
// sorts below it. Above the top shift the end is nil, which an iterator reads
// as unbounded.
func TimestampCoverKeys(lo, hi uint64) []TimestampCover {
	runs := make([]struct {
		shift       uint
		first, last uint64
	}, 0, 32)
	SplitSortableRange(lo, hi, func(shift uint, first, last uint64) {
		runs = append(runs, struct {
			shift       uint
			first, last uint64
		}{shift: shift, first: first, last: last})
	})
	covers := make([]TimestampCover, 0, len(runs))
	for _, run := range runs {
		// A run's buckets are already shifted down, and the encoder shifts
		// right again, so they have to be shifted back up before encoding.
		covers = append(covers, TimestampCover{
			Start: encodePrefixCodedSortableShift(run.first<<run.shift, run.shift),
			End:   coverRunEnd(run.shift, run.last),
		})
	}
	return covers
}

// coverRunEnd returns the exclusive dictionary bound just past a run that ends
// at bucket last of the given shift.
func coverRunEnd(shift uint, last uint64) []byte {
	if shift >= 60 {
		// 60 is the highest shift the writer emits, so nothing sorts above it.
		return nil
	}
	if last < maxSortableBucket(shift) {
		return encodePrefixCodedSortableShift((last+1)<<shift, shift)
	}
	// last is the largest bucket this level can express, so bound the run by
	// the first key of the next level instead.
	return []byte{prefixCodedInt64ShiftStart + byte(shift) + 1}
}

// maxSortableBucket is the largest bucket a document's timestamp can fall in
// at this shift, that is the largest value of ts>>shift.
//
// It is the value range rather than the encoder's seven-bits-per-byte width,
// which is always at least as wide: the encoder's width is
// 7*ceil((63-shift)/7 + 1) bits against a value range of 64-shift bits. So
// last+1 always encodes distinctly below this bound, and only a run reaching
// this bound needs the next-shift-level end key.
func maxSortableBucket(shift uint) uint64 {
	if shift >= 64 {
		return 0
	}
	return uint64(1)<<(64-shift) - 1
}

// SplitSortableRange covers the closed sortable interval [lo, hi] with the
// fewest possible trie buckets, calling emit once per run of consecutive
// buckets as (shift, first, last) in sortable units at that shift.
//
// It is Lucene's NumericUtils.splitRange specialised to a 64-bit value and the
// prefix-coded precision step of 4 that the writer emits, and exists so a time
// range becomes a handful of _timestamp terms instead of a scan. Because the
// writer indexes every document under one term per level, and this cover
// partitions [lo, hi] into disjoint buckets, a document is in the union of the
// emitted runs' postings if and only if its timestamp is in [lo, hi].
//
// The cover never exceeds sortableRangeMaxTerms terms, and for a realistic
// window it is a few dozen: a 15-minute window is about 12 buckets at shift 36
// plus at most 15 at each finer level on either edge.
//
// emit must not retain the shift or bucket values; they are only valid for the
// duration of the call.
func SplitSortableRange(lo, hi uint64, emit func(shift uint, first, last uint64)) {
	if lo > hi {
		return
	}
	for shift := uint(0); ; shift += 4 {
		diff := uint64(1) << (shift + 4)
		mask := uint64(0xF) << shift
		hasLower := lo&mask != 0
		hasUpper := hi&mask != mask
		nextLo := lo
		if hasLower {
			nextLo += diff
		}
		nextLo &^= mask
		nextHi := hi
		if hasUpper {
			nextHi -= diff
		}
		nextHi &^= mask
		if shift+4 >= 64 || nextLo > nextHi || nextLo < lo || nextHi > hi {
			// Either the next level would not narrow the range, or it would
			// step outside it, so the remaining middle is emitted whole here.
			emit(shift, lo>>shift, hi>>shift)
			return
		}
		if hasLower {
			emit(shift, lo>>shift, (lo|mask)>>shift)
		}
		if hasUpper {
			emit(shift, (hi&^mask)>>shift, hi>>shift)
		}
		lo, hi = nextLo, nextHi
	}
}

// DecodePrefixCodedInt64 decodes the shift-zero prefix-coded signed integer
// used by the established date/time stored-field format. It intentionally
// accepts only shift-zero values: date/time fields are full-precision int64
// nanoseconds, while shifted terms are query-analysis tokens rather than a
// stored timestamp value.
func DecodePrefixCodedInt64(value []byte) (int64, error) {
	if len(value) == 0 || value[0] != prefixCodedInt64ShiftStart {
		return 0, fmt.Errorf("invalid prefix-coded int64 header: %w", ErrCorrupt)
	}
	const shiftZeroLength = 11
	if len(value) != shiftZeroLength {
		return 0, fmt.Errorf("prefix-coded int64 length %d, want %d: %w", len(value), shiftZeroLength, ErrCorrupt)
	}
	if value[1] > 1 {
		return 0, fmt.Errorf("prefix-coded int64 high digit 0x%x overflows int64: %w", value[1], ErrCorrupt)
	}
	var sortable uint64
	for _, encodedByte := range value[1:] {
		if encodedByte > 0x7f {
			return 0, fmt.Errorf("invalid prefix-coded int64 byte 0x%x: %w", encodedByte, ErrCorrupt)
		}
		sortable = (sortable << 7) | uint64(encodedByte)
	}
	return int64(sortable ^ 0x8000000000000000), nil
}
