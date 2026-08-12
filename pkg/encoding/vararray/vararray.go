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

// Package vararray holds the escaped, delimiter-separated variable-length array
// codec (MarshalVarArray / UnmarshalVarArray and the delimiter/escape bytes). It
// is a dependency-free leaf (imports only stdlib), so consumers that need only
// this codec — e.g. the plugin SDK at pkg/pipeline/sdk — can use it without
// transitively pulling in pkg/encoding's logging dependencies. pkg/encoding
// forwards to these functions, so existing encoding.*VarArray callers are
// unaffected.
package vararray

import (
	"bytes"
	"errors"
)

const (
	// EntityDelimiter is the delimiter for entities in a variable-length array.
	EntityDelimiter = '|'
	// Escape is the escape character for entities in a variable-length array.
	Escape = '\\'
)

// MarshalVarArray marshals a byte slice into a variable-length array format.
// It escapes delimiter and escape characters within the source slice.
func MarshalVarArray(dest, src []byte) []byte {
	if bytes.IndexByte(src, EntityDelimiter) < 0 && bytes.IndexByte(src, Escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, EntityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == EntityDelimiter || b == Escape {
			dest = append(dest, Escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, EntityDelimiter)
	return dest
}

// UnmarshalVarArray unmarshals a variable-length array from src starting at idx.
//
// WARNING: This function mutates src when the entry contains an escape.
// Decoding is then performed in-place by overwriting bytes in src[idx:next) to
// remove escape characters. The decoded value is the view src[idx:end] into the
// same backing array; copy it (for example, with bytes.Clone or
// append([]byte(nil), ...)) if you need to preserve the original encoded buffer
// or keep the decoded value independent of subsequent in-place decoding on the
// same buffer.
//
// An entry with no escape byte is decoded without writing to src at all. Do NOT
// read that as "src is safe from mutation": whether a write happens is a
// property of the DATA, not of the call. A caller may only skip its defensive
// copy behind its own escape check (e.g. bytes.IndexByte(row, Escape) < 0) —
// never unconditionally.
//
// It returns:
//   - end: the index of the first byte after the decoded value (exclusive)
//   - next: the index of the next element (the byte after the delimiter)
//
// The caller can iterate without creating subslices by tracking indices:
//
//	for idx < len(src) {
//	    end, next, err := UnmarshalVarArray(src, idx)
//	    // use src[idx:end]
//	    idx = next
//	}
func UnmarshalVarArray(src []byte, idx int) (int, int, error) {
	if idx >= len(src) {
		return 0, 0, errors.New("empty entity value")
	}
	if src[idx] == EntityDelimiter {
		// Empty value; value is src[idx:idx], next starts after the delimiter.
		return idx, idx + 1, nil
	}
	// Fast path for an entry that carries no escape. The loop below advances
	// writeIdx and readIdx in lockstep until the first escape is consumed, so
	// until then every `src[writeIdx] = b` writes a byte back over itself. An
	// escape-free entry therefore leaves src byte-identical and returns the
	// delimiter position — which two IndexByte scans find without touching the
	// buffer at all. The escape search is bounded by the delimiter so an escape
	// belonging to a LATER entry cannot drag this one onto the slow path.
	//
	// The trade is deliberate: an escape-free entry drops from O(len) to O(1)
	// (-91% at a 256-byte payload). An entry that DOES carry an escape pays the
	// two scans but skips the loop up to the first escape, which for a value with
	// one escape is a large win too (-89% at 256 bytes); only escape-dense values
	// (an escape every few bytes, where the scans buy no skip) pay a net cost.
	// End-to-end the sampler tag-rule benchmarks move -35%.
	//
	// A missing delimiter falls through: the loop reports "invalid escape
	// character" for a trailing escape and "invalid variable array" otherwise,
	// and that distinction is part of the contract.
	start := idx
	if rel := bytes.IndexByte(src[idx:], EntityDelimiter); rel >= 0 {
		esc := bytes.IndexByte(src[idx:idx+rel], Escape)
		if esc < 0 {
			return idx + rel, idx + rel + 1, nil
		}
		// Escaped entry, so the value must be shifted — but the scan is not
		// wasted. Every byte before the first escape is neither escape nor
		// delimiter, so the loop would only self-assign it. Resume at the escape
		// with writeIdx still in lockstep. For a value carrying a single escape
		// this skips nearly the whole entry.
		start = idx + esc
	}
	// Decode in-place: read index i, write index j.
	writeIdx := start
	for readIdx := start; readIdx < len(src); readIdx++ {
		b := src[readIdx]
		switch {
		case b == Escape:
			// Escape must be followed by at least one more byte.
			if readIdx+1 >= len(src) {
				return 0, 0, errors.New("invalid escape character")
			}
			readIdx++
			src[writeIdx] = src[readIdx]
			writeIdx++
		case b == EntityDelimiter:
			// Return end index of decoded value and index after delimiter.
			return writeIdx, readIdx + 1, nil
		default:
			src[writeIdx] = b
			writeIdx++
		}
	}
	return 0, 0, errors.New("invalid variable array")
}
