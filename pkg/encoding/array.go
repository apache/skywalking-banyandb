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

package encoding

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
// It decodes in-place and returns:
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
	// Decode in-place: read index i, write index j.
	writeIdx := idx
	for readIdx := idx; readIdx < len(src); readIdx++ {
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
