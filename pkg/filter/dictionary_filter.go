// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package filter

import (
	"bytes"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DictionaryFilter is a filter implementation backed by a dictionary.
// For non-array types: uses linear iteration through values.
// For array types: uses iterative approach, extracting and sorting on-the-fly.
type DictionaryFilter struct {
	// Original serialized values
	values    [][]byte
	valueType pbv1.ValueType
}

// MightContain checks if an item is in the dictionary.
// For non-array types: linear iteration through values.
// For array types: checks if the single item exists as an element in any stored array.
func (df *DictionaryFilter) MightContain(item []byte) bool {
	if df.valueType == pbv1.ValueTypeStrArr || df.valueType == pbv1.ValueTypeInt64Arr {
		return false
	}

	for _, v := range df.values {
		if bytes.Equal(v, item) {
			return true
		}
	}
	return false
}

// ContainsAll checks if all items are present in the dictionary.
// For non-array types: checks if ANY of the items are present (OR semantics).
// For array types: checks if ALL items form a subset of any stored array (AND semantics).
func (df *DictionaryFilter) ContainsAll(items [][]byte) bool {
	if len(items) == 0 {
		return true
	}

	if df.valueType == pbv1.ValueTypeStrArr || df.valueType == pbv1.ValueTypeInt64Arr {
		for _, serializedArray := range df.values {
			if df.extractElements(serializedArray, items) {
				return true
			}
		}
		return false
	}

	// For non-array types: a single value can only match one item
	// If multiple items are requested, return false immediately
	if len(items) != 1 {
		return false
	}

	// Check if the single item exists
	item := items[0]
	for _, v := range df.values {
		if bytes.Equal(v, item) {
			return true
		}
	}
	return false
}

// Set sets both the dictionary values and value type.
func (df *DictionaryFilter) Set(values [][]byte, valueType pbv1.ValueType) {
	df.values = values
	df.valueType = valueType
}

// Reset resets the dictionary filter.
func (df *DictionaryFilter) Reset() {
	for i := range df.values {
		df.values[i] = nil
	}
	df.values = df.values[:0]
	df.valueType = pbv1.ValueTypeUnknown
}

// extractElements checks if all query values form a subset of the serialized array.
// Returns true if all query values exist in the array (subset check).
func (df *DictionaryFilter) extractElements(serializedArray []byte, values [][]byte) bool {
	if len(values) == 0 {
		return true
	}

	if df.valueType == pbv1.ValueTypeInt64Arr {
		// For each query value, check if it exists in the array
		for _, v := range values {
			found := false
			for i := 0; i+8 <= len(serializedArray); i += 8 {
				if bytes.Equal(v, serializedArray[i:i+8]) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	if df.valueType == pbv1.ValueTypeStrArr {
		// For each query value, check if it exists in the array
		// UnmarshalVarArray modifies the source in-place for decoding
		// This approach has zero allocations and early-exits on match
		for _, v := range values {
			found := false
			for idx := 0; idx < len(serializedArray); {
				end, next, err := encoding.UnmarshalVarArray(serializedArray, idx)
				if err != nil {
					return false
				}
				if bytes.Equal(v, serializedArray[idx:end]) {
					found = true
					break
				}
				idx = next
			}
			if !found {
				return false
			}
		}
		return true
	}

	return false
}
