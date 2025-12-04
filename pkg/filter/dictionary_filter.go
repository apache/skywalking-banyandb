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
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DictionaryFilter is a filter implementation backed by a dictionary.
// It uses a map-based lookup for O(1) performance instead of O(n) linear search.
type DictionaryFilter struct {
	valueSet  map[string]struct{}
	values    [][]byte
	valueType pbv1.ValueType
}

// NewDictionaryFilter creates a new dictionary filter with the given values.
func NewDictionaryFilter(values [][]byte) *DictionaryFilter {
	df := &DictionaryFilter{
		values: values,
	}
	df.buildValueSet()
	return df
}

// MightContain checks if an item is in the dictionary.
// For non-array types, it uses O(1) map lookup instead of O(n) linear search.
func (df *DictionaryFilter) MightContain(item []byte) bool {
	if df.valueType == pbv1.ValueTypeStrArr || df.valueType == pbv1.ValueTypeInt64Arr {
		// For array types, check if the item exists in the pre-computed element set
		if df.valueSet != nil {
			_, exists := df.valueSet[convert.BytesToString(item)]
			return exists
		}
		return false
	}

	// For non-array types, use O(1) map lookup
	if df.valueSet != nil {
		_, exists := df.valueSet[convert.BytesToString(item)]
		return exists
	}
	return false
}

// SetValues sets the dictionary values and builds the lookup set.
func (df *DictionaryFilter) SetValues(values [][]byte) {
	df.values = values
	df.buildValueSet()
}

// SetValueType sets the value type for the dictionary filter.
// For array types, it rebuilds the lookup set by extracting elements from serialized arrays.
func (df *DictionaryFilter) SetValueType(valueType pbv1.ValueType) {
	df.valueType = valueType
	// Rebuild the set for array types since elements need to be extracted
	if valueType == pbv1.ValueTypeStrArr || valueType == pbv1.ValueTypeInt64Arr {
		df.buildValueSet()
	}
}

// Reset resets the dictionary filter.
func (df *DictionaryFilter) Reset() {
	for i := range df.values {
		df.values[i] = nil
	}
	df.values = df.values[:0]
	df.valueType = pbv1.ValueTypeUnknown
	clear(df.valueSet)
}

// buildValueSet builds a map-based lookup set from the dictionary values.
// For non-array types, values are added directly.
// For array types, elements are extracted from serialized arrays.
func (df *DictionaryFilter) buildValueSet() {
	if df.valueSet == nil {
		df.valueSet = make(map[string]struct{}, len(df.values))
	}

	if df.valueType == pbv1.ValueTypeInt64Arr {
		// Extract int64 elements from serialized arrays
		for _, serializedArray := range df.values {
			for i := 0; i+8 <= len(serializedArray); i += 8 {
				df.valueSet[convert.BytesToString(serializedArray[i:i+8])] = struct{}{}
			}
		}
		return
	}

	if df.valueType == pbv1.ValueTypeStrArr {
		// Extract string elements from serialized arrays
		for _, serializedArray := range df.values {
			extractStrings(serializedArray, df.valueSet)
		}
		return
	}

	// For non-array types, add values directly
	for _, v := range df.values {
		df.valueSet[convert.BytesToString(v)] = struct{}{}
	}
}

// extractStrings extracts string elements from a serialized string array and adds them to the set.
func extractStrings(serializedArray []byte, set map[string]struct{}) {
	const (
		entityDelimiter = '|'
		escape          = '\\'
	)

	src := serializedArray
	var buf []byte
	for len(src) > 0 {
		buf = buf[:0]
		if src[0] == entityDelimiter {
			// Empty string element
			set[""] = struct{}{}
			src = src[1:]
			continue
		}
		for len(src) > 0 {
			switch {
			case src[0] == escape:
				if len(src) < 2 {
					return
				}
				src = src[1:]
				buf = append(buf, src[0])
			case src[0] == entityDelimiter:
				src = src[1:]
				set[string(buf)] = struct{}{}
				goto nextElement
			default:
				buf = append(buf, src[0])
			}
			src = src[1:]
		}
		return
	nextElement:
	}
}
