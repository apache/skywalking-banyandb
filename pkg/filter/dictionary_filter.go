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

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// DictionaryFilter is a filter implementation backed by a dictionary.
type DictionaryFilter struct {
	values    [][]byte
	valueType pbv1.ValueType
}

// NewDictionaryFilter creates a new dictionary filter with the given values.
func NewDictionaryFilter(values [][]byte) *DictionaryFilter {
	return &DictionaryFilter{
		values: values,
	}
}

// MightContain checks if an item is in the dictionary.
func (df *DictionaryFilter) MightContain(item []byte) bool {
	if df.valueType == pbv1.ValueTypeStrArr || df.valueType == pbv1.ValueTypeInt64Arr {
		for _, serializedArray := range df.values {
			if containElement(serializedArray, item, df.valueType) {
				return true
			}
		}
		return false
	}

	for _, v := range df.values {
		if bytes.Equal(v, item) {
			return true
		}
	}
	return false
}

// SetValues sets the dictionary values.
func (df *DictionaryFilter) SetValues(values [][]byte) {
	df.values = values
}

// SetValueType sets the value type for the dictionary filter.
func (df *DictionaryFilter) SetValueType(valueType pbv1.ValueType) {
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

func containElement(serializedArray []byte, element []byte, valueType pbv1.ValueType) bool {
	if len(serializedArray) == 0 {
		return false
	}
	if valueType == pbv1.ValueTypeInt64Arr {
		if len(element) != 8 {
			return false
		}
		for i := 0; i < len(serializedArray); i += 8 {
			if i+8 > len(serializedArray) {
				break
			}
			if bytes.Equal(serializedArray[i:i+8], element) {
				return true
			}
		}
		return false
	}
	if valueType == pbv1.ValueTypeStrArr {
		return containString(serializedArray, element)
	}
	return false
}

func containString(serializedArray, element []byte) bool {
	const (
		entityDelimiter = '|'
		escape          = '\\'
	)

	src := serializedArray
	var buf []byte
	for len(src) > 0 {
		buf = buf[:0]
		if len(src) == 0 {
			break
		}
		if src[0] == entityDelimiter {
			if len(element) == 0 {
				return true
			}
			src = src[1:]
			continue
		}
		for len(src) > 0 {
			switch {
			case src[0] == escape:
				if len(src) < 2 {
					return false
				}
				src = src[1:]
				buf = append(buf, src[0])
			case src[0] == entityDelimiter:
				src = src[1:]
				if bytes.Equal(buf, element) {
					return true
				}
				goto nextElement
			default:
				buf = append(buf, src[0])
			}
			src = src[1:]
		}
		return false
	nextElement:
	}
	return false
}
