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
)

// DictionaryFilter is a filter implementation backed by a dictionary.
type DictionaryFilter struct {
	values [][]byte
}

// NewDictionaryFilter creates a new dictionary filter with the given values.
func NewDictionaryFilter(values [][]byte) *DictionaryFilter {
	return &DictionaryFilter{
		values: values,
	}
}

// MightContain checks if an item is in the dictionary.
func (df *DictionaryFilter) MightContain(item []byte) bool {
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

// Reset resets the dictionary filter.
func (df *DictionaryFilter) Reset() {
	for i := range df.values {
		df.values[i] = nil
	}
	df.values = df.values[:0]
}
