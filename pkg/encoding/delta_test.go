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
	"reflect"
	"testing"
)

func TestInt64ListDeltaToBytesAndBytesDeltaToInt64List(t *testing.T) {
	testCases := []struct {
		name             string
		inputSrc         []int64
		expectedResult   []int64
		expectedFirstVal int64
	}{
		{
			name:             "Basic Test Case",
			inputSrc:         []int64{10, 20, 30, 40, 50},
			expectedResult:   []int64{10, 20, 30, 40, 50},
			expectedFirstVal: 10,
		},
		{
			name:             "Negative Numbers",
			inputSrc:         []int64{-10, -5, 0, 5, 10},
			expectedResult:   []int64{-10, -5, 0, 5, 10},
			expectedFirstVal: -10,
		},
		{
			name:             "Large Numbers",
			inputSrc:         []int64{1000000, 2000000, 3000000, 4000000, 5000000},
			expectedResult:   []int64{1000000, 2000000, 3000000, 4000000, 5000000},
			expectedFirstVal: 1000000,
		},
		{
			name:             "Repeated Numbers",
			inputSrc:         []int64{5, 5, 5, 5, 5},
			expectedResult:   []int64{5, 5, 5, 5, 5},
			expectedFirstVal: 5,
		},
		{
			name:             "Descending Numbers",
			inputSrc:         []int64{10, 8, 6, 4, 2},
			expectedResult:   []int64{10, 8, 6, 4, 2},
			expectedFirstVal: 10,
		},
		{
			name:             "Ascending Numbers",
			inputSrc:         []int64{1, 2, 3, 4, 5},
			expectedResult:   []int64{1, 2, 3, 4, 5},
			expectedFirstVal: 1,
		},
		{
			name:             "Mixed Numbers",
			inputSrc:         []int64{-3, -2, 0, 2, 3},
			expectedResult:   []int64{-3, -2, 0, 2, 3},
			expectedFirstVal: -3,
		},
		{
			name:             "Large Random Numbers",
			inputSrc:         []int64{987654321, 123456789, 987654321, 987654321, 123456789},
			expectedResult:   []int64{987654321, 123456789, 987654321, 987654321, 123456789},
			expectedFirstVal: 987654321,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test int64ListDeltaToBytes
			dst := make([]byte, 0)
			resultBytes, firstValue := int64ListDeltaToBytes(dst, tc.inputSrc)
			if firstValue != tc.expectedFirstVal {
				t.Errorf("Expected first value %d, got %d", tc.expectedFirstVal, firstValue)
			}

			// Test bytesDeltaToInt64List
			dstInt64 := make([]int64, 0)
			itemsCount := len(tc.inputSrc)
			resultInt64, err := bytesDeltaToInt64List(dstInt64, resultBytes, firstValue, itemsCount)
			if err != nil {
				t.Errorf("Error decoding bytes: %v", err)
			}
			if !reflect.DeepEqual(tc.inputSrc, resultInt64) {
				t.Errorf("Expected decoded result %v, got %v", tc.inputSrc, resultInt64)
			}
		})
	}
}

func TestInt64sDeltaOfDeltaToBytesAndBytesDeltaOfDeltaToInt64s(t *testing.T) {
	testCases := []struct {
		name             string
		inputSrc         []int64
		expectedResult   []int64
		expectedFirstVal int64
	}{
		{
			name:             "Basic Test Case",
			inputSrc:         []int64{10, 20, 30, 40, 50},
			expectedResult:   []int64{10, 20, 30, 40, 50},
			expectedFirstVal: 10,
		},
		{
			name:             "Negative Numbers",
			inputSrc:         []int64{-10, -5, 0, 5, 10},
			expectedResult:   []int64{-10, -5, 0, 5, 10},
			expectedFirstVal: -10,
		},
		{
			name:             "Large Numbers",
			inputSrc:         []int64{1000000, 2000000, 3000000, 4000000, 5000000},
			expectedResult:   []int64{1000000, 2000000, 3000000, 4000000, 5000000},
			expectedFirstVal: 1000000,
		},
		{
			name:             "Repeated Numbers",
			inputSrc:         []int64{5, 5, 5, 5, 5},
			expectedResult:   []int64{5, 5, 5, 5, 5},
			expectedFirstVal: 5,
		},
		{
			name:             "Descending Numbers",
			inputSrc:         []int64{10, 8, 6, 4, 2},
			expectedResult:   []int64{10, 8, 6, 4, 2},
			expectedFirstVal: 10,
		},
		{
			name:             "Ascending Numbers",
			inputSrc:         []int64{1, 2, 3, 4, 5},
			expectedResult:   []int64{1, 2, 3, 4, 5},
			expectedFirstVal: 1,
		},
		{
			name:             "Mixed Numbers",
			inputSrc:         []int64{-3, -2, 0, 2, 3},
			expectedResult:   []int64{-3, -2, 0, 2, 3},
			expectedFirstVal: -3,
		},
		{
			name:             "Large Random Numbers",
			inputSrc:         []int64{987654321, 123456789, 987654321, 987654321, 123456789},
			expectedResult:   []int64{987654321, 123456789, 987654321, 987654321, 123456789},
			expectedFirstVal: 987654321,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test int64sDeltaOfDeltaToBytes
			dst := make([]byte, 0)
			resultBytes, firstValue := int64sDeltaOfDeltaToBytes(dst, tc.inputSrc)
			if firstValue != tc.expectedFirstVal {
				t.Errorf("Expected first value %d, got %d", tc.expectedFirstVal, firstValue)
			}

			// Test bytesDeltaOfDeltaToInt64s
			dstInt64 := make([]int64, 0)
			itemsCount := len(tc.inputSrc)
			resultInt64, err := bytesDeltaOfDeltaToInt64s(dstInt64, resultBytes, firstValue, itemsCount)
			if err != nil {
				t.Errorf("Error decoding bytes: %v", err)
			}
			if !reflect.DeepEqual(tc.inputSrc, resultInt64) {
				t.Errorf("Expected decoded result %v, got %v", tc.inputSrc, resultInt64)
			}
		})
	}
}
