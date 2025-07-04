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
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Float64ListToDecimalIntList converts float64 values to int64s with a common decimal scale factor.
func Float64ListToDecimalIntList(dst []int64, nums []float64) ([]int64, int16, error) {
	if len(nums) == 0 {
		logger.Panicf("a must contain at least one item")
	}

	// Maximum allowed decimal places for float64 safety
	const maxDecimalPlaces = 15
	var maxPlaces int16

	// Determine the maximum number of decimal places needed
	for _, f := range nums {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, 0, fmt.Errorf("invalid float value: %v", f)
		}
		places := countDecimalPlaces(f, maxDecimalPlaces)
		if places > maxPlaces {
			maxPlaces = places
		}
	}

	scale := math.Pow10(int(maxPlaces))
	for _, f := range nums {
		scaled := f * scale
		rounded := math.Round(scaled)
		if rounded > math.MaxInt64 || rounded < math.MinInt64 {
			// // -1 and err represents an overflow condition
			return nil, -1, fmt.Errorf("value %f overflows int64", rounded)
		}
		dst = append(dst, int64(rounded))
	}

	return dst, -maxPlaces, nil
}

// DecimalIntListToFloat64List restores float64 values from scaled int64s using a decimal exponent.
func DecimalIntListToFloat64List(dst []float64, values []int64, exponent int16, itemsCount int) ([]float64, error) {
	dst = ExtendListCapacity(dst, itemsCount)

	if len(values) == 0 {
		return dst[:0], nil
	}
	scale := math.Pow10(int(-exponent))
	for _, v := range values {
		dst = append(dst, float64(v)/scale)
	}
	return dst, nil
}

// countDecimalPlaces estimates the number of decimal places in a float64 up to a given maximum.
// If you enter 3.1, it may be less than 1e-9 in the computer, so it will be discarded.
// finally, rounded = 31，exp = -1.
// However, if the input is really 3.10000000000000009, this may result in loss of accuracy.
func countDecimalPlaces(f float64, maxPlace int) int16 {
	for i := 0; i < maxPlace; i++ {
		modf, frac := math.Modf(f)
		fmt.Println(modf, frac)
		if math.Abs(f-math.Round(f)) < 1e-9 {
			return int16(i)
		}
		f *= 10
	}
	return int16(maxPlace)
}
