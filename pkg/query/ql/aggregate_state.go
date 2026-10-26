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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type aggregateState struct {
	count    []int64
	sumInt   []int64
	sumFloat []float64
	minInt   []int64
	maxInt   []int64
	minFloat []float64
	maxFloat []float64
	source   vectorized.ColumnDef
	needs    uint8
}

const (
	aggregateNeedsIntSum uint8 = 1 << iota
	aggregateNeedsFloatSum
	aggregateNeedsMin
	aggregateNeedsMax
)

func (s *aggregateState) require(function AggregateFunction) {
	switch function {
	case AggregateCount:
	case AggregateSum:
		if s.source.Type == vectorized.ColumnTypeInt64 {
			s.needs |= aggregateNeedsIntSum
		} else {
			s.needs |= aggregateNeedsFloatSum
		}
	case AggregateAvg:
		s.needs |= aggregateNeedsFloatSum
	case AggregateMin:
		s.needs |= aggregateNeedsMin
	case AggregateMax:
		s.needs |= aggregateNeedsMax
	}
}

func (s *aggregateState) lanes() int {
	lanes := 1 // Exact non-null count also distinguishes an all-null group.
	for mask := uint8(1); mask <= aggregateNeedsMax; mask <<= 1 {
		if s.needs&mask != 0 {
			lanes++
		}
	}
	return lanes
}

func growAggregateLane[T int64 | float64](old []T, capacity int) []T {
	lane := make([]T, capacity)
	copy(lane, old)
	return lane
}

func (s *aggregateState) grow(capacity int) {
	s.count = growAggregateLane(s.count, capacity)
	if s.needs&aggregateNeedsIntSum != 0 {
		s.sumInt = growAggregateLane(s.sumInt, capacity)
	}
	if s.needs&aggregateNeedsFloatSum != 0 {
		s.sumFloat = growAggregateLane(s.sumFloat, capacity)
	}
	if s.source.Type == vectorized.ColumnTypeInt64 {
		if s.needs&aggregateNeedsMin != 0 {
			s.minInt = growAggregateLane(s.minInt, capacity)
		}
		if s.needs&aggregateNeedsMax != 0 {
			s.maxInt = growAggregateLane(s.maxInt, capacity)
		}
	} else {
		if s.needs&aggregateNeedsMin != 0 {
			s.minFloat = growAggregateLane(s.minFloat, capacity)
		}
		if s.needs&aggregateNeedsMax != 0 {
			s.maxFloat = growAggregateLane(s.maxFloat, capacity)
		}
	}
}

func (s *aggregateState) update(group int, value aggregateValue) error {
	if value.null {
		return nil
	}
	if s.count[group] == math.MaxInt64 {
		return fmt.Errorf("aggregate COUNT(%s) exceeds int64", s.source.Name)
	}
	first := s.count[group] == 0
	s.count[group]++
	if s.needs&aggregateNeedsIntSum != 0 {
		previous := s.sumInt[group]
		if (value.i64 > 0 && previous > math.MaxInt64-value.i64) || (value.i64 < 0 && previous < math.MinInt64-value.i64) {
			return fmt.Errorf("aggregate SUM(%s) exceeds int64", s.source.Name)
		}
		s.sumInt[group] += value.i64
	}
	if s.needs&aggregateNeedsFloatSum != 0 {
		number := value.f64
		if value.typ == vectorized.ColumnTypeInt64 {
			number = float64(value.i64)
		}
		s.sumFloat[group] += number
	}
	if s.source.Type == vectorized.ColumnTypeInt64 {
		if s.needs&aggregateNeedsMin != 0 && (first || value.i64 < s.minInt[group]) {
			s.minInt[group] = value.i64
		}
		if s.needs&aggregateNeedsMax != 0 && (first || value.i64 > s.maxInt[group]) {
			s.maxInt[group] = value.i64
		}
	} else {
		if s.needs&aggregateNeedsMin != 0 && (first || aggregateFloatLess(value.f64, s.minFloat[group])) {
			s.minFloat[group] = value.f64
		}
		if s.needs&aggregateNeedsMax != 0 && (first || aggregateFloatLess(s.maxFloat[group], value.f64)) {
			s.maxFloat[group] = value.f64
		}
	}
	return nil
}

func aggregateFloatLess(left, right float64) bool {
	if math.IsNaN(left) {
		return false
	}
	return math.IsNaN(right) || left < right
}

func (s *aggregateState) result(group int, function AggregateFunction) aggregateValue {
	value := aggregateValue{typ: s.source.Type, null: s.count[group] == 0}
	switch function {
	case AggregateCount:
		value.typ, value.i64, value.null = vectorized.ColumnTypeInt64, s.count[group], false
	case AggregateSum:
		if s.source.Type == vectorized.ColumnTypeInt64 {
			value.i64 = s.sumInt[group]
		} else {
			value.f64 = s.sumFloat[group]
		}
	case AggregateMin:
		if s.source.Type == vectorized.ColumnTypeInt64 {
			value.i64 = s.minInt[group]
		} else {
			value.f64 = s.minFloat[group]
		}
	case AggregateMax:
		if s.source.Type == vectorized.ColumnTypeInt64 {
			value.i64 = s.maxInt[group]
		} else {
			value.f64 = s.maxFloat[group]
		}
	case AggregateAvg:
		value.typ = vectorized.ColumnTypeFloat64
		if !value.null {
			value.f64 = s.sumFloat[group] / float64(s.count[group])
		}
	}
	return value
}
