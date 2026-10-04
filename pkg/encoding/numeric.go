// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package encoding contains sortable numeric encodings shared by index paths.
package encoding

import "math"

// Float64ToSortableInt64 converts an IEEE-754 float to the signed sortable representation used by indexes.
func Float64ToSortableInt64(value float64) int64 {
	bits := int64(math.Float64bits(value))
	if bits < 0 {
		bits ^= 0x7fffffffffffffff
	}
	return bits
}

// SortableInt64ToFloat64 reverses Float64ToSortableInt64.
func SortableInt64ToFloat64(value int64) float64 {
	if value < 0 {
		value ^= 0x7fffffffffffffff
	}
	return math.Float64frombits(uint64(value))
}
