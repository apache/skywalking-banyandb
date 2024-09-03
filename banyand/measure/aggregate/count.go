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

package aggregate

// Count calculates the count value of elements.
type Count[A, B Input, R Output] struct {
	count int64
}

// Combine takes elements to do the aggregation.
// Count uses type parameter A.
func (m *Count[A, B, R]) Combine(arguments Arguments[A, B]) error {
	for _, arg0 := range arguments.arg0 {
		switch arg0 := any(arg0).(type) {
		case int64:
			m.count += arg0
		default:
			return errFieldValueType
		}
	}
	return nil
}

// Result gives the result for the aggregation.
func (m *Count[A, B, R]) Result() R {
	return R(m.count)
}

// NewCountArguments constructs arguments.
func NewCountArguments(a []int64) Arguments[int64, Void] {
	return Arguments[int64, Void]{
		arg0: a,
		arg1: nil,
	}
}
