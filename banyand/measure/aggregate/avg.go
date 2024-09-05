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

// Avg calculates the average value of elements.
type Avg[A, B Input, R Output] struct {
	summation R
	count     int64
}

// Combine takes elements to do the aggregation.
// Avg uses type parameter A and B.
func (a *Avg[A, B, R]) Combine(arguments Arguments[A, B]) error {
	for _, arg0 := range arguments.arg0 {
		switch arg0 := any(arg0).(type) {
		case int64:
			a.summation += R(arg0)
		case float64:
			a.summation += R(arg0)
		default:
			return errFieldValueType
		}
	}

	for _, arg1 := range arguments.arg1 {
		switch arg1 := any(arg1).(type) {
		case int64:
			a.count += arg1
		default:
			return errFieldValueType
		}
	}

	return nil
}

// Result gives the result for the aggregation.
func (a *Avg[A, B, R]) Result() R {
	// In unusual situations it returns the zero value.
	if a.count == 0 {
		return zeroValue[R]()
	}
	// According to the semantics of GoLang, the division of one int by another int
	// returns an int, instead of a float.
	return a.summation / R(a.count)
}

// NewAvgArguments constructs arguments.
func NewAvgArguments[A Input](a []A, b []int64) Arguments[A, int64] {
	return Arguments[A, int64]{
		arg0: a,
		arg1: b,
	}
}
