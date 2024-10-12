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
// Avg uses type parameter A.
func (f *Avg[A, B, R]) Combine(arguments Arguments[A, B]) error {
	i := 0
	n := len(arguments.arg0)
	// step-4 aggregate
	for ; i <= n-4; i += 4 {
		f.summation += R(arguments.arg0[i]) + R(arguments.arg0[i+1]) +
			R(arguments.arg0[i+2]) + R(arguments.arg0[i+3])
	}
	// tail aggregate
	for ; i < n; i++ {
		f.summation += R(arguments.arg0[i])
	}

	i = 0
	n = len(arguments.arg1)
	// step-4 aggregate
	for ; i <= n-4; i += 4 {
		f.count += int64(arguments.arg1[i]) + int64(arguments.arg1[i+1]) +
			int64(arguments.arg1[i+2]) + int64(arguments.arg1[i+3])
	}
	// tail aggregate
	for ; i < n; i++ {
		f.count += int64(arguments.arg1[i])
	}

	return nil
}

// Result gives the result for the aggregation.
func (f *Avg[A, B, R]) Result() (A, B, R) {
	var average R
	if f.count != 0 {
		// According to the semantics of GoLang, the division of one int by another int
		// returns an int, instead of f float.
		average = f.summation / R(f.count)
	}
	return A(f.summation), B(f.count), average
}

// NewAvgArguments constructs arguments.
func NewAvgArguments[A Input](a []A, b []int64) Arguments[A, int64] {
	return Arguments[A, int64]{
		arg0: a,
		arg1: b,
	}
}
