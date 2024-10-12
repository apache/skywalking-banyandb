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

// Rate calculates the average value of elements.
type Rate[A, B Input, R Output] struct {
	denominator int64
	numerator   int64
}

// Combine takes elements to do the aggregation.
// Rate uses none of type parameters.
func (f *Rate[A, B, R]) Combine(arguments Arguments[A, B]) error {
	i := 0
	n := len(arguments.arg0)
	// step-4 aggregate
	for ; i <= n-4; i += 4 {
		f.denominator += int64(arguments.arg0[i]) + int64(arguments.arg0[i+1]) +
			int64(arguments.arg0[i+2]) + int64(arguments.arg0[i+3])
	}
	// tail aggregate
	for ; i < n; i++ {
		f.denominator += int64(arguments.arg0[i])
	}

	i = 0
	n = len(arguments.arg1)
	// step-4 aggregate
	for ; i <= n-4; i += 4 {
		f.numerator += int64(arguments.arg1[i]) + int64(arguments.arg1[i+1]) +
			int64(arguments.arg1[i+2]) + int64(arguments.arg1[i+3])
	}
	// tail aggregate
	for ; i < n; i++ {
		f.numerator += int64(arguments.arg1[i])
	}

	return nil
}

// Result gives the result for the aggregation.
func (f *Rate[A, B, R]) Result() (A, B, R) {
	var rate R
	if f.denominator != 0 {
		// Factory 10000 is used to improve accuracy. This factory is same as OAP.
		rate = R(f.numerator) * 10000 / R(f.denominator)
	}
	return A(f.denominator), B(f.numerator), rate
}

// NewRateArguments constructs arguments.
func NewRateArguments(a []int64, b []int64) Arguments[int64, int64] {
	return Arguments[int64, int64]{
		arg0: a,
		arg1: b,
	}
}
