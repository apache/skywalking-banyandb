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

// Sum calculates the summation value of elements.
type Sum[A, B Input, R Output] struct {
	summation R
}

// Combine takes elements to do the aggregation.
// Sum uses type parameter A.
func (f *Sum[A, B, R]) Combine(arguments Arguments[A, B]) error {
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

	return nil
}

// Result gives the result for the aggregation.
func (f *Sum[A, B, R]) Result() (A, B, R) {
	return A(f.summation), zeroValue[B](), f.summation
}

// NewSumArguments constructs arguments.
func NewSumArguments[A Input](a []A) Arguments[A, Void] {
	return Arguments[A, Void]{
		arg0: a,
		arg1: nil,
	}
}
