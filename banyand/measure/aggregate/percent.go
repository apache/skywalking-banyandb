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

// Percent calculates the average value of elements.
type Percent[A, B Input, R Output] struct {
	total int64
	match int64
}

// Combine takes elements to do the aggregation.
// Percent uses none of type parameters.
func (f *Percent[A, B, R]) Combine(arguments Arguments[A, B]) error {
	for _, arg0 := range arguments.arg0 {
		switch arg0 := any(arg0).(type) {
		case int64:
			f.total += arg0
		default:
			return errFieldValueType
		}
	}

	for _, arg1 := range arguments.arg1 {
		switch arg1 := any(arg1).(type) {
		case int64:
			f.match += arg1
		default:
			return errFieldValueType
		}
	}

	return nil
}

// Result gives the result for the aggregation.
func (f *Percent[A, B, R]) Result() R {
	// In unusual situations it returns the zero value.
	if f.total == 0 {
		return zeroValue[R]()
	}
	// Factory 10000 is used to improve accuracy. This factory is same as OAP.
	// For example, "10 percent" will return 1000.
	return R(f.match) * 10000 / R(f.total)
}

// NewPercentArguments constructs arguments.
func NewPercentArguments(a []int64, b []int64) Arguments[int64, int64] {
	return Arguments[int64, int64]{
		arg0: a,
		arg1: b,
	}
}
