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

// Min calculates the minimum value of elements.
type Min[A, B, C MAFInput, K MAFKeep] struct {
	minimum K
}

// Combine takes elements to do the aggregation.
// Min uses type parameter A.
func (m *Min[A, B, C, K]) Combine(arguments MAFArguments[A, B, C]) error {
	for _, arg0 := range arguments.arg0 {
		switch arg0 := any(arg0).(type) {
		case int64:
			if K(arg0) < m.minimum {
				m.minimum = K(arg0)
			}
		case float64:
			if K(arg0) < m.minimum {
				m.minimum = K(arg0)
			}
		case []int64:
			for _, v := range arg0 {
				if K(v) < m.minimum {
					m.minimum = K(v)
				}
			}
		default:
			return errFieldValueType
		}
	}
	return nil
}

// Result gives the result for the aggregation.
func (m *Min[A, B, C, K]) Result() K {
	return m.minimum
}
