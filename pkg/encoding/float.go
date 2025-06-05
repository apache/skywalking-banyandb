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
	"math"
)

// Float64ToUint64 converts float64 to uint64 representation
func Float64ToUint64(f float64) uint64 {
	return math.Float64bits(f)
}

// Uint64ToFloat64 converts uint64 representation back to float64
func Uint64ToFloat64(u uint64) float64 {
	return math.Float64frombits(u)
}

// Float64ToBytes converts float64 to byte array
func Float64ToBytes(dst []byte, f float64) []byte {
	u := Float64ToUint64(f)
	return Uint64ToBytes(dst, u)
}

// BytesToFloat64 converts byte array to float64
func BytesToFloat64(b []byte) float64 {
	u := BytesToUint64(b)
	return Uint64ToFloat64(u)
}
