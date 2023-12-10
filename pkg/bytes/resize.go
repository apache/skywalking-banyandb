// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bytes

import "math/bits"

func ResizeOver(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	nNew := roundToNearestPow2(n)
	bNew := make([]byte, nNew)
	return bNew[:n]
}

func ResizeExact(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	return make([]byte, n)
}

func roundToNearestPow2(n int) int {
	pow2 := uint8(bits.Len(uint(n - 1)))
	return 1 << pow2
}
