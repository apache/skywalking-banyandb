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
//
package timestamp

import (
	"time"
	// link runtime pkg fastrand
	_ "unsafe"
)

var (
	maxNanoSecond = uint32(time.Millisecond - 1)
	mSecond       = int64(time.Millisecond)
)

// FastRandN is a fast thread local random function.
//go:linkname fastRandN runtime.fastrandn
func fastRandN(n uint32) uint32

// MToN convert time unix millisends to nanoseconds
func MToN(ms time.Time) time.Time {
	ns := ms.UnixNano()
	if ms.Nanosecond()%int(mSecond) > 0 {
		ns = ns / mSecond * mSecond
	}
	nns := ns + int64(fastRandN(maxNanoSecond))
	return time.Unix(ms.Unix(), nns%int64(time.Second))
}

// NowMilli returns a time based on a unix millisecond
func NowMilli() time.Time {
	return time.UnixMilli(time.Now().UnixMilli())
}
