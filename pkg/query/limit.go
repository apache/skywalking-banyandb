// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package query

import "math"

// AddWindow adds a result limit and offset without uint32 overflow.
func AddWindow(limit, offset uint32) (uint64, bool) {
	window := uint64(limit) + uint64(offset)
	return window, window <= uint64(^uint32(0))
}

// IsUnboundedLimit reports historical list-all / max-limit sentinels with no
// offset. OAP uses Integer.MAX_VALUE (MaxInt32); some fixtures use MaxUint32.
// Callers should admit these as scans and bound retained results incrementally
// instead of pre-reserving the window.
func IsUnboundedLimit(limit, offset uint32) bool {
	return offset == 0 && (limit == math.MaxUint32 || limit == math.MaxInt32)
}
