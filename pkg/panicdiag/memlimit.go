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

package panicdiag

import (
	"fmt"
	"runtime/debug"

	"github.com/apache/skywalking-banyandb/pkg/cgroups"
)

// Package-level vars allow tests to override without forking the process.
var (
	cgroupMemLimit = cgroups.MemoryLimit
	setMemoryLimit = debug.SetMemoryLimit
)

// ApplyGoMemLimit sets GOMEMLIMIT to pct percent of the cgroup memory limit,
// reserving the remaining capacity as headroom for diagnostic work after a panic.
// pct is clamped to [1, 99]. Returns the value applied, or 0 when the cgroup
// memory limit is unavailable (non-Linux or no cgroup configured).
func ApplyGoMemLimit(pct int) (int64, error) {
	if pct < 1 {
		pct = 1
	} else if pct > 99 {
		pct = 99
	}
	cgLimit, err := cgroupMemLimit()
	if err != nil {
		// Non-Linux or no cgroup configured — skip without error.
		return 0, nil
	}
	if cgLimit <= 0 {
		return 0, nil
	}
	limit := cgLimit * int64(pct) / 100
	if limit <= 0 {
		return 0, fmt.Errorf("computed GOMEMLIMIT is non-positive: cgLimit=%d pct=%d", cgLimit, pct)
	}
	setMemoryLimit(limit)
	return limit, nil
}
