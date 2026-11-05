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

package transfer

import (
	"context"
	"slices"
	"time"
)

// DefaultStageName is the name the CLI and manifest give the engine's unnamed default tier.
const DefaultStageName = "hot"

// NormalizeStage maps the engine's empty default tier to DefaultStageName.
func NormalizeStage(stage string) string {
	if stage == "" {
		return DefaultStageName
	}
	return stage
}

// UniqueSorted returns the distinct values in ascending order, or nil for no values.
func UniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := slices.Clone(values)
	slices.Sort(out)
	return slices.Compact(out)
}

// MergeRange widens [minDst, maxDst] by a source range. A source without a minimum (0) has no
// flushed timestamps and is skipped, so a 0 minDst always means no source was merged yet and
// the result does not depend on the merge order.
func MergeRange(minDst, maxDst *int64, minSrc, maxSrc int64) {
	if minSrc == 0 {
		return
	}
	if *minDst == 0 || minSrc < *minDst {
		*minDst = minSrc
	}
	if maxSrc > *maxDst {
		*maxDst = maxSrc
	}
}

// SleepCtx waits for d, returning nil when it elapses or context.Cause(ctx) when ctx ends first.
func SleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}
