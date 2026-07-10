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

package sdktest

import "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"

// ChainBypass pairs a bypassed link's index (position in the samplers slice
// passed to RunChain) with the reason it was bypassed.
type ChainBypass struct {
	sdk.BypassInfo
	// Idx is the bypassed link's index in the samplers slice.
	Idx int
}

// ChainReport records every link RunChain's underlying sdk.EvaluateChain
// bypassed, in evaluation order.
type ChainReport struct {
	Bypassed []ChainBypass
}

// RunChain evaluates samplers over batch via the shared sdk.EvaluateChain —
// the exact AND-aggregation + per-link panic/error/length-mismatch fail-open
// logic the real engine's merge chain runs — recording every bypassed link
// into the returned ChainReport instead of logging it. This makes the chain
// harness golden-file friendly and gives a plugin author the same
// "why was this link skipped" visibility the host's WARN log carries, without
// a cluster.
func RunChain(samplers []sdk.Sampler, batch *sdk.TraceBatch) (sdk.Verdict, ChainReport) {
	var report ChainReport
	verdict := sdk.EvaluateChain(samplers, batch, func(idx int, info sdk.BypassInfo) {
		report.Bypassed = append(report.Bypassed, ChainBypass{Idx: idx, BypassInfo: info})
	})
	return verdict, report
}
