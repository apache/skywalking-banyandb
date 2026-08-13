// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type traceFragmentGuardContractFilter struct {
	filterErr  error
	membership traceFragmentMembership
	probes     int
}

func (f *traceFragmentGuardContractFilter) Lookup(_ string) (traceFragmentMembership, error) {
	f.probes++
	return f.membership, f.filterErr
}

type traceFragmentGuardContractPin struct {
	releases int
}

func (p *traceFragmentGuardContractPin) Release() {
	p.releases++
}

type traceFragmentGuardResolveInput struct {
	trace         traceFragmentGuardTrace
	catalog       traceFragmentGuardCatalog
	samplerAction traceFragmentSamplerAction
}

func TestTraceFragmentGuardResolveContract(t *testing.T) {
	filterReadErr := errors.New("filter read failed")
	negativeGraceConfig := contractGuardConfig()
	negativeGraceConfig.Grace = -time.Nanosecond
	negativeBudgetConfig := contractGuardConfig()
	negativeBudgetConfig.MaxBloomProbes = -1
	insufficientGraceConfig := contractGuardConfig()
	insufficientGraceConfig.Grace = 9 * time.Nanosecond
	zeroGraceConfig := contractGuardConfig()
	zeroGraceConfig.Grace = 0
	zeroProbeConfig := contractGuardConfig()
	zeroProbeConfig.MaxBloomProbes = 0
	oneProbeConfig := contractGuardConfig()
	oneProbeConfig.MaxBloomProbes = 1
	twoProbeConfig := contractGuardConfig()
	twoProbeConfig.MaxBloomProbes = 2

	testCases := []struct {
		config   *traceFragmentGuardConfig
		name     string
		input    traceFragmentGuardResolveInput
		want     traceFragmentGuardDecision
		canceled bool
	}{
		{
			name:   "sampler keep bypasses every guard check",
			config: &negativeGraceConfig,
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(contractPart(1, 100, 110, contractFilter(traceFragmentMembershipMaybePresent, nil)))
				input.samplerAction = traceFragmentSamplerActionKeep
				input.trace.Complete = false
				input.catalog.Pin = nil
				input.catalog.Complete = false
				input.catalog.CoverageKnown = false
				input.catalog.TemporalSafety = traceFragmentTemporalSafetyUnknown
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionKeep, traceFragmentGuardReasonSamplerKeep, 42, 0, 0),
		},
		{
			name: "unknown sampler action defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.samplerAction = traceFragmentSamplerActionUnknown
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSamplerActionInvalid, 42, 0, 0),
		},
		{
			name:   "negative grace defers",
			config: &negativeGraceConfig,
			input:  contractDropInput(),
			want:   contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonConfigInvalid, 42, 0, 0),
		},
		{
			name:   "negative probe budget defers",
			config: &negativeBudgetConfig,
			input:  contractDropInput(),
			want:   contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonConfigInvalid, 42, 0, 0),
		},
		{
			name: "missing temporal safety proof defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.catalog.TemporalSafety = traceFragmentTemporalSafetyUnknown
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTemporalSafetyUnknown, 42, 0, 0),
		},
		{
			name:   "grace below the catalog enforced gap defers",
			config: &insufficientGraceConfig,
			input:  contractDropInput(),
			want:   contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTemporalSafetyUnknown, 42, 0, 0),
		},
		{
			name: "negative catalog enforced gap defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.catalog.EnforcedMaxFragmentGap = -time.Nanosecond
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTemporalSafetyUnknown, 42, 0, 0),
		},
		{
			name: "unpinned catalog defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.catalog.Pin = nil
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCatalogUnpinned, 42, 0, 0),
		},
		{
			name: "drop with no time candidate",
			input: contractDropInput(
				contractPart(1, 50, 80, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				contractPart(2, 131, 150, contractFilter(traceFragmentMembershipMaybePresent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonNoCandidate, 42, 0, 0),
		},
		{
			name: "earlier positive at the inclusive grace boundary defers",
			input: contractDropInput(
				contractPart(1, 80, 90, contractFilter(traceFragmentMembershipMaybePresent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "later positive at the inclusive grace boundary defers",
			input: contractDropInput(
				contractPart(1, 120, 130, contractFilter(traceFragmentMembershipMaybePresent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name:   "zero grace still includes a touching part",
			config: &zeroGraceConfig,
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(1, 110, 120, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				)
				input.catalog.EnforcedMaxFragmentGap = 0
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name:   "exact probe budget with all filters absent permits drop",
			config: &twoProbeConfig,
			input: contractDropInput(
				contractPart(1, 95, 105, contractFilter(traceFragmentMembershipAbsent, nil)),
				contractPart(2, 110, 120, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonAllCandidatesNegative, 42, 2, 2),
		},
		{
			name: "all selected blocks contribute to the guard range",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(1, 150, 160, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				)
				input.trace.Blocks = contractMultiBlockRange()
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "confirmed drop carries the canonical multi-block range",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(1, 150, 160, contractFilter(traceFragmentMembershipAbsent, nil)),
				)
				input.trace.Blocks = contractMultiBlockRange()
				return input
			}(),
			want: contractDropDecision(traceFragmentGuardReasonAllCandidatesNegative, 42, 1, 1, "trace-a", 100, 210),
		},
		{
			name: "nested non-adjacent candidate is not skipped",
			input: contractDropInput(
				contractPart(1, 0, 0, contractFilter(traceFragmentMembershipAbsent, nil)),
				contractPart(2, 50, 500, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				contractPart(3, 80, 89, contractFilter(traceFragmentMembershipAbsent, nil)),
				contractPart(4, 121, 130, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "first positive stops later candidate probes",
			input: contractDropInput(
				contractPart(1, 90, 100, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				contractPart(2, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
				contractPart(3, 110, 120, contractFilter(traceFragmentMembershipAbsent, filterReadErr)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 3, 1),
		},
		{
			name: "incomplete selected trace defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.trace.Complete = false
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceIncomplete, 42, 0, 0),
		},
		{
			name: "empty trace id defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.trace.TraceID = ""
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceIncomplete, 42, 0, 0),
		},
		{
			name: "selected trace without blocks defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.trace.Blocks = nil
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceBoundsInvalid, 42, 0, 0),
		},
		{
			name: "unknown selected block bounds defer",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.trace.Blocks[0].BoundsKnown = false
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceBoundsInvalid, 42, 0, 0),
		},
		{
			name: "reversed selected block bounds defer",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.trace.Blocks[0] = traceFragmentGuardBlock{
					MinTimestamp: 110,
					MaxTimestamp: 100,
					BoundsKnown:  true,
				}
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceBoundsInvalid, 42, 0, 0),
		},
		{
			name: "incomplete outside catalog defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.catalog.Complete = false
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCatalogIncomplete, 42, 0, 0),
		},
		{
			name: "unknown catalog coverage defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput()
				input.catalog.CoverageKnown = false
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSegmentBoundary, 42, 0, 0),
		},
		{
			name: "guard range outside catalog coverage defers",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
				)
				input.catalog.CoverageMinTimestamp = 91
				input.catalog.CoverageMaxTimestamp = math.MaxInt64
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSegmentBoundary, 42, 0, 0),
		},
		{
			name: "unknown outside part bounds defer",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
				)
				input.catalog.OutsideParts[0].BoundsKnown = false
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonPartBoundsInvalid, 42, 0, 0),
		},
		{
			name: "reversed outside part bounds defer",
			input: contractDropInput(
				contractPart(1, 120, 100, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonPartBoundsInvalid, 42, 0, 0),
		},
		{
			name: "missing candidate filter defers and stops",
			input: contractDropInput(
				contractPart(1, 100, 110, nil),
				contractPart(2, 110, 120, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterUnavailable, 42, 2, 0),
		},
		{
			name: "unknown candidate membership defers",
			input: contractDropInput(
				contractPart(1, 100, 110, contractFilter(traceFragmentMembershipUnknown, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterUnavailable, 42, 1, 1),
		},
		{
			name: "candidate filter error defers",
			input: contractDropInput(
				contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, filterReadErr)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterError, 42, 1, 1),
		},
		{
			name: "missing non-candidate filter is not consulted",
			input: contractDropInput(
				contractPart(1, 50, 80, nil),
			),
			want: contractDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonNoCandidate, 42, 0, 0),
		},
		{
			name:   "probe budget exhaustion defers",
			config: &oneProbeConfig,
			input: contractDropInput(
				contractPart(1, 95, 105, contractFilter(traceFragmentMembershipAbsent, nil)),
				contractPart(2, 110, 120, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonBudgetExhausted, 42, 2, 1),
		},
		{
			name:   "zero probe budget defers before lookup",
			config: &zeroProbeConfig,
			input: contractDropInput(
				contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
			),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonBudgetExhausted, 42, 1, 0),
		},
		{
			name:     "canceled evaluation defers",
			input:    contractDropInput(),
			want:     contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCanceled, 42, 0, 0),
			canceled: true,
		},
		{
			name: "minimum timestamp expansion saturates",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(math.MaxUint64, math.MinInt64, math.MinInt64, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				)
				input.trace.Blocks[0] = traceFragmentGuardBlock{
					MinTimestamp: math.MinInt64 + 1,
					MaxTimestamp: math.MinInt64 + 2,
					BoundsKnown:  true,
				}
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "maximum timestamp expansion saturates",
			input: func() traceFragmentGuardResolveInput {
				input := contractDropInput(
					contractPart(math.MaxUint64, math.MaxInt64, math.MaxInt64, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				)
				input.trace.Blocks[0] = traceFragmentGuardBlock{
					MinTimestamp: math.MaxInt64 - 2,
					MaxTimestamp: math.MaxInt64 - 1,
					BoundsKnown:  true,
				}
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			config := contractGuardConfig()
			if testCase.config != nil {
				config = *testCase.config
			}
			guard := newTraceFragmentGuard(config, testCase.input.catalog)
			defer guard.Close()

			testContext := context.Background()
			if testCase.canceled {
				canceledContext, cancel := context.WithCancel(testContext)
				cancel()
				testContext = canceledContext
			}

			got := guard.Resolve(testContext, testCase.input.trace, testCase.input.samplerAction)

			assert.Equal(t, testCase.want, got)
			assert.Equal(t, testCase.want.BloomProbes, contractProbeCount(testCase.input.catalog.OutsideParts))
		})
	}
}

func TestTraceFragmentGuardTwoHourGraceContract(t *testing.T) {
	config := contractGuardConfig()
	config.Grace = defaultTracePipelineMergeGrace
	const traceMinTimestamp = int64(10 * time.Hour)
	const traceMaxTimestamp = int64(10*time.Hour + time.Minute)
	guardMinTimestamp := traceMinTimestamp - int64(config.Grace)
	guardMaxTimestamp := traceMaxTimestamp + int64(config.Grace)

	newInput := func(outsideParts ...traceFragmentGuardPart) traceFragmentGuardResolveInput {
		input := contractDropInput(outsideParts...)
		input.trace.Blocks[0] = traceFragmentGuardBlock{
			MinTimestamp: traceMinTimestamp,
			MaxTimestamp: traceMaxTimestamp,
			BoundsKnown:  true,
		}
		input.catalog.EnforcedMaxFragmentGap = config.Grace
		input.catalog.CoverageMinTimestamp = guardMinTimestamp
		input.catalog.CoverageMaxTimestamp = guardMaxTimestamp
		return input
	}

	testCases := []struct {
		name  string
		input traceFragmentGuardResolveInput
		want  traceFragmentGuardDecision
	}{
		{
			name: "earlier exact boundary is a candidate",
			input: newInput(contractPart(
				1,
				guardMinTimestamp-time.Minute.Nanoseconds(),
				guardMinTimestamp,
				contractFilter(traceFragmentMembershipMaybePresent, nil),
			)),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "earlier boundary minus one nanosecond is excluded",
			input: newInput(contractPart(
				1,
				guardMinTimestamp-time.Minute.Nanoseconds(),
				guardMinTimestamp-1,
				contractFilter(traceFragmentMembershipMaybePresent, nil),
			)),
			want: contractDropDecision(
				traceFragmentGuardReasonNoCandidate,
				42,
				0,
				0,
				"trace-a",
				traceMinTimestamp,
				traceMaxTimestamp,
			),
		},
		{
			name: "later exact boundary is a candidate",
			input: newInput(contractPart(
				1,
				guardMaxTimestamp,
				guardMaxTimestamp+time.Minute.Nanoseconds(),
				contractFilter(traceFragmentMembershipMaybePresent, nil),
			)),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, 42, 1, 1),
		},
		{
			name: "later boundary plus one nanosecond is excluded",
			input: newInput(contractPart(
				1,
				guardMaxTimestamp+1,
				guardMaxTimestamp+time.Minute.Nanoseconds(),
				contractFilter(traceFragmentMembershipMaybePresent, nil),
			)),
			want: contractDropDecision(
				traceFragmentGuardReasonNoCandidate,
				42,
				0,
				0,
				"trace-a",
				traceMinTimestamp,
				traceMaxTimestamp,
			),
		},
		{
			name: "two hour plus one nanosecond proof is insufficient",
			input: func() traceFragmentGuardResolveInput {
				input := newInput()
				input.catalog.EnforcedMaxFragmentGap = config.Grace + time.Nanosecond
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTemporalSafetyUnknown, 42, 0, 0),
		},
		{
			name:  "coverage exactly at both guard boundaries is sufficient",
			input: newInput(),
			want: contractDropDecision(
				traceFragmentGuardReasonNoCandidate,
				42,
				0,
				0,
				"trace-a",
				traceMinTimestamp,
				traceMaxTimestamp,
			),
		},
		{
			name: "coverage one nanosecond inside the earlier boundary defers",
			input: func() traceFragmentGuardResolveInput {
				input := newInput()
				input.catalog.CoverageMinTimestamp = guardMinTimestamp + 1
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSegmentBoundary, 42, 0, 0),
		},
		{
			name: "coverage one nanosecond inside the later boundary defers",
			input: func() traceFragmentGuardResolveInput {
				input := newInput()
				input.catalog.CoverageMaxTimestamp = guardMaxTimestamp - 1
				return input
			}(),
			want: contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSegmentBoundary, 42, 0, 0),
		},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			guard := newTraceFragmentGuard(config, testCase.input.catalog)
			defer guard.Close()

			got := guard.Resolve(context.Background(), testCase.input.trace, testCase.input.samplerAction)

			assert.Equal(t, testCase.want, got)
		})
	}
}

func TestTraceFragmentGuardProbeBudgetIsPerMerge(t *testing.T) {
	config := contractGuardConfig()
	config.MaxBloomProbes = 1
	firstFilter := contractFilter(traceFragmentMembershipAbsent, nil)
	secondFilter := contractFilter(traceFragmentMembershipAbsent, nil)
	catalog := contractCatalog(
		contractPart(1, 100, 110, firstFilter),
		contractPart(2, 200, 210, secondFilter),
	)
	guard := newTraceFragmentGuard(config, catalog)
	defer guard.Close()

	firstTrace := contractDropInput().trace
	firstDecision := guard.Resolve(context.Background(), firstTrace, traceFragmentSamplerActionDrop)
	require.Equal(t,
		contractDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonAllCandidatesNegative, 42, 1, 1),
		firstDecision,
	)

	secondTrace := firstTrace
	secondTrace.TraceID = "trace-b"
	secondTrace.Blocks = []traceFragmentGuardBlock{
		{MinTimestamp: 200, MaxTimestamp: 210, BoundsKnown: true},
	}
	secondDecision := guard.Resolve(context.Background(), secondTrace, traceFragmentSamplerActionDrop)
	require.Equal(t,
		contractDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonBudgetExhausted, 42, 1, 0),
		secondDecision,
	)
	assert.Equal(t, 1, firstFilter.probes)
	assert.Zero(t, secondFilter.probes)
}

func TestTraceFragmentGuardResolveToRevalidationUsesCanonicalRange(t *testing.T) {
	catalog := contractCatalog()
	guard := newTraceFragmentGuard(contractGuardConfig(), catalog)
	defer guard.Close()
	traceData := contractDropInput().trace
	traceData.Blocks = contractMultiBlockRange()

	decision := guard.Resolve(context.Background(), traceData, traceFragmentSamplerActionDrop)
	require.Equal(t,
		contractDropDecision(traceFragmentGuardReasonNoCandidate, 42, 0, 0, "trace-a", 100, 210),
		decision,
	)

	deltaFilter := contractFilter(traceFragmentMembershipMaybePresent, nil)
	request := contractRevalidationRequest(
		43,
		[]traceFragmentGuardPart{
			contractPart(1, 220, 220, deltaFilter),
		},
	)
	revalidation := guard.RevalidateDrops(context.Background(), request)

	assert.Equal(t,
		contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		revalidation,
	)
	assert.Equal(t, 1, deltaFilter.probes)
}

func TestTraceFragmentGuardRevalidationSharesProbeBudget(t *testing.T) {
	config := contractGuardConfig()
	config.MaxBloomProbes = 1
	baseFilter := contractFilter(traceFragmentMembershipAbsent, nil)
	catalog := contractCatalog(contractPart(1, 100, 110, baseFilter))
	guard := newTraceFragmentGuard(config, catalog)
	defer guard.Close()

	decision := guard.Resolve(context.Background(), contractDropInput().trace, traceFragmentSamplerActionDrop)
	require.Equal(t,
		contractDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonAllCandidatesNegative, 42, 1, 1),
		decision,
	)

	deltaFilter := contractFilter(traceFragmentMembershipAbsent, nil)
	revalidation := guard.RevalidateDrops(
		context.Background(),
		contractRevalidationRequest(
			43,
			[]traceFragmentGuardPart{
				contractPart(2, 100, 110, deltaFilter),
			},
		),
	)

	assert.Equal(t,
		contractRevalidation(false, traceFragmentGuardReasonBudgetExhausted, 43, 1, 0),
		revalidation,
	)
	assert.Equal(t, 1, baseFilter.probes)
	assert.Zero(t, deltaFilter.probes)
}

func TestTraceFragmentGuardCatalogPinReleasedOnce(t *testing.T) {
	catalog := contractCatalog()
	catalogPin := catalog.Pin.(*traceFragmentGuardContractPin)
	guard := newTraceFragmentGuard(contractGuardConfig(), catalog)

	guard.Close()
	guard.Close()

	assert.Equal(t, 1, catalogPin.releases)
}

func TestTraceFragmentGuardRevalidateContract(t *testing.T) {
	filterReadErr := errors.New("filter read failed")
	negativeGraceConfig := contractGuardConfig()
	negativeGraceConfig.Grace = -time.Nanosecond
	oneProbeConfig := contractGuardConfig()
	oneProbeConfig.MaxBloomProbes = 1

	testCases := []struct {
		config         *traceFragmentGuardConfig
		name           string
		catalog        traceFragmentGuardCatalog
		request        traceFragmentGuardRevalidationRequest
		confirmedDrops []traceFragmentGuardConfirmedDrop
		want           traceFragmentGuardRevalidation
		canceled       bool
	}{
		{
			name:    "unchanged epoch publishes without probes",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(42, nil),
			want:    contractRevalidation(true, traceFragmentGuardReasonSnapshotUnchanged, 42, 0, 0),
		},
		{
			name:    "invalid guard config rejects publication",
			config:  &negativeGraceConfig,
			catalog: contractCatalog(),
			request: contractRevalidationRequest(42, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonConfigInvalid, 42, 0, 0),
		},
		{
			name: "missing base temporal proof rejects publication",
			catalog: func() traceFragmentGuardCatalog {
				catalog := contractCatalog()
				catalog.TemporalSafety = traceFragmentTemporalSafetyUnknown
				return catalog
			}(),
			request: contractRevalidationRequest(42, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonTemporalSafetyUnknown, 42, 0, 0),
		},
		{
			name: "unpinned base catalog rejects publication",
			catalog: func() traceFragmentGuardCatalog {
				catalog := contractCatalog()
				catalog.Pin = nil
				return catalog
			}(),
			request: contractRevalidationRequest(42, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonCatalogUnpinned, 42, 0, 0),
		},
		{
			name: "incomplete base catalog rejects publication",
			catalog: func() traceFragmentGuardCatalog {
				catalog := contractCatalog()
				catalog.Complete = false
				return catalog
			}(),
			request: contractRevalidationRequest(42, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonCatalogIncomplete, 42, 0, 0),
		},
		{
			name:    "new negative parts permit publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
				},
			),
			want: contractRevalidation(true, traceFragmentGuardReasonSnapshotDeltaClear, 43, 1, 1),
		},
		{
			name:    "new non-candidate part permits publication without a probe",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 200, 210, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(true, traceFragmentGuardReasonSnapshotDeltaClear, 43, 1, 0),
		},
		{
			name:    "new positive part rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		},
		{
			name:    "earlier grace-boundary positive rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 80, 90, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		},
		{
			name:    "later grace-boundary positive rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 120, 130, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		},
		{
			name:    "publication fence is mandatory",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(42, nil)
				request.PublicationFenceHeld = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonPublicationFenceMissing, 42, 0, 0),
		},
		{
			name:    "incomplete delta catalog rejects publication",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(43, nil)
				request.DeltaCatalogComplete = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonCatalogIncomplete, 43, 0, 0),
		},
		{
			name:    "ownership change rejects publication",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(43, nil)
				request.OwnershipUnchanged = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonOwnershipChanged, 43, 0, 0),
		},
		{
			name:    "ownership change rejects unchanged epoch shortcut",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(42, nil)
				request.OwnershipUnchanged = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonOwnershipChanged, 42, 0, 0),
		},
		{
			name:    "selected input change rejects publication",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(43, nil)
				request.SelectedInputsUnchanged = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonSelectedInputsChanged, 43, 0, 0),
		},
		{
			name:    "selected input change rejects unchanged epoch shortcut",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(42, nil)
				request.SelectedInputsUnchanged = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonSelectedInputsChanged, 42, 0, 0),
		},
		{
			name:    "snapshot regression rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(41, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonSnapshotRegressed, 41, 0, 0),
		},
		{
			name:    "unknown confirmed drop bounds reject publication",
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				{TraceID: "trace-a", MinTimestamp: 100, MaxTimestamp: 110},
			},
			request: contractRevalidationRequest(43, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonTraceBoundsInvalid, 43, 0, 0),
		},
		{
			name:    "empty confirmed trace id rejects publication",
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				{MinTimestamp: 100, MaxTimestamp: 110, BoundsKnown: true},
			},
			request: contractRevalidationRequest(43, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonTraceIncomplete, 43, 0, 0),
		},
		{
			name:    "reversed confirmed drop bounds reject publication",
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				{TraceID: "trace-a", MinTimestamp: 110, MaxTimestamp: 100, BoundsKnown: true},
			},
			request: contractRevalidationRequest(43, nil),
			want:    contractRevalidation(false, traceFragmentGuardReasonTraceBoundsInvalid, 43, 0, 0),
		},
		{
			name:    "unknown delta part bounds reject publication",
			catalog: contractCatalog(),
			request: func() traceFragmentGuardRevalidationRequest {
				request := contractRevalidationRequest(
					43,
					[]traceFragmentGuardPart{
						contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
					},
				)
				request.DeltaParts[0].BoundsKnown = false
				return request
			}(),
			want: contractRevalidation(false, traceFragmentGuardReasonPartBoundsInvalid, 43, 0, 0),
		},
		{
			name:    "reversed delta part bounds reject publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 110, 100, contractFilter(traceFragmentMembershipAbsent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonPartBoundsInvalid, 43, 0, 0),
		},
		{
			name:    "missing delta filter rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, nil),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonFilterUnavailable, 43, 1, 0),
		},
		{
			name:    "unknown delta membership rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipUnknown, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonFilterUnavailable, 43, 1, 1),
		},
		{
			name:    "delta filter error rejects publication",
			catalog: contractCatalog(),
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, filterReadErr)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonFilterError, 43, 1, 1),
		},
		{
			name:     "canceled revalidation rejects publication",
			catalog:  contractCatalog(),
			request:  contractRevalidationRequest(43, nil),
			want:     contractRevalidation(false, traceFragmentGuardReasonCanceled, 43, 0, 0),
			canceled: true,
		},
		{
			name:    "revalidation probe budget rejects publication",
			config:  &oneProbeConfig,
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				contractConfirmedDrop("trace-a", 100, 110),
				contractConfirmedDrop("trace-b", 100, 110),
			},
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, 100, 110, contractFilter(traceFragmentMembershipAbsent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonBudgetExhausted, 43, 2, 1),
		},
		{
			name:    "minimum timestamp expansion saturates during revalidation",
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				contractConfirmedDrop("trace-a", math.MinInt64+1, math.MinInt64+2),
			},
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, math.MinInt64, math.MinInt64, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		},
		{
			name:    "maximum timestamp expansion saturates during revalidation",
			catalog: contractCatalog(),
			confirmedDrops: []traceFragmentGuardConfirmedDrop{
				contractConfirmedDrop("trace-a", math.MaxInt64-2, math.MaxInt64-1),
			},
			request: contractRevalidationRequest(
				43,
				[]traceFragmentGuardPart{
					contractPart(1, math.MaxInt64, math.MaxInt64, contractFilter(traceFragmentMembershipMaybePresent, nil)),
				},
			),
			want: contractRevalidation(false, traceFragmentGuardReasonSnapshotDeltaPositive, 43, 1, 1),
		},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			config := contractGuardConfig()
			if testCase.config != nil {
				config = *testCase.config
			}
			confirmedDrops := testCase.confirmedDrops
			if confirmedDrops == nil {
				confirmedDrops = []traceFragmentGuardConfirmedDrop{
					contractConfirmedDrop("trace-a", 100, 110),
				}
			}
			guard := newContractGuardWithDrops(config, testCase.catalog, confirmedDrops)
			defer guard.Close()

			testContext := context.Background()
			if testCase.canceled {
				canceledContext, cancel := context.WithCancel(testContext)
				cancel()
				testContext = canceledContext
			}

			got := guard.RevalidateDrops(testContext, testCase.request)

			assert.Equal(t, testCase.want, got)
			assert.Equal(t, testCase.want.BloomProbes, contractProbeCount(testCase.request.DeltaParts))
		})
	}
}

func TestAssembleTraceFragmentGuardTraceIncludesEveryStagedBlock(t *testing.T) {
	staged := []stagedTrace{
		{
			traceID: "trace-a",
			slowBlock: &blockPointer{
				bm: blockMetadata{
					traceID:    "trace-a",
					timestamps: timestampsMetadata{min: 100, max: 110, known: true},
				},
			},
		},
		{
			traceID: "trace-a",
			slowBlock: &blockPointer{
				bm: blockMetadata{
					traceID:    "trace-a",
					timestamps: timestampsMetadata{min: 200, max: 210, known: true},
				},
			},
		},
	}

	got := assembleTraceFragmentGuardTrace("trace-a", staged)

	assert.Equal(t, traceFragmentGuardTrace{
		TraceID: "trace-a",
		Blocks: []traceFragmentGuardBlock{
			{MinTimestamp: 100, MaxTimestamp: 110, BoundsKnown: true},
			{MinTimestamp: 200, MaxTimestamp: 210, BoundsKnown: true},
		},
		Complete: true,
	}, got)
}

func TestAssembleTraceFragmentGuardTraceReusesCallerStorage(t *testing.T) {
	staged := []stagedTrace{{
		traceID: "trace-a",
		slowBlock: &blockPointer{bm: blockMetadata{
			traceID: "trace-a", timestamps: timestampsMetadata{min: 100, max: 110, known: true},
		}},
	}}
	storage := make([]traceFragmentGuardBlock, 0, len(staged))

	got := assembleTraceFragmentGuardTraceInto("trace-a", staged, storage)

	require.Len(t, got.Blocks, 1)
	require.Equal(t, &storage[:cap(storage)][0], &got.Blocks[0])
	allocations := testing.AllocsPerRun(100, func() {
		got = assembleTraceFragmentGuardTraceInto("trace-a", staged, got.Blocks[:0])
	})
	require.Zero(t, allocations)
}

func TestAssembleTraceFragmentGuardTracePreallocatesStandaloneStorage(t *testing.T) {
	const blockCount = 8
	staged := make([]stagedTrace, blockCount)
	for stagedIdx := range staged {
		staged[stagedIdx] = stagedTrace{
			traceID: "trace-a",
			slowBlock: &blockPointer{bm: blockMetadata{
				traceID: "trace-a", timestamps: timestampsMetadata{min: 100, max: 110, known: true},
			}},
		}
	}

	var got traceFragmentGuardTrace
	allocations := testing.AllocsPerRun(100, func() {
		got = assembleTraceFragmentGuardTrace("trace-a", staged)
	})
	require.Len(t, got.Blocks, blockCount)
	require.LessOrEqual(t, allocations, 1.0, "the standalone helper should allocate its block vector once")
}

func TestMergeTwoBlocksPreservesTraceTimestampBoundsForGuard(t *testing.T) {
	left := &blockPointer{
		block: block{
			spans:   [][]byte{[]byte("left")},
			spanIDs: []string{"left"},
			minTS:   100,
			maxTS:   110,
		},
		bm: blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 100, max: 110, known: true},
		},
	}
	right := &blockPointer{
		block: block{
			spans:   [][]byte{[]byte("right")},
			spanIDs: []string{"right"},
			minTS:   200,
			maxTS:   210,
		},
		bm: blockMetadata{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 200, max: 210, known: true},
		},
	}
	target := &blockPointer{}

	mergeTwoBlocks(target, left, right)

	assert.Equal(t, timestampsMetadata{min: 100, max: 210, known: true}, target.bm.timestamps)
	assert.Equal(t, int64(100), target.block.minTS)
	assert.Equal(t, int64(210), target.block.maxTS)
}

func TestSequentialBlockReadRestoresTraceTimestampBounds(t *testing.T) {
	spanBuffer := &pkgbytes.Buffer{}
	spanWriter := &writer{}
	spanWriter.init(spanBuffer)
	blockWriters := &writers{
		spanWriter: *spanWriter,
	}
	source := block{
		spans:   [][]byte{[]byte("span")},
		spanIDs: []string{"span-id"},
		minTS:   100,
		maxTS:   110,
	}
	var metadata blockMetadata
	source.mustWriteTo("trace-a", &metadata, blockWriters)
	metadata.timestamps.known = true
	sourcePart := &part{
		primary:     &pkgbytes.Buffer{},
		spans:       spanBuffer,
		tagMetadata: make(map[string]fs.Reader),
		tags:        make(map[string]fs.Reader),
	}
	var readers seqReaders
	readers.init(sourcePart)
	defer readers.reset()
	var decoder encoding.BytesBlockDecoder
	var decoded block

	decoded.mustSeqReadFrom(&decoder, &readers, metadata)

	assert.Equal(t, int64(100), decoded.minTS)
	assert.Equal(t, int64(110), decoded.maxTS)
}

func TestBlockMetadataPersistsTraceTimestampBounds(t *testing.T) {
	testCases := []timestampsMetadata{
		{min: -123, max: 456, known: true},
		{min: 0, max: 0, known: true},
	}
	for testCaseIdx := range testCases {
		expectedTimestamps := testCases[testCaseIdx]
		t.Run(expectedTimestampsName(expectedTimestamps), func(t *testing.T) {
			original := blockMetadata{
				traceID:    "trace-a",
				timestamps: expectedTimestamps,
				spans:      &dataBlock{},
				tags:       make(map[string]*dataBlock),
			}

			encoded := original.marshal(nil)
			var decoded blockMetadata
			remaining, decodeErr := decoded.unmarshal(encoded, nil)

			require.NoError(t, decodeErr)
			require.Empty(t, remaining)
			assert.Equal(t, original.timestamps, decoded.timestamps)
		})
	}
}

func TestBlockWriteMarksTraceTimestampBoundsKnown(t *testing.T) {
	spanBuffer := &pkgbytes.Buffer{}
	spanWriter := &writer{}
	spanWriter.init(spanBuffer)
	blockWriters := &writers{
		spanWriter: *spanWriter,
	}
	blockData := block{
		spans:   [][]byte{[]byte("span")},
		spanIDs: []string{"span-id"},
		minTS:   100,
		maxTS:   110,
	}
	var metadata blockMetadata

	blockData.mustWriteTo("trace-a", &metadata, blockWriters)

	assert.Equal(t, timestampsMetadata{min: 100, max: 110, known: true}, metadata.timestamps)
}

func TestFilteredBlockMetadataPreservesTraceTimestampBounds(t *testing.T) {
	entries := []blockMetadata{
		{
			traceID:    "trace-a",
			timestamps: timestampsMetadata{min: 100, max: 110, known: true},
			spans:      &dataBlock{},
			tags:       make(map[string]*dataBlock),
		},
		{
			traceID:    "trace-b",
			timestamps: timestampsMetadata{min: 200, max: 210, known: true},
			spans:      &dataBlock{},
			tags:       make(map[string]*dataBlock),
		},
	}
	var encoded []byte
	for entryIdx := range entries {
		encoded = entries[entryIdx].marshal(encoded)
	}

	decoded, decodeErr := unmarshalBlockMetadataFiltered(nil, encoded, nil, []string{"trace-b"})

	require.NoError(t, decodeErr)
	require.Len(t, decoded, 1)
	assert.Equal(t, entries[1].traceID, decoded[0].traceID)
	assert.Equal(t, entries[1].timestamps, decoded[0].timestamps)
}

func contractDropInput(outsideParts ...traceFragmentGuardPart) traceFragmentGuardResolveInput {
	return traceFragmentGuardResolveInput{
		trace: traceFragmentGuardTrace{
			TraceID: "trace-a",
			Blocks: []traceFragmentGuardBlock{
				{MinTimestamp: 100, MaxTimestamp: 110, BoundsKnown: true},
			},
			Complete: true,
		},
		catalog:       contractCatalog(outsideParts...),
		samplerAction: traceFragmentSamplerActionDrop,
	}
}

func contractCatalog(outsideParts ...traceFragmentGuardPart) traceFragmentGuardCatalog {
	return traceFragmentGuardCatalog{
		Pin:                    &traceFragmentGuardContractPin{},
		OutsideParts:           outsideParts,
		BaseEpoch:              42,
		CoverageMinTimestamp:   math.MinInt64,
		CoverageMaxTimestamp:   math.MaxInt64,
		EnforcedMaxFragmentGap: 10 * time.Nanosecond,
		Complete:               true,
		CoverageKnown:          true,
		TemporalSafety:         traceFragmentTemporalSafetyMaxGapEnforced,
	}
}

func contractGuardConfig() traceFragmentGuardConfig {
	return traceFragmentGuardConfig{
		Grace:          10 * time.Nanosecond,
		MaxBloomProbes: 16,
	}
}

func contractMultiBlockRange() []traceFragmentGuardBlock {
	return []traceFragmentGuardBlock{
		{MinTimestamp: 100, MaxTimestamp: 110, BoundsKnown: true},
		{MinTimestamp: 200, MaxTimestamp: 210, BoundsKnown: true},
	}
}

func contractRevalidationRequest(currentEpoch uint64,
	deltaParts []traceFragmentGuardPart,
) traceFragmentGuardRevalidationRequest {
	return traceFragmentGuardRevalidationRequest{
		DeltaParts:              deltaParts,
		CurrentEpoch:            currentEpoch,
		DeltaCatalogComplete:    true,
		OwnershipUnchanged:      true,
		SelectedInputsUnchanged: true,
		PublicationFenceHeld:    true,
	}
}

func contractConfirmedDrop(traceID string, minTimestamp, maxTimestamp int64) traceFragmentGuardConfirmedDrop {
	return traceFragmentGuardConfirmedDrop{
		TraceID:      traceID,
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BoundsKnown:  true,
	}
}

func newContractGuardWithDrops(config traceFragmentGuardConfig, catalog traceFragmentGuardCatalog,
	confirmedDrops []traceFragmentGuardConfirmedDrop,
) traceFragmentGuard {
	guard := newTraceFragmentGuard(config, catalog)
	defaultGuard := guard.(*defaultTraceFragmentGuard)
	defaultGuard.confirmedDrops = append(defaultGuard.confirmedDrops, confirmedDrops...)
	return guard
}

func contractPart(id uint64, minTimestamp, maxTimestamp int64,
	membershipFilter traceFragmentMembershipFilter,
) traceFragmentGuardPart {
	return traceFragmentGuardPart{
		Filter:       membershipFilter,
		ID:           id,
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BoundsKnown:  true,
	}
}

func contractFilter(membership traceFragmentMembership, filterErr error) *traceFragmentGuardContractFilter {
	return &traceFragmentGuardContractFilter{
		filterErr:  filterErr,
		membership: membership,
	}
}

func contractDecision(action traceFragmentGuardAction, reason traceFragmentGuardReason, baseEpoch uint64,
	candidateParts, bloomProbes int,
) traceFragmentGuardDecision {
	decision := traceFragmentGuardDecision{
		Action:         action,
		Reason:         reason,
		BaseEpoch:      baseEpoch,
		CandidateParts: candidateParts,
		BloomProbes:    bloomProbes,
	}
	if action == traceFragmentGuardActionDrop {
		confirmedDrop := contractConfirmedDrop("trace-a", 100, 110)
		decision.ConfirmedDrop = &confirmedDrop
	}
	return decision
}

func contractDropDecision(reason traceFragmentGuardReason, baseEpoch uint64,
	candidateParts, bloomProbes int, traceID string, minTimestamp, maxTimestamp int64,
) traceFragmentGuardDecision {
	decision := contractDecision(traceFragmentGuardActionDrop, reason, baseEpoch, candidateParts, bloomProbes)
	confirmedDrop := contractConfirmedDrop(traceID, minTimestamp, maxTimestamp)
	decision.ConfirmedDrop = &confirmedDrop
	return decision
}

func contractRevalidation(publish bool, reason traceFragmentGuardReason, currentEpoch uint64,
	recheckedTraces, bloomProbes int,
) traceFragmentGuardRevalidation {
	return traceFragmentGuardRevalidation{
		Publish:         publish,
		Reason:          reason,
		CurrentEpoch:    currentEpoch,
		RecheckedTraces: recheckedTraces,
		BloomProbes:     bloomProbes,
	}
}

func contractProbeCount(parts []traceFragmentGuardPart) int {
	probeCount := 0
	for partIdx := range parts {
		contractPartFilter, ok := parts[partIdx].Filter.(*traceFragmentGuardContractFilter)
		if ok {
			probeCount += contractPartFilter.probes
		}
	}
	return probeCount
}

func expectedTimestampsName(timestamps timestampsMetadata) string {
	if timestamps.min == 0 && timestamps.max == 0 {
		return "known zero range"
	}
	return "non-zero range"
}
