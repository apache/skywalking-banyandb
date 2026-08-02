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
	"math"
	"strings"
	"sync"
	"time"
)

type traceFragmentGuard interface {
	Resolve(context.Context, traceFragmentGuardTrace, traceFragmentSamplerAction) traceFragmentGuardDecision
	RevalidateDrops(context.Context, traceFragmentGuardRevalidationRequest) traceFragmentGuardRevalidation
	Close()
}

type traceFragmentMembershipFilter interface {
	Lookup(string) (traceFragmentMembership, error)
}

type traceFragmentGuardCatalogPin interface {
	Release()
}

type traceFragmentGuardConfig struct {
	Grace time.Duration
	// MaxBloomProbes is a hard per-session limit; zero prohibits Bloom lookups.
	MaxBloomProbes int
	// MaxConfirmedDrops is optional for direct guard callers: zero preserves the
	// original unbounded contract. Runtime sessions always supply a positive bound.
	MaxConfirmedDrops int
}

type traceFragmentTemporalSafety uint8

const (
	traceFragmentTemporalSafetyUnknown traceFragmentTemporalSafety = iota
	traceFragmentTemporalSafetyMaxGapEnforced
)

type traceFragmentMembership uint8

const (
	traceFragmentMembershipUnknown traceFragmentMembership = iota
	traceFragmentMembershipAbsent
	traceFragmentMembershipMaybePresent
)

type traceFragmentSamplerAction uint8

const (
	traceFragmentSamplerActionUnknown traceFragmentSamplerAction = iota
	traceFragmentSamplerActionKeep
	traceFragmentSamplerActionDrop
)

type traceFragmentGuardAction uint8

const (
	traceFragmentGuardActionDefer traceFragmentGuardAction = iota
	traceFragmentGuardActionKeep
	traceFragmentGuardActionDrop
)

type traceFragmentGuardReason string

const (
	traceFragmentGuardReasonSamplerKeep             traceFragmentGuardReason = "sampler_keep"
	traceFragmentGuardReasonSamplerActionInvalid    traceFragmentGuardReason = "sampler_action_invalid"
	traceFragmentGuardReasonConfigInvalid           traceFragmentGuardReason = "config_invalid"
	traceFragmentGuardReasonTemporalSafetyUnknown   traceFragmentGuardReason = "temporal_safety_unknown"
	traceFragmentGuardReasonNoCandidate             traceFragmentGuardReason = "no_candidate"
	traceFragmentGuardReasonAllCandidatesNegative   traceFragmentGuardReason = "all_candidates_negative"
	traceFragmentGuardReasonFilterPositive          traceFragmentGuardReason = "filter_positive"
	traceFragmentGuardReasonTraceIncomplete         traceFragmentGuardReason = "trace_incomplete"
	traceFragmentGuardReasonTraceBoundsInvalid      traceFragmentGuardReason = "trace_bounds_invalid"
	traceFragmentGuardReasonCatalogIncomplete       traceFragmentGuardReason = "catalog_incomplete"
	traceFragmentGuardReasonCatalogUnpinned         traceFragmentGuardReason = "catalog_unpinned"
	traceFragmentGuardReasonPartBoundsInvalid       traceFragmentGuardReason = "part_bounds_invalid"
	traceFragmentGuardReasonFilterUnavailable       traceFragmentGuardReason = "filter_unavailable"
	traceFragmentGuardReasonFilterError             traceFragmentGuardReason = "filter_error"
	traceFragmentGuardReasonBudgetExhausted         traceFragmentGuardReason = "budget_exhausted"
	traceFragmentGuardReasonCanceled                traceFragmentGuardReason = "canceled"
	traceFragmentGuardReasonSegmentBoundary         traceFragmentGuardReason = "segment_boundary"
	traceFragmentGuardReasonSnapshotUnchanged       traceFragmentGuardReason = "snapshot_unchanged"
	traceFragmentGuardReasonSnapshotDeltaClear      traceFragmentGuardReason = "snapshot_delta_clear"
	traceFragmentGuardReasonSnapshotDeltaPositive   traceFragmentGuardReason = "snapshot_delta_positive"
	traceFragmentGuardReasonSnapshotRegressed       traceFragmentGuardReason = "snapshot_regressed"
	traceFragmentGuardReasonSnapshotChanged         traceFragmentGuardReason = "snapshot_changed_after_revalidation"
	traceFragmentGuardReasonOwnershipChanged        traceFragmentGuardReason = "ownership_changed"
	traceFragmentGuardReasonSelectedInputsChanged   traceFragmentGuardReason = "selected_inputs_changed"
	traceFragmentGuardReasonPublicationFenceMissing traceFragmentGuardReason = "publication_fence_missing"
)

type traceFragmentGuardBlock struct {
	MinTimestamp int64
	MaxTimestamp int64
	BoundsKnown  bool
}

type traceFragmentGuardTrace struct {
	TraceID  string
	Blocks   []traceFragmentGuardBlock
	Complete bool
}

type traceFragmentGuardPart struct {
	Filter       traceFragmentMembershipFilter
	ID           uint64
	MinTimestamp int64
	MaxTimestamp int64
	BoundsKnown  bool
}

type traceFragmentGuardCatalog struct {
	Pin                    traceFragmentGuardCatalogPin
	OutsideParts           []traceFragmentGuardPart
	BaseEpoch              uint64
	CoverageMinTimestamp   int64
	CoverageMaxTimestamp   int64
	EnforcedMaxFragmentGap time.Duration
	Complete               bool
	CoverageKnown          bool
	TemporalSafety         traceFragmentTemporalSafety
}

type traceFragmentGuardDecision struct {
	ConfirmedDrop  *traceFragmentGuardConfirmedDrop
	Reason         traceFragmentGuardReason
	BaseEpoch      uint64
	CandidateParts int
	BloomProbes    int
	Action         traceFragmentGuardAction
}

type traceFragmentGuardConfirmedDrop struct {
	TraceID      string
	MinTimestamp int64
	MaxTimestamp int64
	BoundsKnown  bool
}

type traceFragmentGuardRevalidationRequest struct {
	DeltaParts              []traceFragmentGuardPart
	CurrentEpoch            uint64
	DeltaCatalogComplete    bool
	OwnershipUnchanged      bool
	SelectedInputsUnchanged bool
	// PublicationFenceHeld means the request is evaluated against the pinned immutable
	// current snapshot. The serialized introducer separately verifies this epoch.
	PublicationFenceHeld bool
}

type traceFragmentGuardRevalidation struct {
	Reason          traceFragmentGuardReason
	CurrentEpoch    uint64
	RecheckedTraces int
	BloomProbes     int
	Publish         bool
}

type defaultTraceFragmentGuard struct {
	confirmedDrops          []traceFragmentGuardConfirmedDrop
	catalogPartBoundsReason traceFragmentGuardReason
	catalog                 traceFragmentGuardCatalog
	config                  traceFragmentGuardConfig
	lifecycleMu             sync.RWMutex
	stateMu                 sync.Mutex
	bloomProbes             int
	closed                  bool
}

func newTraceFragmentGuard(config traceFragmentGuardConfig, catalog traceFragmentGuardCatalog) traceFragmentGuard {
	catalog.OutsideParts = append([]traceFragmentGuardPart(nil), catalog.OutsideParts...)
	return &defaultTraceFragmentGuard{
		config:                  config,
		catalog:                 catalog,
		catalogPartBoundsReason: traceFragmentPartsValidationReason(catalog.OutsideParts),
	}
}

func assembleTraceFragmentGuardTrace(traceID string, staged []stagedTrace) traceFragmentGuardTrace {
	assembled := traceFragmentGuardTrace{
		TraceID: traceID,
		Blocks:  make([]traceFragmentGuardBlock, 0, len(staged)),
	}
	matched := false
	complete := traceID != ""
	for stagedIdx := range staged {
		stagedBlock := &staged[stagedIdx]
		if stagedBlock.traceID != traceID {
			complete = false
			continue
		}
		matched = true
		var timestamps timestampsMetadata
		switch {
		case stagedBlock.isRaw:
			if stagedBlock.rawBM.traceID != traceID {
				complete = false
			}
			timestamps = stagedBlock.rawBM.timestamps
		case stagedBlock.slowBlock != nil:
			if stagedBlock.slowBlock.bm.traceID != traceID {
				complete = false
			}
			timestamps = stagedBlock.slowBlock.bm.timestamps
		default:
			complete = false
		}
		assembled.Blocks = append(assembled.Blocks, traceFragmentGuardBlock{
			MinTimestamp: timestamps.min,
			MaxTimestamp: timestamps.max,
			BoundsKnown:  timestamps.known,
		})
	}
	assembled.Complete = complete && matched
	return assembled
}

func (g *defaultTraceFragmentGuard) Resolve(ctx context.Context, traceData traceFragmentGuardTrace,
	samplerAction traceFragmentSamplerAction,
) traceFragmentGuardDecision {
	if samplerAction == traceFragmentSamplerActionKeep {
		return g.resolveDecision(traceFragmentGuardActionKeep, traceFragmentGuardReasonSamplerKeep, 0, 0, nil)
	}
	if samplerAction != traceFragmentSamplerActionDrop {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSamplerActionInvalid, 0, 0, nil)
	}
	g.lifecycleMu.RLock()
	defer g.lifecycleMu.RUnlock()
	if g.closed {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCatalogUnpinned, 0, 0, nil)
	}
	if reason := g.baseValidationReason(); reason != "" {
		return g.resolveDecision(traceFragmentGuardActionDefer, reason, 0, 0, nil)
	}
	if ctx.Err() != nil {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCanceled, 0, 0, nil)
	}
	if !traceData.Complete || traceData.TraceID == "" {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceIncomplete, 0, 0, nil)
	}
	traceMin, traceMax, validBounds := traceFragmentBounds(traceData.Blocks)
	if !validBounds {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonTraceBoundsInvalid, 0, 0, nil)
	}
	if !g.catalog.Complete {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCatalogIncomplete, 0, 0, nil)
	}
	guardMin := traceFragmentSaturatingSub(traceMin, int64(g.config.Grace))
	guardMax := traceFragmentSaturatingAdd(traceMax, int64(g.config.Grace))
	if !g.catalog.CoverageKnown || guardMin < g.catalog.CoverageMinTimestamp || guardMax > g.catalog.CoverageMaxTimestamp {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonSegmentBoundary, 0, 0, nil)
	}
	if g.catalogPartBoundsReason != "" {
		return g.resolveDecision(traceFragmentGuardActionDefer, g.catalogPartBoundsReason, 0, 0, nil)
	}
	candidateCount := traceFragmentCandidateCount(g.catalog.OutsideParts, guardMin, guardMax)
	if ctx.Err() != nil {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCanceled, candidateCount, 0, nil)
	}
	confirmedDrop := &traceFragmentGuardConfirmedDrop{
		TraceID:      traceData.TraceID,
		MinTimestamp: traceMin,
		MaxTimestamp: traceMax,
		BoundsKnown:  true,
	}
	if candidateCount == 0 {
		if !g.recordConfirmedDrop(confirmedDrop) {
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonBudgetExhausted, 0, 0, nil)
		}
		return g.resolveDecision(traceFragmentGuardActionDrop, traceFragmentGuardReasonNoCandidate, 0, 0, confirmedDrop)
	}

	callProbes := 0
	for candidateIdx := range g.catalog.OutsideParts {
		candidate := g.catalog.OutsideParts[candidateIdx]
		if candidate.MaxTimestamp < guardMin || candidate.MinTimestamp > guardMax {
			continue
		}
		if candidate.Filter == nil {
			return g.resolveDecision(
				traceFragmentGuardActionDefer,
				traceFragmentGuardReasonFilterUnavailable,
				candidateCount,
				callProbes,
				nil,
			)
		}
		if !g.reserveBloomProbe() {
			return g.resolveDecision(
				traceFragmentGuardActionDefer,
				traceFragmentGuardReasonBudgetExhausted,
				candidateCount,
				callProbes,
				nil,
			)
		}
		if ctx.Err() != nil {
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCanceled, candidateCount, callProbes, nil)
		}
		callProbes++
		membership, lookupErr := candidate.Filter.Lookup(traceData.TraceID)
		if lookupErr != nil {
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterError, candidateCount, callProbes, nil)
		}
		if ctx.Err() != nil {
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonCanceled, candidateCount, callProbes, nil)
		}
		switch membership {
		case traceFragmentMembershipAbsent:
			continue
		case traceFragmentMembershipMaybePresent:
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterPositive, candidateCount, callProbes, nil)
		case traceFragmentMembershipUnknown:
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterUnavailable, candidateCount, callProbes, nil)
		default:
			return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonFilterUnavailable, candidateCount, callProbes, nil)
		}
	}
	if !g.recordConfirmedDrop(confirmedDrop) {
		return g.resolveDecision(traceFragmentGuardActionDefer, traceFragmentGuardReasonBudgetExhausted, candidateCount, callProbes, nil)
	}
	return g.resolveDecision(
		traceFragmentGuardActionDrop,
		traceFragmentGuardReasonAllCandidatesNegative,
		candidateCount,
		callProbes,
		confirmedDrop,
	)
}

func (g *defaultTraceFragmentGuard) RevalidateDrops(ctx context.Context,
	request traceFragmentGuardRevalidationRequest,
) traceFragmentGuardRevalidation {
	result := traceFragmentGuardRevalidation{CurrentEpoch: request.CurrentEpoch}
	g.lifecycleMu.RLock()
	defer g.lifecycleMu.RUnlock()
	if g.closed {
		result.Reason = traceFragmentGuardReasonCatalogUnpinned
		return result
	}
	if reason := g.baseValidationReason(); reason != "" {
		result.Reason = reason
		return result
	}
	if !g.catalog.Complete {
		result.Reason = traceFragmentGuardReasonCatalogIncomplete
		return result
	}
	switch {
	case !request.PublicationFenceHeld:
		result.Reason = traceFragmentGuardReasonPublicationFenceMissing
		return result
	case !request.OwnershipUnchanged:
		result.Reason = traceFragmentGuardReasonOwnershipChanged
		return result
	case !request.SelectedInputsUnchanged:
		result.Reason = traceFragmentGuardReasonSelectedInputsChanged
		return result
	case request.CurrentEpoch < g.catalog.BaseEpoch:
		result.Reason = traceFragmentGuardReasonSnapshotRegressed
		return result
	}
	if ctx.Err() != nil {
		result.Reason = traceFragmentGuardReasonCanceled
		return result
	}
	if request.CurrentEpoch == g.catalog.BaseEpoch {
		result.Publish = true
		result.Reason = traceFragmentGuardReasonSnapshotUnchanged
		return result
	}
	if !request.DeltaCatalogComplete {
		result.Reason = traceFragmentGuardReasonCatalogIncomplete
		return result
	}
	confirmedDrops := g.snapshotConfirmedDrops()
	if reason := traceFragmentConfirmedDropsValidationReason(confirmedDrops); reason != "" {
		result.Reason = reason
		return result
	}
	if reason := traceFragmentPartsValidationReason(request.DeltaParts); reason != "" {
		result.Reason = reason
		return result
	}

	for dropIdx := range confirmedDrops {
		if ctx.Err() != nil {
			result.Reason = traceFragmentGuardReasonCanceled
			return result
		}
		confirmedDrop := &confirmedDrops[dropIdx]
		result.RecheckedTraces++
		guardMin := traceFragmentSaturatingSub(confirmedDrop.MinTimestamp, int64(g.config.Grace))
		guardMax := traceFragmentSaturatingAdd(confirmedDrop.MaxTimestamp, int64(g.config.Grace))
		for partIdx := range request.DeltaParts {
			deltaPart := &request.DeltaParts[partIdx]
			if deltaPart.MaxTimestamp < guardMin || deltaPart.MinTimestamp > guardMax {
				continue
			}
			if deltaPart.Filter == nil {
				result.Reason = traceFragmentGuardReasonFilterUnavailable
				return result
			}
			if !g.reserveBloomProbe() {
				result.Reason = traceFragmentGuardReasonBudgetExhausted
				return result
			}
			if ctx.Err() != nil {
				result.Reason = traceFragmentGuardReasonCanceled
				return result
			}
			result.BloomProbes++
			membership, lookupErr := deltaPart.Filter.Lookup(confirmedDrop.TraceID)
			if lookupErr != nil {
				result.Reason = traceFragmentGuardReasonFilterError
				return result
			}
			if ctx.Err() != nil {
				result.Reason = traceFragmentGuardReasonCanceled
				return result
			}
			switch membership {
			case traceFragmentMembershipAbsent:
				continue
			case traceFragmentMembershipMaybePresent:
				result.Reason = traceFragmentGuardReasonSnapshotDeltaPositive
				return result
			case traceFragmentMembershipUnknown:
				result.Reason = traceFragmentGuardReasonFilterUnavailable
				return result
			default:
				result.Reason = traceFragmentGuardReasonFilterUnavailable
				return result
			}
		}
	}
	result.Publish = true
	result.Reason = traceFragmentGuardReasonSnapshotDeltaClear
	return result
}

func (g *defaultTraceFragmentGuard) Close() {
	g.lifecycleMu.Lock()
	if g.closed {
		g.lifecycleMu.Unlock()
		return
	}
	g.closed = true
	pin := g.catalog.Pin
	g.catalog.Pin = nil
	g.stateMu.Lock()
	g.confirmedDrops = nil
	g.stateMu.Unlock()
	g.lifecycleMu.Unlock()
	if pin != nil {
		pin.Release()
	}
}

func (g *defaultTraceFragmentGuard) resolveDecision(action traceFragmentGuardAction, reason traceFragmentGuardReason,
	candidateParts, bloomProbes int, confirmedDrop *traceFragmentGuardConfirmedDrop,
) traceFragmentGuardDecision {
	return traceFragmentGuardDecision{
		ConfirmedDrop:  confirmedDrop,
		Action:         action,
		Reason:         reason,
		BaseEpoch:      g.catalog.BaseEpoch,
		CandidateParts: candidateParts,
		BloomProbes:    bloomProbes,
	}
}

func (g *defaultTraceFragmentGuard) baseValidationReason() traceFragmentGuardReason {
	if g.config.Grace < 0 || g.config.MaxBloomProbes < 0 || g.config.MaxConfirmedDrops < 0 {
		return traceFragmentGuardReasonConfigInvalid
	}
	switch g.catalog.TemporalSafety {
	case traceFragmentTemporalSafetyMaxGapEnforced:
		if g.catalog.EnforcedMaxFragmentGap < 0 || g.config.Grace < g.catalog.EnforcedMaxFragmentGap {
			return traceFragmentGuardReasonTemporalSafetyUnknown
		}
	default:
		return traceFragmentGuardReasonTemporalSafetyUnknown
	}
	if g.catalog.Pin == nil {
		return traceFragmentGuardReasonCatalogUnpinned
	}
	return ""
}

func traceFragmentBounds(blocks []traceFragmentGuardBlock) (int64, int64, bool) {
	if len(blocks) == 0 {
		return 0, 0, false
	}
	minTimestamp := int64(math.MaxInt64)
	maxTimestamp := int64(math.MinInt64)
	for blockIdx := range blocks {
		blockData := &blocks[blockIdx]
		if !blockData.BoundsKnown || blockData.MinTimestamp > blockData.MaxTimestamp {
			return 0, 0, false
		}
		minTimestamp = min(minTimestamp, blockData.MinTimestamp)
		maxTimestamp = max(maxTimestamp, blockData.MaxTimestamp)
	}
	return minTimestamp, maxTimestamp, true
}

func traceFragmentCandidateCount(parts []traceFragmentGuardPart, guardMin, guardMax int64) int {
	candidateCount := 0
	for partIdx := range parts {
		partData := parts[partIdx]
		if partData.MaxTimestamp >= guardMin && partData.MinTimestamp <= guardMax {
			candidateCount++
		}
	}
	return candidateCount
}

func traceFragmentPartsValidationReason(parts []traceFragmentGuardPart) traceFragmentGuardReason {
	for partIdx := range parts {
		partData := &parts[partIdx]
		if !partData.BoundsKnown || partData.MinTimestamp > partData.MaxTimestamp {
			return traceFragmentGuardReasonPartBoundsInvalid
		}
	}
	return ""
}

func traceFragmentConfirmedDropsValidationReason(drops []traceFragmentGuardConfirmedDrop) traceFragmentGuardReason {
	for dropIdx := range drops {
		dropData := &drops[dropIdx]
		if dropData.TraceID == "" {
			return traceFragmentGuardReasonTraceIncomplete
		}
		if !dropData.BoundsKnown || dropData.MinTimestamp > dropData.MaxTimestamp {
			return traceFragmentGuardReasonTraceBoundsInvalid
		}
	}
	return ""
}

func traceFragmentSaturatingSub(value, delta int64) int64 {
	if delta > 0 && value < math.MinInt64+delta {
		return math.MinInt64
	}
	return value - delta
}

func traceFragmentSaturatingAdd(value, delta int64) int64 {
	if delta > 0 && value > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return value + delta
}

func (g *defaultTraceFragmentGuard) reserveBloomProbe() bool {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	if g.bloomProbes >= g.config.MaxBloomProbes {
		return false
	}
	g.bloomProbes++
	return true
}

func (g *defaultTraceFragmentGuard) recordConfirmedDrop(drop *traceFragmentGuardConfirmedDrop) bool {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	if g.config.MaxConfirmedDrops > 0 && len(g.confirmedDrops) >= g.config.MaxConfirmedDrops {
		return false
	}
	storedDrop := *drop
	storedDrop.TraceID = strings.Clone(drop.TraceID)
	g.confirmedDrops = append(g.confirmedDrops, storedDrop)
	return true
}

func (g *defaultTraceFragmentGuard) snapshotConfirmedDrops() []traceFragmentGuardConfirmedDrop {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return append([]traceFragmentGuardConfirmedDrop(nil), g.confirmedDrops...)
}

var _ traceFragmentGuard = (*defaultTraceFragmentGuard)(nil)
