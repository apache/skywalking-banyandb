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
	"time"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	traceFragmentProbeBudgetBytes = 16
	traceFragmentTokenBudgetBytes = 64
)

type traceFragmentGuardSnapshotPin struct {
	snapshot *snapshot
}

func (p *traceFragmentGuardSnapshotPin) Release() {
	if p.snapshot == nil {
		return
	}
	p.snapshot.decRef()
	p.snapshot = nil
}

type traceFragmentGuardPartFilter struct {
	part *part
}

func (f traceFragmentGuardPartFilter) Lookup(traceID string) (traceFragmentMembership, error) {
	if f.part == nil || f.part.traceIDFilter.filter == nil {
		return traceFragmentMembershipUnknown, nil
	}
	if f.part.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
		return traceFragmentMembershipMaybePresent, nil
	}
	return traceFragmentMembershipAbsent, nil
}

type traceFragmentGuardSession struct {
	guard            traceFragmentGuard
	owner            *tsTable
	ownerClose       <-chan struct{}
	baseParts        map[uint64]*partWrapper
	selectedParts    map[uint64]*partWrapper
	selectedInFlight map[uint64]struct{}
}

func (s *traceFragmentGuardSession) Close() {
	if s == nil || s.guard == nil {
		return
	}
	s.guard.Close()
	s.guard = nil
	s.owner = nil
	s.ownerClose = nil
	s.baseParts = nil
	s.selectedParts = nil
	s.selectedInFlight = nil
}

func (tst *tsTable) newTraceFragmentGuardSession(selected []*partWrapper, grace time.Duration, stageBudget uint64) *traceFragmentGuardSession {
	baseSnapshot := tst.currentSnapshot()
	session := &traceFragmentGuardSession{
		owner:            tst,
		baseParts:        make(map[uint64]*partWrapper),
		selectedParts:    make(map[uint64]*partWrapper, len(selected)),
		selectedInFlight: make(map[uint64]struct{}),
	}
	if tst.loopCloser != nil {
		session.ownerClose = tst.loopCloser.CloseNotify()
	}
	selectedComplete := len(selected) > 0
	for selectedIdx := range selected {
		selectedPart := selected[selectedIdx]
		if selectedPart == nil || selectedPart.p == nil {
			selectedComplete = false
			continue
		}
		selectedID := selectedPart.ID()
		if _, duplicated := session.selectedParts[selectedID]; duplicated ||
			!traceFragmentPartFromWrapper(selectedPart).BoundsKnown {
			selectedComplete = false
		}
		session.selectedParts[selectedID] = selectedPart
	}
	tst.inFlightMu.RLock()
	for selectedID := range session.selectedParts {
		if _, inFlight := tst.inFlight[selectedID]; inFlight {
			session.selectedInFlight[selectedID] = struct{}{}
		}
	}
	tst.inFlightMu.RUnlock()

	catalog := traceFragmentGuardCatalog{}
	if baseSnapshot != nil {
		catalog.Pin = &traceFragmentGuardSnapshotPin{snapshot: baseSnapshot}
		catalog.BaseEpoch = baseSnapshot.epoch
		catalog.Complete = selectedComplete
		for partIdx := range baseSnapshot.parts {
			partData := baseSnapshot.parts[partIdx]
			if partData == nil || partData.p == nil {
				catalog.Complete = false
				continue
			}
			partID := partData.ID()
			if _, duplicated := session.baseParts[partID]; duplicated {
				catalog.Complete = false
			}
			session.baseParts[partID] = partData
		}
		for selectedID, selectedPart := range session.selectedParts {
			if session.baseParts[selectedID] != selectedPart {
				catalog.Complete = false
			}
		}
		for partIdx := range baseSnapshot.parts {
			partData := baseSnapshot.parts[partIdx]
			if partData == nil || partData.p == nil || partData.p.partMetadata.TotalCount == 0 {
				continue
			}
			if session.selectedParts[partData.ID()] == partData {
				continue
			}
			catalog.OutsideParts = append(catalog.OutsideParts, traceFragmentPartFromWrapper(partData))
		}
	}
	catalog.CoverageMinTimestamp, catalog.CoverageMaxTimestamp, catalog.CoverageKnown = traceFragmentCoverage(tst.segmentTimeRange)
	if tst.option.maxTraceFragmentGap > 0 {
		catalog.EnforcedMaxFragmentGap = tst.option.maxTraceFragmentGap
		catalog.TemporalSafety = traceFragmentTemporalSafetyMaxGapEnforced
	} else {
		catalog.TemporalSafety = traceFragmentTemporalSafetyUnknown
	}
	if catalog.Pin == nil || !catalog.Complete || !catalog.CoverageKnown ||
		!traceFragmentCoverageHasInterior(catalog.CoverageMinTimestamp, catalog.CoverageMaxTimestamp, grace) ||
		traceFragmentPartsValidationReason(catalog.OutsideParts) != "" {
		if catalog.Pin != nil {
			catalog.Pin.Release()
		}
		return nil
	}

	guardBudget := max(stageBudget, uint64(defaultStageBudgetFloor))
	session.guard = newTraceFragmentGuard(traceFragmentGuardConfig{
		Grace:             grace,
		MaxBloomProbes:    traceFragmentBudgetCount(guardBudget, traceFragmentProbeBudgetBytes),
		MaxConfirmedDrops: traceFragmentBudgetCount(guardBudget, traceFragmentTokenBudgetBytes),
	}, catalog)
	return session
}

func traceFragmentPartFromWrapper(partData *partWrapper) traceFragmentGuardPart {
	if partData == nil || partData.p == nil {
		return traceFragmentGuardPart{}
	}
	metadata := &partData.p.partMetadata
	guardPart := traceFragmentGuardPart{
		ID:           partData.ID(),
		MinTimestamp: metadata.MinTimestamp,
		MaxTimestamp: metadata.MaxTimestamp,
		BoundsKnown:  metadata.TotalCount > 0 && metadata.MinTimestamp <= metadata.MaxTimestamp,
	}
	if partData.p.traceIDFilter.filter != nil {
		guardPart.Filter = traceFragmentGuardPartFilter{part: partData.p}
	}
	return guardPart
}

func traceFragmentCoverage(segmentTimeRange timestamp.TimeRange) (int64, int64, bool) {
	if segmentTimeRange.Start.IsZero() || segmentTimeRange.End.IsZero() ||
		!segmentTimeRange.Start.Before(segmentTimeRange.End) {
		return 0, 0, false
	}
	minTimestamp := segmentTimeRange.Start.UnixNano()
	maxTimestamp := segmentTimeRange.End.UnixNano()
	if !segmentTimeRange.IncludeStart {
		minTimestamp = traceFragmentSaturatingAdd(minTimestamp, 1)
	}
	if !segmentTimeRange.IncludeEnd {
		maxTimestamp = traceFragmentSaturatingSub(maxTimestamp, 1)
	}
	return minTimestamp, maxTimestamp, minTimestamp <= maxTimestamp
}

func traceFragmentCoverageHasInterior(minTimestamp, maxTimestamp int64, grace time.Duration) bool {
	if grace < 0 || minTimestamp > maxTimestamp {
		return false
	}
	interiorMin := traceFragmentSaturatingAdd(minTimestamp, int64(grace))
	interiorMax := traceFragmentSaturatingSub(maxTimestamp, int64(grace))
	return interiorMin <= interiorMax
}

func traceFragmentBudgetCount(byteBudget uint64, bytesPerItem uint64) int {
	if bytesPerItem == 0 {
		return 0
	}
	itemBudget := byteBudget / bytesPerItem
	if itemBudget == 0 {
		return 1
	}
	if itemBudget > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(itemBudget)
}

func (s *traceFragmentGuardSession) revalidate(tst *tsTable) traceFragmentGuardRevalidation {
	if s == nil || s.guard == nil {
		return traceFragmentGuardRevalidation{
			Reason: traceFragmentGuardReasonCatalogUnpinned,
		}
	}
	currentSnapshot := tst.currentSnapshot()
	if currentSnapshot == nil {
		return s.guard.RevalidateDrops(context.Background(), traceFragmentGuardRevalidationRequest{})
	}
	defer currentSnapshot.decRef()

	currentParts := make(map[uint64]*partWrapper, len(currentSnapshot.parts))
	deltaParts := make([]traceFragmentGuardPart, 0)
	deltaComplete := true
	for partIdx := range currentSnapshot.parts {
		partData := currentSnapshot.parts[partIdx]
		if partData == nil || partData.p == nil {
			deltaComplete = false
			continue
		}
		partID := partData.ID()
		if _, duplicated := currentParts[partID]; duplicated {
			deltaComplete = false
		}
		currentParts[partID] = partData
		if partData.p.partMetadata.TotalCount == 0 || s.baseParts[partID] == partData {
			continue
		}
		deltaParts = append(deltaParts, traceFragmentPartFromWrapper(partData))
	}

	selectedUnchanged := true
	tst.inFlightMu.RLock()
	for selectedID, selectedPart := range s.selectedParts {
		if currentParts[selectedID] != selectedPart {
			selectedUnchanged = false
			break
		}
		if _, expectedInFlight := s.selectedInFlight[selectedID]; expectedInFlight {
			if _, inFlight := tst.inFlight[selectedID]; !inFlight {
				selectedUnchanged = false
				break
			}
		}
	}
	tst.inFlightMu.RUnlock()

	ownershipUnchanged := s.ownershipUnchanged(tst)
	revalidationContext := context.Background()
	if ownershipUnchanged {
		revalidationContext = tst.loopCloser.Ctx()
		select {
		case <-s.ownerClose:
			ownershipUnchanged = false
		default:
		}
	}
	return s.guard.RevalidateDrops(revalidationContext, traceFragmentGuardRevalidationRequest{
		DeltaParts:              deltaParts,
		CurrentEpoch:            currentSnapshot.epoch,
		DeltaCatalogComplete:    deltaComplete,
		OwnershipUnchanged:      ownershipUnchanged,
		SelectedInputsUnchanged: selectedUnchanged,
		PublicationFenceHeld:    true,
	})
}

func (s *traceFragmentGuardSession) ownershipUnchanged(tst *tsTable) bool {
	if s == nil || tst == nil || tst != s.owner || tst.loopCloser == nil || tst.loopCloser.CloseNotify() != s.ownerClose {
		return false
	}
	select {
	case <-s.ownerClose:
		return false
	default:
		return true
	}
}
