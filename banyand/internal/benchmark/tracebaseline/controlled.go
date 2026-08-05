// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

const controlledMergeSeedVersion = 1

// ControlledMergeSelection freezes the production picker's input contract.
type ControlledMergeSelection struct {
	SHA256        string   `json:"sha256"`
	InputPartIDs  []uint64 `json:"inputPartIDs"`
	InputBytes    uint64   `json:"inputBytes"`
	InputRows     uint64   `json:"inputRows"`
	MinTimestamp  int64    `json:"minTimestamp"`
	MaxTimestamp  int64    `json:"maxTimestamp"`
	InputMinDepth uint32   `json:"inputMinDepth"`
	InputMaxDepth uint32   `json:"inputMaxDepth"`
}

// ControlledMergeSeedManifest freezes one production-derived pre-dispatch shard snapshot.
type ControlledMergeSeedManifest struct {
	MatureLogicalNow    time.Time                `json:"matureLogicalNow"`
	DiscoveryLogicalNow time.Time                `json:"discoveryLogicalNow"`
	LogicalLedger       map[string]string        `json:"logicalLedgerSHA256"`
	PartMergeDepths     map[uint64]uint32        `json:"partMergeDepths"`
	Snapshot            benchmark.Manifest       `json:"snapshot"`
	Selection           ControlledMergeSelection `json:"selection"`
	MergeGrace          time.Duration            `json:"mergeGrace"`
	DiscoveryMergeCount int                      `json:"discoveryMergeCount"`
	PublishedParts      int                      `json:"publishedParts"`
	Version             uint32                   `json:"version"`
}

// BuildControlledMergeSeedManifest builds the immutable contract for one controlled merge seed.
func BuildControlledMergeSeedManifest(snapshotRoot string, selection storagetrace.BenchmarkMergeEvent, mergeGrace time.Duration,
	logicalLedger map[string]string, partMergeDepths map[uint64]uint32,
) (ControlledMergeSeedManifest, error) {
	if mergeGrace <= 0 {
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled merge grace must be positive")
	}
	if selectionErr := validateControlledSelection(selection); selectionErr != nil {
		return ControlledMergeSeedManifest{}, selectionErr
	}
	if !validControlledLedger(logicalLedger) {
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled merge logical ledger must contain non-empty checksums")
	}
	if len(partMergeDepths) == 0 {
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled merge part depths are required")
	}
	snapshotManifest, snapshotErr := benchmark.TreeManifest(snapshotRoot)
	if snapshotErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot build controlled merge snapshot manifest: %w", snapshotErr)
	}
	return ControlledMergeSeedManifest{
		Version: controlledMergeSeedVersion, Snapshot: snapshotManifest, MergeGrace: mergeGrace,
		MatureLogicalNow: time.Unix(0, selection.MaxTimestamp).Add(mergeGrace), LogicalLedger: maps.Clone(logicalLedger),
		Selection: controlledSelectionFromEvent(selection), PartMergeDepths: maps.Clone(partMergeDepths),
	}, nil
}

// ValidateControlledMergeSeedManifest verifies a cloned seed before a measured merge starts.
func ValidateControlledMergeSeedManifest(snapshotRoot string, manifest ControlledMergeSeedManifest,
	selection storagetrace.BenchmarkMergeEvent, logicalLedger map[string]string, partMergeDepths map[uint64]uint32,
) error {
	if manifest.Version != controlledMergeSeedVersion {
		return fmt.Errorf("controlled merge seed version %d is not supported", manifest.Version)
	}
	actualSnapshot, snapshotErr := benchmark.TreeManifest(snapshotRoot)
	if snapshotErr != nil {
		return fmt.Errorf("cannot verify controlled merge snapshot manifest: %w", snapshotErr)
	}
	if actualSnapshot != manifest.Snapshot {
		return fmt.Errorf("controlled merge snapshot manifest %+v does not match expected %+v", actualSnapshot, manifest.Snapshot)
	}
	if selectionErr := validateControlledSelection(selection); selectionErr != nil {
		return selectionErr
	}
	actualSelection := controlledSelectionFromEvent(selection)
	if actualSelection.SHA256 != manifest.Selection.SHA256 {
		return fmt.Errorf("controlled merge selection checksum %s does not match expected %s", actualSelection.SHA256, manifest.Selection.SHA256)
	}
	if !slices.Equal(actualSelection.InputPartIDs, manifest.Selection.InputPartIDs) {
		return fmt.Errorf("controlled merge input part IDs %v do not match expected %v", actualSelection.InputPartIDs, manifest.Selection.InputPartIDs)
	}
	if actualSelection.InputBytes != manifest.Selection.InputBytes || actualSelection.InputRows != manifest.Selection.InputRows ||
		actualSelection.MinTimestamp != manifest.Selection.MinTimestamp || actualSelection.MaxTimestamp != manifest.Selection.MaxTimestamp ||
		actualSelection.InputMinDepth != manifest.Selection.InputMinDepth || actualSelection.InputMaxDepth != manifest.Selection.InputMaxDepth {
		return fmt.Errorf("controlled merge selection metadata %+v does not match expected %+v", actualSelection, manifest.Selection)
	}
	if !equalLedgerChecksums(manifest.LogicalLedger, logicalLedger) {
		return fmt.Errorf("controlled merge logical ledger does not match the frozen seed")
	}
	if !maps.Equal(manifest.PartMergeDepths, partMergeDepths) {
		return fmt.Errorf("controlled merge part depths do not match the frozen seed")
	}
	return nil
}

func validControlledLedger(logicalLedger map[string]string) bool {
	if len(logicalLedger) != 3 {
		return false
	}
	for _, ledgerName := range []string{"core", "latency", "start_time"} {
		if logicalLedger[ledgerName] == "" {
			return false
		}
	}
	return true
}

func controlledSelectionFromEvent(event storagetrace.BenchmarkMergeEvent) ControlledMergeSelection {
	return ControlledMergeSelection{
		SHA256: event.SelectionSHA256, InputPartIDs: slices.Clone(event.InputPartIDs), InputBytes: event.InputBytes, InputRows: event.InputRows,
		MinTimestamp: event.MinTimestamp, MaxTimestamp: event.MaxTimestamp, InputMinDepth: event.InputMinDepth, InputMaxDepth: event.InputMaxDepth,
	}
}

func validateControlledSelection(selection storagetrace.BenchmarkMergeEvent) error {
	digest, decodeErr := hex.DecodeString(selection.SelectionSHA256)
	if decodeErr != nil || len(digest) != 32 {
		return fmt.Errorf("controlled merge selection checksum %q is not a SHA-256 digest", selection.SelectionSHA256)
	}
	if len(selection.InputPartIDs) < 2 {
		return fmt.Errorf("controlled merge selection must contain at least two input parts")
	}
	if selection.InputBytes == 0 || selection.InputRows == 0 {
		return fmt.Errorf("controlled merge selection must contain bytes and rows")
	}
	return nil
}
