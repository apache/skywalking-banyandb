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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracefixture"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

// ControlledSeedCaptureOptions configures untimed discovery of one production-shaped controlled merge seed.
type ControlledSeedCaptureOptions struct {
	SourceRoot    string
	DataRoot      string
	SchedulePath  string
	OutputRoot    string
	MinInputRows  uint64
	MinInputDepth uint32
	MinInputParts int
}

// CaptureControlledMergeSeed replays writes one merge at a time and freezes the first qualifying pre-dispatch snapshot.
func CaptureControlledMergeSeed(ctx context.Context, options ControlledSeedCaptureOptions) (manifest ControlledMergeSeedManifest, captureErr error) {
	if options.MinInputRows == 0 {
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled seed minimum input rows must be positive")
	}
	if options.MinInputParts < 2 {
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled seed minimum input parts must be at least two")
	}
	if _, statErr := os.Stat(options.OutputRoot); !errors.Is(statErr, os.ErrNotExist) {
		if statErr != nil {
			return ControlledMergeSeedManifest{}, fmt.Errorf("cannot inspect controlled seed output %q: %w", options.OutputRoot, statErr)
		}
		return ControlledMergeSeedManifest{}, fmt.Errorf("controlled seed output %q already exists", options.OutputRoot)
	}
	scheduleData, readErr := os.ReadFile(options.SchedulePath)
	if readErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot read controlled seed schedule: %w", readErr)
	}
	var schedule scheduleDocument
	if decodeErr := json.Unmarshal(scheduleData, &schedule); decodeErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot decode controlled seed schedule: %w", decodeErr)
	}
	for _, indexName := range []string{"latency", "start_time"} {
		if mkdirErr := os.MkdirAll(filepath.Join(options.DataRoot, "sidx", indexName), 0o755); mkdirErr != nil {
			return ControlledMergeSeedManifest{}, fmt.Errorf("cannot create controlled seed index root %q: %w", indexName, mkdirErr)
		}
	}
	receiver, receiverErr := storagetrace.NewBenchmarkMergeReceiver( //nolint:contextcheck // The storage constructor has no context parameter.
		options.DataRoot, storagetrace.BenchmarkMergeReceiverOptions{
			LogicalNow: schedule.DayStart, MergeGrace: storagetrace.BenchmarkDefaultMergeGrace, MaxInputPartID: uint64(len(schedule.Writes)), Attribution: true,
			BlockMerges: true,
		})
	if receiverErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot open controlled seed receiver: %w", receiverErr)
	}
	defer func() {
		if closeErr := receiver.Close(); closeErr != nil {
			captureErr = errors.Join(captureErr, fmt.Errorf("cannot close controlled seed receiver: %w", closeErr))
		}
	}()
	mergeCount := 0
	for writeIdx := range schedule.Writes {
		if contextErr := ctx.Err(); contextErr != nil {
			return ControlledMergeSeedManifest{}, fmt.Errorf("controlled seed capture canceled: %w", contextErr)
		}
		write := &schedule.Writes[writeIdx]
		partID, parseErr := strconv.ParseUint(write.PartID, 16, 64)
		if parseErr != nil {
			return ControlledMergeSeedManifest{}, fmt.Errorf("cannot parse controlled seed part ID %q: %w", write.PartID, parseErr)
		}
		driverOptions := DriverOptions{SourceRoot: options.SourceRoot, DataRoot: options.DataRoot}
		if publishErr := publishPart(driverOptions, write); publishErr != nil {
			return ControlledMergeSeedManifest{}, publishErr
		}
		partName := fmt.Sprintf("%016x", partID)
		indexPaths := map[string]string{
			"latency":    filepath.Join(options.DataRoot, "sidx", "latency", partName),
			"start_time": filepath.Join(options.DataRoot, "sidx", "start_time", partName),
		}
		if receiveErr := receiver.PublishExistingPart(partID, filepath.Join(options.DataRoot, partName), indexPaths, write.Publication); receiveErr != nil {
			return ControlledMergeSeedManifest{}, fmt.Errorf("cannot introduce controlled seed part %s: %w", partName, receiveErr)
		}
		for {
			selection, selectionErr := receiver.PreviewMergeSelection()
			if errors.Is(selectionErr, storagetrace.ErrBenchmarkNoMergeSelection) {
				break
			}
			if selectionErr != nil {
				return ControlledMergeSeedManifest{}, fmt.Errorf("cannot preview controlled seed selection: %w", selectionErr)
			}
			if qualifiesControlledSeed(selection, options) {
				logicalLedger, ledgerErr := tracefixture.LogicalLedgerChecksums(ctx, receiver)
				if ledgerErr != nil {
					return ControlledMergeSeedManifest{}, fmt.Errorf("cannot checksum controlled seed ledgers: %w", ledgerErr)
				}
				partDepths, depthErr := receiver.MergePartDepths()
				if depthErr != nil {
					return ControlledMergeSeedManifest{}, fmt.Errorf("cannot read controlled seed merge depths: %w", depthErr)
				}
				snapshotRoot := filepath.Join(options.OutputRoot, "shard")
				activePartIDs, activePartsErr := receiver.ActivePartIDs()
				if activePartsErr != nil {
					return ControlledMergeSeedManifest{}, fmt.Errorf("cannot list controlled seed active parts: %w", activePartsErr)
				}
				if copyErr := copyControlledSnapshot(options.DataRoot, snapshotRoot, activePartIDs); copyErr != nil {
					return ControlledMergeSeedManifest{}, fmt.Errorf("cannot copy controlled seed snapshot: %w", copyErr)
				}
				manifest, buildErr := BuildControlledMergeSeedManifest(
					snapshotRoot, selection, storagetrace.BenchmarkDefaultMergeGrace, logicalLedger, partDepths,
				)
				if buildErr != nil {
					return ControlledMergeSeedManifest{}, buildErr
				}
				manifest.DiscoveryLogicalNow = write.Publication
				manifest.DiscoveryMergeCount = mergeCount
				manifest.PublishedParts = writeIdx + 1
				if writeErr := writeControlledSeedManifest(options.OutputRoot, manifest); writeErr != nil {
					return ControlledMergeSeedManifest{}, writeErr
				}
				return manifest, nil
			}
			if _, mergeErr := receiver.RunOneMerge(ctx, storagetrace.BenchmarkOneMergeOptions{
				LogicalNow: write.Publication, ExpectedSelectionSHA256: selection.SelectionSHA256,
			}); mergeErr != nil {
				return ControlledMergeSeedManifest{}, fmt.Errorf("cannot replay controlled seed merge %d: %w", mergeCount+1, mergeErr)
			}
			mergeCount++
		}
	}
	return ControlledMergeSeedManifest{}, fmt.Errorf("no controlled merge seed matched depth >= %d, parts >= %d, and rows >= %d",
		options.MinInputDepth, options.MinInputParts, options.MinInputRows)
}

func copyControlledSnapshot(sourceRoot, destinationRoot string, partIDs []uint64) error {
	for _, indexName := range []string{"latency", "start_time"} {
		if mkdirErr := os.MkdirAll(filepath.Join(destinationRoot, "sidx", indexName), 0o755); mkdirErr != nil {
			return fmt.Errorf("cannot create copied index root %q: %w", indexName, mkdirErr)
		}
	}
	for _, partID := range partIDs {
		partName := fmt.Sprintf("%016x", partID)
		if copyErr := os.CopyFS(filepath.Join(destinationRoot, partName), os.DirFS(filepath.Join(sourceRoot, partName))); copyErr != nil {
			return fmt.Errorf("cannot copy active core part %s: %w", partName, copyErr)
		}
		for _, indexName := range []string{"latency", "start_time"} {
			sourcePath := filepath.Join(sourceRoot, "sidx", indexName, partName)
			destinationPath := filepath.Join(destinationRoot, "sidx", indexName, partName)
			if copyErr := os.CopyFS(destinationPath, os.DirFS(sourcePath)); copyErr != nil {
				return fmt.Errorf("cannot copy active index part %s/%s: %w", indexName, partName, copyErr)
			}
		}
	}
	rootEntries, readErr := os.ReadDir(sourceRoot)
	if readErr != nil {
		return fmt.Errorf("cannot list controlled snapshot files: %w", readErr)
	}
	var snapshotNames []string
	for _, rootEntry := range rootEntries {
		if rootEntry.Type().IsRegular() && strings.HasSuffix(rootEntry.Name(), ".snp") {
			snapshotNames = append(snapshotNames, rootEntry.Name())
		}
	}
	if len(snapshotNames) == 0 {
		return fmt.Errorf("controlled snapshot has no persisted snapshot file")
	}
	sort.Strings(snapshotNames)
	snapshotName := snapshotNames[len(snapshotNames)-1]
	snapshotData, snapshotErr := os.ReadFile(filepath.Join(sourceRoot, snapshotName))
	if snapshotErr != nil {
		return fmt.Errorf("cannot read controlled snapshot %q: %w", snapshotName, snapshotErr)
	}
	if writeErr := os.WriteFile(filepath.Join(destinationRoot, snapshotName), snapshotData, 0o600); writeErr != nil {
		return fmt.Errorf("cannot write controlled snapshot %q: %w", snapshotName, writeErr)
	}
	return nil
}

func qualifiesControlledSeed(selection storagetrace.BenchmarkMergeEvent, options ControlledSeedCaptureOptions) bool {
	return len(selection.InputPartIDs) >= options.MinInputParts && selection.InputRows >= options.MinInputRows &&
		selection.InputMinDepth >= options.MinInputDepth
}

func writeControlledSeedManifest(root string, manifest ControlledMergeSeedManifest) error {
	manifestData, marshalErr := json.MarshalIndent(manifest, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("cannot encode controlled seed manifest: %w", marshalErr)
	}
	manifestData = append(manifestData, '\n')
	if writeErr := os.WriteFile(filepath.Join(root, "seed.json"), manifestData, 0o600); writeErr != nil {
		return fmt.Errorf("cannot write controlled seed manifest: %w", writeErr)
	}
	return nil
}
