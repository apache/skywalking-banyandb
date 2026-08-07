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

package sourcecatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type coreScan struct {
	digests  digestSet
	metadata map[uint64]dumptrace.PartMetadata
	rows     map[string]map[uint64]uint64
	parts    []uint64
}

// Build validates a source shard and writes deterministic per-trace ledgers.
func Build(ctx context.Context, options Options) (catalogResult *Catalog, buildErr error) {
	paths, pathErr := validatePaths(options.SourcePath, options.OutputPath)
	if pathErr != nil {
		return nil, fmt.Errorf("cannot validate source catalog paths: %w", pathErr)
	}
	before, manifestErr := benchmark.TreeManifest(paths.source)
	if manifestErr != nil {
		return nil, fmt.Errorf("cannot read source manifest: %w", manifestErr)
	}
	if before.SHA256 != options.Expectations.ManifestSHA256 {
		return nil, fmt.Errorf("source manifest mismatch: got %s, want %s", before.SHA256, options.Expectations.ManifestSHA256)
	}

	fileSystem := fs.NewLocalFileSystem()
	core, coreErr := scanCore(ctx, paths.source, fileSystem, options.Format)
	if coreErr != nil {
		return nil, fmt.Errorf("cannot scan source core: %w", coreErr)
	}
	coreCatalog, validateCoreErr := validateCore(core, options.Expectations)
	if validateCoreErr != nil {
		return nil, fmt.Errorf("cannot validate source core: %w", validateCoreErr)
	}
	small, smallErr := buildPopulation("small", core, options.Expectations.Small)
	if smallErr != nil {
		return nil, fmt.Errorf("cannot build small source population: %w", smallErr)
	}
	mature, matureErr := buildPopulation("mature", core, options.Expectations.Mature)
	if matureErr != nil {
		return nil, fmt.Errorf("cannot build mature source population: %w", matureErr)
	}
	if overlapErr := validateDisjointPopulations("small", small, "mature", mature); overlapErr != nil {
		return nil, fmt.Errorf("cannot validate source population separation: %w", overlapErr)
	}

	if mkdirErr := os.MkdirAll(filepath.Dir(paths.output), 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("cannot create catalog parent: %w", mkdirErr)
	}
	temporary, tempErr := os.MkdirTemp(filepath.Dir(paths.output), ".source-catalog-")
	if tempErr != nil {
		return nil, fmt.Errorf("cannot create temporary catalog: %w", tempErr)
	}
	keepTemporary := false
	defer func() {
		if !keepTemporary {
			if cleanupErr := os.RemoveAll(temporary); cleanupErr != nil {
				buildErr = errors.Join(buildErr, fmt.Errorf("cannot remove temporary catalog %q: %w", temporary, cleanupErr))
			}
		}
	}()

	ledgers := make(map[string]LedgerCatalog, len(options.Expectations.Indexes)+1)
	coreLedger, coreLedgerErr := writeLedger(filepath.Join(temporary, "core-ledger.jsonl"), core.digests)
	if coreLedgerErr != nil {
		return nil, fmt.Errorf("cannot write core ledger: %w", coreLedgerErr)
	}
	ledgers["core"] = coreLedger
	coreCatalog.LogicalChecksum = coreLedger.LogicalChecksum

	indexes := make(map[string]IndexCatalog, len(options.Expectations.Indexes))
	indexNames := make([]string, 0, len(options.Expectations.Indexes))
	for indexName := range options.Expectations.Indexes {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)
	for _, indexName := range indexNames {
		indexCatalog, indexDigests, indexErr := scanAndValidateIndex(ctx, paths.source, fileSystem, indexName, core, options.Expectations.Indexes[indexName])
		if indexErr != nil {
			return nil, fmt.Errorf("cannot catalog secondary index %q: %w", indexName, indexErr)
		}
		ledgerName := fmt.Sprintf("sidx-%s-ledger.jsonl", indexName)
		indexLedger, ledgerErr := writeLedger(filepath.Join(temporary, ledgerName), indexDigests)
		if ledgerErr != nil {
			return nil, fmt.Errorf("cannot write secondary-index ledger %q: %w", indexName, ledgerErr)
		}
		indexCatalog.LogicalChecksum = indexLedger.LogicalChecksum
		indexes[indexName] = indexCatalog
		ledgers["sidx-"+indexName] = indexLedger
	}

	catalog := &Catalog{
		Version:              catalogVersion,
		SourceManifestSHA256: before.SHA256,
		SourceFiles:          before.Files,
		SourceBytes:          before.Bytes,
		Core:                 coreCatalog,
		Indexes:              indexes,
		Small:                small,
		Mature:               mature,
		Ledgers:              ledgers,
	}
	if writeErr := writeCatalog(filepath.Join(temporary, "catalog.json"), catalog); writeErr != nil {
		return nil, fmt.Errorf("cannot write catalog summary: %w", writeErr)
	}
	after, afterErr := benchmark.TreeManifest(paths.source)
	if afterErr != nil {
		return nil, fmt.Errorf("cannot re-read source manifest: %w", afterErr)
	}
	if after != before {
		return nil, fmt.Errorf("source manifest changed during catalog build: before=%s after=%s", before.SHA256, after.SHA256)
	}
	if renameErr := os.Rename(temporary, paths.output); renameErr != nil {
		return nil, fmt.Errorf("cannot publish catalog %q: %w", paths.output, renameErr)
	}
	keepTemporary = true
	return catalog, nil
}

type validatedPaths struct {
	source string
	output string
}

func validatePaths(source, output string) (validatedPaths, error) {
	if source == "" || output == "" {
		return validatedPaths{}, fmt.Errorf("source and output paths are required")
	}
	absoluteSource, sourceErr := filepath.Abs(source)
	if sourceErr != nil {
		return validatedPaths{}, fmt.Errorf("cannot resolve source path: %w", sourceErr)
	}
	absoluteOutput, outputErr := filepath.Abs(output)
	if outputErr != nil {
		return validatedPaths{}, fmt.Errorf("cannot resolve output path: %w", outputErr)
	}
	relativeOutput, relativeErr := filepath.Rel(absoluteSource, absoluteOutput)
	if relativeErr != nil {
		return validatedPaths{}, fmt.Errorf("cannot compare source and output paths: %w", relativeErr)
	}
	if relativeOutput == "." || relativeOutput != ".." && !strings.HasPrefix(relativeOutput, ".."+string(filepath.Separator)) {
		return validatedPaths{}, fmt.Errorf("catalog output must be outside the immutable source")
	}
	if _, statErr := os.Stat(absoluteOutput); statErr == nil {
		return validatedPaths{}, fmt.Errorf("catalog output already exists: %s", absoluteOutput)
	} else if !os.IsNotExist(statErr) {
		return validatedPaths{}, fmt.Errorf("cannot inspect catalog output: %w", statErr)
	}
	return validatedPaths{source: absoluteSource, output: absoluteOutput}, nil
}

func scanCore(ctx context.Context, root string, fileSystem fs.FileSystem, format dumptrace.PartFormat) (*coreScan, error) {
	partIDs, discoverErr := dump.DiscoverPartIDs(root)
	if discoverErr != nil {
		return nil, fmt.Errorf("cannot discover core parts: %w", discoverErr)
	}
	result := &coreScan{
		digests:  make(digestSet),
		metadata: make(map[uint64]dumptrace.PartMetadata, len(partIDs)),
		rows:     make(map[string]map[uint64]uint64),
		parts:    partIDs,
	}
	for _, partID := range partIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("core source scan canceled before part %016x: %w", partID, contextErr)
		}
		partReader, openErr := dumptrace.OpenPartWithFormat(partID, root, fileSystem, format)
		if openErr != nil {
			return nil, fmt.Errorf("cannot open core part %016x: %w", partID, openErr)
		}
		metadata := partReader.Metadata()
		result.metadata[partID] = metadata
		iterator := partReader.Iterator()
		var rows uint64
		for iterator.Next() {
			row := iterator.Row()
			result.digests.add(row.TraceID, partID, hashCoreRow(row))
			partRows := result.rows[row.TraceID]
			if partRows == nil {
				partRows = make(map[uint64]uint64)
				result.rows[row.TraceID] = partRows
			}
			partRows[partID]++
			rows++
		}
		partScanErr := errors.Join(iterator.Err(), iterator.Close(), partReader.Close())
		if partScanErr != nil {
			return nil, fmt.Errorf("cannot scan or close core part %016x: %w", partID, partScanErr)
		}
		if rows != metadata.TotalCount {
			return nil, fmt.Errorf("core part %016x row count mismatch: scanned %d, metadata %d", partID, rows, metadata.TotalCount)
		}
	}
	return result, nil
}

func validateCore(core *coreScan, expected Expectations) (CoreCatalog, error) {
	result := CoreCatalog{PartCount: uint64(len(core.parts)), TraceCount: uint64(len(core.rows))}
	for _, metadata := range core.metadata {
		result.RowCount += metadata.TotalCount
		result.BlockCount += metadata.BlocksCount
		result.CompressedBytes += metadata.CompressedSizeBytes
	}
	if result.PartCount != expected.PartCount || result.TraceCount != expected.TraceCount || result.RowCount != expected.RowCount ||
		result.CompressedBytes != expected.CoreBytes {
		return CoreCatalog{}, fmt.Errorf("core population mismatch: got parts=%d traces=%d rows=%d bytes=%d, want parts=%d traces=%d rows=%d bytes=%d",
			result.PartCount, result.TraceCount, result.RowCount, result.CompressedBytes,
			expected.PartCount, expected.TraceCount, expected.RowCount, expected.CoreBytes)
	}
	return result, nil
}

func buildPopulation(name string, core *coreScan, expected ExpectedPopulation) (PopulationCatalog, error) {
	selectedParts := make(map[uint64]struct{}, len(expected.PartIDs))
	result := PopulationCatalog{PartIDs: make([]string, 0, len(expected.PartIDs))}
	for _, partID := range expected.PartIDs {
		metadata, found := core.metadata[partID]
		if !found {
			return PopulationCatalog{}, fmt.Errorf("%s population part %016x is missing", name, partID)
		}
		selectedParts[partID] = struct{}{}
		result.PartIDs = append(result.PartIDs, formatPartID(partID))
		result.PartTemplates = append(result.PartTemplates, PartTemplate{
			PartID:                formatPartID(partID),
			Blocks:                metadata.BlocksCount,
			Rows:                  metadata.TotalCount,
			CompressedCoreBytes:   metadata.CompressedSizeBytes,
			UncompressedSpanBytes: metadata.UncompressedSpanSizeBytes,
		})
		result.RowCount += metadata.TotalCount
		result.BlockCount += metadata.BlocksCount
		result.CompressedBytes += metadata.CompressedSizeBytes
	}
	sort.Strings(result.PartIDs)
	sort.Slice(result.PartTemplates, func(leftIdx, rightIdx int) bool {
		return result.PartTemplates[leftIdx].PartID < result.PartTemplates[rightIdx].PartID
	})
	for traceID, partRows := range core.rows {
		for partID := range partRows {
			if _, selected := selectedParts[partID]; selected {
				result.TraceIDs = append(result.TraceIDs, traceID)
				break
			}
		}
	}
	sort.Strings(result.TraceIDs)
	result.TraceCount = uint64(len(result.TraceIDs))
	carrierTraces := make(map[uint64]map[string]uint64)
	for _, traceID := range result.TraceIDs {
		for partID, rows := range core.rows[traceID] {
			if _, selected := selectedParts[partID]; selected {
				continue
			}
			traces := carrierTraces[partID]
			if traces == nil {
				traces = make(map[string]uint64)
				carrierTraces[partID] = traces
			}
			traces[traceID] += rows
		}
	}
	closureSet := make(map[string]struct{})
	carrierIDs := make([]uint64, 0, len(carrierTraces))
	for partID := range carrierTraces {
		carrierIDs = append(carrierIDs, partID)
	}
	sort.Slice(carrierIDs, func(leftIdx, rightIdx int) bool { return carrierIDs[leftIdx] < carrierIDs[rightIdx] })
	for _, partID := range carrierIDs {
		traceRows := carrierTraces[partID]
		traceIDs := make([]string, 0, len(traceRows))
		var rowCount uint64
		for traceID, rows := range traceRows {
			traceIDs = append(traceIDs, traceID)
			closureSet[traceID] = struct{}{}
			rowCount += rows
		}
		sort.Strings(traceIDs)
		result.Carriers = append(result.Carriers, CarrierCatalog{
			PartID:     formatPartID(partID),
			TraceIDs:   traceIDs,
			TraceCount: uint64(len(traceIDs)),
			RowCount:   rowCount,
		})
	}
	for traceID := range closureSet {
		result.ClosureTraceIDs = append(result.ClosureTraceIDs, traceID)
	}
	sort.Strings(result.ClosureTraceIDs)
	if populationErr := validatePopulation(name, result, expected); populationErr != nil {
		return PopulationCatalog{}, fmt.Errorf("cannot validate %s source population: %w", name, populationErr)
	}
	return result, nil
}

func validateDisjointPopulations(leftName string, left PopulationCatalog, rightName string, right PopulationCatalog) error {
	leftIdx, rightIdx := 0, 0
	for leftIdx < len(left.TraceIDs) && rightIdx < len(right.TraceIDs) {
		switch {
		case left.TraceIDs[leftIdx] < right.TraceIDs[rightIdx]:
			leftIdx++
		case left.TraceIDs[leftIdx] > right.TraceIDs[rightIdx]:
			rightIdx++
		default:
			return fmt.Errorf("%s and %s source catalogs overlap at trace %q", leftName, rightName, left.TraceIDs[leftIdx])
		}
	}
	return nil
}

func validatePopulation(name string, actual PopulationCatalog, expected ExpectedPopulation) error {
	if actual.TraceCount != expected.TraceCount || actual.RowCount != expected.RowCount || actual.BlockCount != expected.BlockCount ||
		actual.CompressedBytes != expected.CoreBytes {
		return fmt.Errorf("%s population mismatch: got traces=%d rows=%d blocks=%d bytes=%d, want traces=%d rows=%d blocks=%d bytes=%d", name,
			actual.TraceCount, actual.RowCount, actual.BlockCount, actual.CompressedBytes,
			expected.TraceCount, expected.RowCount, expected.BlockCount, expected.CoreBytes)
	}
	if len(actual.Carriers) != len(expected.Carriers) {
		return fmt.Errorf("%s closure carrier count mismatch: got %d, want %d", name, len(actual.Carriers), len(expected.Carriers))
	}
	for _, carrier := range actual.Carriers {
		partID, parseErr := parsePartID(carrier.PartID)
		if parseErr != nil {
			return fmt.Errorf("cannot validate %s carrier: %w", name, parseErr)
		}
		expectedCarrier, found := expected.Carriers[partID]
		if !found || carrier.TraceCount != expectedCarrier.TraceCount || carrier.RowCount != expectedCarrier.RowCount {
			return fmt.Errorf("%s carrier %s mismatch: got traces=%d rows=%d", name, carrier.PartID, carrier.TraceCount, carrier.RowCount)
		}
	}
	return nil
}

func scanAndValidateIndex(ctx context.Context, source string, fileSystem fs.FileSystem, name string,
	core *coreScan, expected ExpectedIndex,
) (IndexCatalog, digestSet, error) {
	indexRoot := filepath.Join(source, "sidx", name)
	manifest, manifestErr := benchmark.TreeManifest(indexRoot)
	if manifestErr != nil {
		return IndexCatalog{}, nil, fmt.Errorf("cannot manifest secondary index %q: %w", name, manifestErr)
	}
	partIDs, discoverErr := dump.DiscoverPartIDs(indexRoot)
	if discoverErr != nil {
		return IndexCatalog{}, nil, fmt.Errorf("cannot discover secondary index %q parts: %w", name, discoverErr)
	}
	digests := make(digestSet)
	scanErr := sidx.ScanRawParts(ctx, fileSystem, indexRoot, partIDs, func(row sidx.RawRow) error {
		if len(row.Data) < 2 || row.Data[0] != 1 {
			return fmt.Errorf("unsupported trace ID encoding")
		}
		traceID := string(row.Data[1:])
		digests.add(traceID, row.PartID, hashIndexRow(traceID, row))
		return nil
	})
	if scanErr != nil {
		return IndexCatalog{}, nil, fmt.Errorf("cannot scan secondary index %q: %w", name, scanErr)
	}
	result := IndexCatalog{PartCount: uint64(len(partIDs)), Bytes: manifest.Bytes}
	for _, traceData := range digests {
		result.RowCount += uint64(len(traceData.rows))
	}
	if result.PartCount != expected.PartCount || result.RowCount != expected.RowCount || result.Bytes != expected.Bytes {
		return IndexCatalog{}, nil, fmt.Errorf("secondary index %q population mismatch: got parts=%d rows=%d bytes=%d, want parts=%d rows=%d bytes=%d",
			name, result.PartCount, result.RowCount, result.Bytes, expected.PartCount, expected.RowCount, expected.Bytes)
	}
	if reconcileErr := reconcileIndex(name, core.digests, digests); reconcileErr != nil {
		return IndexCatalog{}, nil, reconcileErr
	}
	return result, digests, nil
}

func reconcileIndex(name string, core, index digestSet) error {
	if len(core) != len(index) {
		return fmt.Errorf("secondary index %q trace count mismatch: got %d, want %d", name, len(index), len(core))
	}
	for traceID, coreTrace := range core {
		indexTrace := index[traceID]
		if indexTrace == nil {
			return fmt.Errorf("secondary index %q is missing trace %q", name, traceID)
		}
		if len(indexTrace.rows) != len(coreTrace.rows) {
			return fmt.Errorf("secondary index %q row count mismatch for trace %q: got %d, want %d", name, traceID, len(indexTrace.rows), len(coreTrace.rows))
		}
		if len(indexTrace.parts) != len(coreTrace.parts) {
			return fmt.Errorf("secondary index %q part count mismatch for trace %q: got %d, want %d", name, traceID, len(indexTrace.parts), len(coreTrace.parts))
		}
		for partID, corePart := range coreTrace.parts {
			indexPart := indexTrace.parts[partID]
			if indexPart == nil || len(indexPart.rows) != len(corePart.rows) {
				indexRows := 0
				if indexPart != nil {
					indexRows = len(indexPart.rows)
				}
				return fmt.Errorf("secondary index %q part %016x row count mismatch for trace %q: got %d, want %d",
					name, partID, traceID, indexRows, len(corePart.rows))
			}
		}
	}
	return nil
}

func writeCatalog(path string, catalog *Catalog) error {
	data, marshalErr := json.MarshalIndent(catalog, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("cannot marshal source catalog: %w", marshalErr)
	}
	data = append(data, '\n')
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		return fmt.Errorf("cannot write source catalog: %w", writeErr)
	}
	return nil
}

func formatPartID(partID uint64) string {
	return fmt.Sprintf("%016x", partID)
}

func parsePartID(value string) (uint64, error) {
	var result uint64
	if _, scanErr := fmt.Sscanf(value, "%x", &result); scanErr != nil {
		return 0, fmt.Errorf("cannot parse part ID %q: %w", value, scanErr)
	}
	return result, nil
}
