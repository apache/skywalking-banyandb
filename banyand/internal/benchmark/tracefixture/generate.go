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

package tracefixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	fixtureVersion      = 1
	traceIDEncodingV1   = byte(1)
	defaultMergeGrace   = 2 * time.Hour
	coreSizeTolerance   = 0.02
	indexSizeTolerance  = 0.10
	indexTotalTolerance = 0.05
)

var fixtureIndexNames = [...]string{"latency", "start_time"}

// GenerateOptions configures production-written fixture generation.
type GenerateOptions struct {
	OutputPath  string
	DayStart    time.Time
	DayDuration time.Duration
	MergeGrace  time.Duration
}

// Artifact is the reproducibility and acceptance record for one generated fixture.
type Artifact struct {
	CoreManifest               benchmark.Manifest            `json:"coreManifest"`
	CoreCompressedBytes        uint64                        `json:"coreCompressedBytes"`
	CoreConsolidatedBytes      uint64                        `json:"coreConsolidatedBytes"`
	IndexManifests             map[string]benchmark.Manifest `json:"indexManifests"`
	IndexCompressedBytes       map[string]uint64             `json:"indexCompressedBytes"`
	IndexConsolidatedBytes     map[string]uint64             `json:"indexConsolidatedBytes"`
	SourceIndexCompressedBytes map[string]uint64             `json:"sourceIndexCompressedBytes"`
	SourceManifestSHA256       string                        `json:"sourceManifestSHA256"`
	SourceCatalogSHA256        string                        `json:"sourceCatalogSHA256"`
	ClosureAllowlistSHA256     string                        `json:"closureAllowlistSHA256"`
	CopyAllowlistSHA256        string                        `json:"copyAllowlistSHA256"`
	GeneratedIDManifestSHA256  string                        `json:"generatedIDManifestSHA256"`
	ScheduleSHA256             string                        `json:"scheduleSHA256"`
	DayStart                   time.Time                     `json:"dayStart"`
	DayDuration                time.Duration                 `json:"dayDuration"`
	MergeGrace                 time.Duration                 `json:"mergeGrace"`
	TraceCount                 uint64                        `json:"traceCount"`
	RowCount                   uint64                        `json:"rowCount"`
	WriteCount                 uint64                        `json:"writeCount"`
	PartialTailWrites          uint64                        `json:"partialTailWrites"`
	Version                    int                           `json:"version"`
}

type sourceLookup map[string]LoadedTrace

// Generate writes a planned fixture through the native encoders and production data-node receipt path.
func Generate(ctx context.Context, source Source, plan Plan, options GenerateOptions) (artifact Artifact, generateErr error) {
	if options.OutputPath == "" {
		return Artifact{}, fmt.Errorf("fixture output path is required")
	}
	if options.DayDuration == 0 {
		options.DayDuration = plan.DayDuration
	}
	if options.DayStart.IsZero() {
		options.DayStart = plan.DayStart
	}
	if options.MergeGrace == 0 {
		options.MergeGrace = defaultMergeGrace
	}
	if pathErr := prepareNewOutput(options.OutputPath); pathErr != nil {
		return Artifact{}, fmt.Errorf("cannot prepare fixture output: %w", pathErr)
	}
	keepOutput := false
	defer func() {
		if !keepOutput {
			if cleanupErr := os.RemoveAll(options.OutputPath); cleanupErr != nil {
				generateErr = errors.Join(generateErr, fmt.Errorf("cannot clean failed fixture output %q: %w", options.OutputPath, cleanupErr))
			}
		}
	}()
	receiverRoot := filepath.Join(options.OutputPath, "shard")
	senderRoot := filepath.Join(options.OutputPath, ".sender")
	if mkdirErr := os.MkdirAll(filepath.Join(senderRoot, "core"), 0o755); mkdirErr != nil {
		return Artifact{}, fmt.Errorf("cannot create fixture sender root: %w", mkdirErr)
	}
	receiver, receiverErr := storagetrace.NewBenchmarkPartReceiver(receiverRoot)
	if receiverErr != nil {
		return Artifact{}, fmt.Errorf("cannot create fixture receiver: %w", receiverErr)
	}
	defer func() {
		if closeErr := receiver.Close(); closeErr != nil {
			generateErr = errors.Join(generateErr, fmt.Errorf("cannot close fixture receiver: %w", closeErr))
		}
	}()
	fileSystem := fs.NewLocalFileSystem()
	indexEncoders, encoderErr := newIndexEncoders(senderRoot, fileSystem)
	if encoderErr != nil {
		return Artifact{}, fmt.Errorf("cannot create fixture index encoders: %w", encoderErr)
	}
	defer func() {
		if closeErr := closeIndexEncoders(indexEncoders); closeErr != nil {
			generateErr = errors.Join(generateErr, fmt.Errorf("cannot close fixture index encoders: %w", closeErr))
		}
	}()
	lookup := buildSourceLookup(source)
	offsets, offsetErr := traceOffsets(plan, lookup, options)
	if offsetErr != nil {
		return Artifact{}, fmt.Errorf("cannot map fixture timestamps: %w", offsetErr)
	}
	var rowCount uint64
	for writeIdx := range plan.Writes {
		if contextErr := ctx.Err(); contextErr != nil {
			return Artifact{}, fmt.Errorf("fixture generation canceled: %w", contextErr)
		}
		write := &plan.Writes[writeIdx]
		partID := uint64(writeIdx + 1)
		rows, materializeErr := materializeWriteRows(plan, lookup, offsets, write)
		if materializeErr != nil {
			return Artifact{}, fmt.Errorf("cannot materialize fixture write %d: %w", writeIdx, materializeErr)
		}
		rowCount += uint64(len(rows))
		corePath, releaseCore := storagetrace.EncodePart(filepath.Join(senderRoot, "core"), fileSystem, partID, rows)
		indexPaths, encodeErr := encodeIndexes(indexEncoders, fileSystem, senderRoot, partID, rows)
		if encodeErr != nil {
			releaseCore()
			return Artifact{}, fmt.Errorf("cannot encode fixture write %d indexes: %w", writeIdx, encodeErr)
		}
		if evidenceErr := captureWriteEvidence(fileSystem, write, partID, corePath, indexPaths); evidenceErr != nil {
			releaseCore()
			return Artifact{}, fmt.Errorf("cannot capture fixture write %d evidence: %w", writeIdx, evidenceErr)
		}
		if receiveErr := receiver.Receive(ctx, corePath, indexPaths); receiveErr != nil {
			releaseCore()
			return Artifact{}, fmt.Errorf("cannot receive fixture write %d: %w", writeIdx, receiveErr)
		}
		releaseCore()
		if removeErr := removeSenderParts(corePath, indexPaths); removeErr != nil {
			return Artifact{}, fmt.Errorf("cannot remove fixture write %d sender files: %w", writeIdx, removeErr)
		}
	}
	if reopenErr := receiver.Reopen(); reopenErr != nil {
		return Artifact{}, fmt.Errorf("cannot reopen generated fixture: %w", reopenErr)
	}
	consolidatedSizes, consolidateErr := receiver.ConsolidatedCompressedSizes(ctx, int(source.Catalog.Core.PartCount))
	if consolidateErr != nil {
		return Artifact{}, fmt.Errorf("cannot measure consolidated fixture size: %w", consolidateErr)
	}
	if reconcileErr := reconcileFixture(ctx, receiver, source, plan, lookup, offsets, rowCount); reconcileErr != nil {
		return Artifact{}, fmt.Errorf("cannot reconcile generated fixture: %w", reconcileErr)
	}
	scheduleHash, scheduleErr := writeSchedule(options.OutputPath, plan)
	if scheduleErr != nil {
		return Artifact{}, fmt.Errorf("cannot persist fixture schedule: %w", scheduleErr)
	}
	artifact, artifactErr := buildArtifact(receiverRoot, source, plan, options, rowCount, scheduleHash)
	if artifactErr != nil {
		return Artifact{}, fmt.Errorf("cannot build fixture artifact: %w", artifactErr)
	}
	artifact.CoreConsolidatedBytes = consolidatedSizes.Core
	artifact.IndexConsolidatedBytes = consolidatedSizes.Indexes
	if gateErr := validateArtifactSizes(artifact, source); gateErr != nil {
		return Artifact{}, fmt.Errorf("fixture acceptance gate failed: %w", gateErr)
	}
	if writeErr := writeJSON(filepath.Join(options.OutputPath, "fixture.json"), artifact); writeErr != nil {
		return Artifact{}, fmt.Errorf("cannot write fixture artifact: %w", writeErr)
	}
	if removeErr := os.RemoveAll(senderRoot); removeErr != nil {
		return Artifact{}, fmt.Errorf("cannot remove fixture sender root: %w", removeErr)
	}
	keepOutput = true
	return artifact, nil
}

func captureWriteEvidence(fileSystem fs.FileSystem, write *Write, partID uint64, corePath string, indexPaths map[string]string) error {
	coreMetadata, metadataErr := storagetrace.ParsePartMetadata(fileSystem, corePath)
	if metadataErr != nil {
		return fmt.Errorf("cannot parse core evidence metadata: %w", metadataErr)
	}
	coreManifest, manifestErr := benchmark.TreeManifest(corePath)
	if manifestErr != nil {
		return fmt.Errorf("cannot hash core evidence: %w", manifestErr)
	}
	write.PartID = fmt.Sprintf("%016x", partID)
	write.MinTimestamp = coreMetadata.MinTimestamp
	write.MaxTimestamp = coreMetadata.MaxTimestamp
	write.CoreCompressedBytes = coreMetadata.CompressedSizeBytes
	write.CoreSHA256 = coreManifest.SHA256
	write.IndexSHA256 = make(map[string]string, len(indexPaths))
	write.IndexCompressedBytes = make(map[string]uint64, len(indexPaths))
	for indexName, indexPath := range indexPaths {
		indexMetadata, indexMetadataErr := sidx.ParsePartMetadata(fileSystem, indexPath)
		if indexMetadataErr != nil {
			return fmt.Errorf("cannot parse index %q evidence metadata: %w", indexName, indexMetadataErr)
		}
		indexManifest, indexManifestErr := benchmark.TreeManifest(indexPath)
		if indexManifestErr != nil {
			return fmt.Errorf("cannot hash index %q evidence: %w", indexName, indexManifestErr)
		}
		write.IndexCompressedBytes[indexName] = indexMetadata.CompressedSizeBytes
		write.IndexSHA256[indexName] = indexManifest.SHA256
	}
	return nil
}

func prepareNewOutput(path string) error {
	if _, statErr := os.Stat(path); statErr == nil {
		return fmt.Errorf("fixture output path %q already exists", path)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("cannot inspect fixture output path %q: %w", path, statErr)
	}
	if mkdirErr := os.MkdirAll(path, 0o755); mkdirErr != nil {
		return fmt.Errorf("cannot create fixture output path %q: %w", path, mkdirErr)
	}
	return nil
}

func buildSourceLookup(source Source) sourceLookup {
	lookup := make(sourceLookup, len(source.Mature)+len(source.Small))
	for _, population := range [][]LoadedTrace{source.Mature, source.Small} {
		for traceIdx := range population {
			lookup[population[traceIdx].SourceID] = population[traceIdx]
		}
	}
	return lookup
}

func traceOffsets(plan Plan, lookup sourceLookup, options GenerateOptions) ([]int64, error) {
	if options.MergeGrace < 0 {
		return nil, fmt.Errorf("merge grace must not be negative")
	}
	offsets := make([]int64, len(plan.Instances))
	initialized := make([]bool, len(plan.Instances))
	hotLowerBound := make([]int64, len(plan.Instances))
	globalSourceMin := int64(0)
	globalMinInitialized := false
	for sourceID := range lookup {
		minTimestamp, _ := loadedTraceBounds(lookup[sourceID])
		if !globalMinInitialized || minTimestamp < globalSourceMin {
			globalSourceMin = minTimestamp
			globalMinInitialized = true
		}
	}
	for writeIdx := range plan.Writes {
		write := &plan.Writes[writeIdx]
		publicationNanos := write.Publication.UnixNano()
		for fragmentIdx := range write.Fragments {
			fragmentRef := &write.Fragments[fragmentIdx]
			instance := &plan.Instances[fragmentRef.InstanceOrdinal]
			sourceTrace, ok := lookup[instance.SourceID]
			if !ok || fragmentRef.FragmentOrdinal >= len(sourceTrace.Fragments) {
				return nil, fmt.Errorf("scheduled trace %q fragment %d is absent from source", instance.SourceID, fragmentRef.FragmentOrdinal)
			}
			fragment := &sourceTrace.Fragments[fragmentRef.FragmentOrdinal].Fragment
			maturityFrontier := saturatingSubInt64(publicationNanos, int64(options.MergeGrace))
			candidateHotLowerBound := saturatingSubInt64(saturatingAddInt64(maturityFrontier, 1), fragment.MaxTimestamp)
			if !initialized[fragmentRef.InstanceOrdinal] {
				hotLowerBound[fragmentRef.InstanceOrdinal] = candidateHotLowerBound
				initialized[fragmentRef.InstanceOrdinal] = true
			} else {
				hotLowerBound[fragmentRef.InstanceOrdinal] = max(hotLowerBound[fragmentRef.InstanceOrdinal], candidateHotLowerBound)
			}
		}
	}
	dayStartNanos := options.DayStart.UnixNano()
	dayEndNanos := options.DayStart.Add(options.DayDuration).UnixNano()
	for instanceIdx := range plan.Instances {
		if !initialized[instanceIdx] {
			return nil, fmt.Errorf("generated trace %q has no scheduled fragments", plan.Instances[instanceIdx].GeneratedID)
		}
		sourceTrace := lookup[plan.Instances[instanceIdx].SourceID]
		minTimestamp, maxTimestamp := loadedTraceBounds(sourceTrace)
		offset := dayStartNanos - globalSourceMin
		offset = max(offset, hotLowerBound[instanceIdx])
		if minTimestamp+offset < dayStartNanos || maxTimestamp+offset >= dayEndNanos {
			return nil, fmt.Errorf("generated trace %q cannot fit in the half-open logical day", plan.Instances[instanceIdx].GeneratedID)
		}
		offsets[instanceIdx] = offset
	}
	for writeIdx := range plan.Writes {
		write := &plan.Writes[writeIdx]
		maturityFrontier := saturatingSubInt64(write.Publication.UnixNano(), int64(options.MergeGrace))
		for fragmentIdx := range write.Fragments {
			fragmentRef := &write.Fragments[fragmentIdx]
			sourceTrace := lookup[plan.Instances[fragmentRef.InstanceOrdinal].SourceID]
			fragment := &sourceTrace.Fragments[fragmentRef.FragmentOrdinal].Fragment
			remappedMax := saturatingAddInt64(fragment.MaxTimestamp, offsets[fragmentRef.InstanceOrdinal])
			if remappedMax <= maturityFrontier {
				return nil, fmt.Errorf("write %d trace %q is not hot: max timestamp %d, frontier %d", writeIdx,
					fragmentRef.GeneratedTraceID, remappedMax, maturityFrontier)
			}
		}
	}
	return offsets, nil
}

func saturatingAddInt64(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	if right < 0 && left < math.MinInt64-right {
		return math.MinInt64
	}
	return left + right
}

func saturatingSubInt64(left, right int64) int64 {
	if right == math.MinInt64 {
		if left >= 0 {
			return math.MaxInt64
		}
		return left - right
	}
	return saturatingAddInt64(left, -right)
}

func loadedTraceBounds(trace LoadedTrace) (int64, int64) {
	minTimestamp := trace.Fragments[0].Fragment.MinTimestamp
	maxTimestamp := trace.Fragments[0].Fragment.MaxTimestamp
	for fragmentIdx := 1; fragmentIdx < len(trace.Fragments); fragmentIdx++ {
		minTimestamp = min(minTimestamp, trace.Fragments[fragmentIdx].Fragment.MinTimestamp)
		maxTimestamp = max(maxTimestamp, trace.Fragments[fragmentIdx].Fragment.MaxTimestamp)
	}
	return minTimestamp, maxTimestamp
}

func materializeWriteRows(plan Plan, lookup sourceLookup, offsets []int64, write *Write) ([]storagetrace.PartEncoderRow, error) {
	rows := make([]storagetrace.PartEncoderRow, 0, write.Rows)
	for scheduledIdx := range write.Fragments {
		scheduled := &write.Fragments[scheduledIdx]
		instance := &plan.Instances[scheduled.InstanceOrdinal]
		sourceTrace := lookup[instance.SourceID]
		if scheduled.FragmentOrdinal >= len(sourceTrace.Fragments) {
			return nil, fmt.Errorf("source trace %q fragment ordinal %d is invalid", instance.SourceID, scheduled.FragmentOrdinal)
		}
		fragment := &sourceTrace.Fragments[scheduled.FragmentOrdinal]
		for rowIdx := range fragment.Rows {
			rows = append(rows, remapRow(fragment.Rows[rowIdx], instance.GeneratedID, offsets[scheduled.InstanceOrdinal]))
		}
	}
	if uint64(len(rows)) != write.Rows {
		return nil, fmt.Errorf("materialized row mismatch: got %d, want %d", len(rows), write.Rows)
	}
	return rows, nil
}

func remapRow(source Row, traceID string, offset int64) storagetrace.PartEncoderRow {
	tagNames := make([]string, 0, len(source.Tags))
	for tagName := range source.Tags {
		tagNames = append(tagNames, tagName)
	}
	sort.Strings(tagNames)
	tags := make([]storagetrace.PartEncoderTag, 0, len(tagNames))
	for _, tagName := range tagNames {
		valueType := source.TagTypes[tagName]
		value := append([]byte(nil), source.Tags[tagName]...)
		if valueType == pbv1.ValueTypeTimestamp && len(value) >= 8 {
			value = convert.Int64ToBytes(convert.BytesToInt64(value) + offset)
		}
		tags = append(tags, storagetrace.PartEncoderTag{Name: tagName, RawValue: value, ValueType: valueType})
	}
	indexTags := make(map[string][]sidx.Tag, len(source.IndexTags))
	for indexName, sourceTags := range source.IndexTags {
		indexTags[indexName] = cloneIndexTags(sourceTags)
		for tagIdx := range indexTags[indexName] {
			tag := &indexTags[indexName][tagIdx]
			if tag.ValueType == pbv1.ValueTypeTimestamp && len(tag.Value) >= 8 {
				tag.Value = convert.Int64ToBytes(convert.BytesToInt64(tag.Value) + offset)
			}
		}
	}
	return storagetrace.PartEncoderRow{
		IndexSeries: source.IndexSeries, IndexTags: indexTags, TraceID: traceID, SpanID: source.SpanID, Span: append([]byte(nil), source.Span...),
		Tags: tags, Timestamp: source.Timestamp + offset,
	}
}

func newIndexEncoders(senderRoot string, fileSystem fs.FileSystem) (map[string]sidx.SIDX, error) {
	encoders := make(map[string]sidx.SIDX, 2)
	for _, indexName := range fixtureIndexNames {
		indexRoot, absoluteErr := filepath.Abs(filepath.Join(senderRoot, "sidx", indexName))
		if absoluteErr != nil {
			return nil, errors.Join(fmt.Errorf("cannot resolve sender index root %q: %w", indexName, absoluteErr), closeIndexEncoders(encoders))
		}
		options, optionErr := sidx.NewOptions(indexRoot, protector.Nop{})
		if optionErr != nil {
			return nil, errors.Join(fmt.Errorf("cannot configure sender index %q: %w", indexName, optionErr), closeIndexEncoders(encoders))
		}
		encoder, encoderErr := sidx.NewSIDX(fileSystem, options)
		if encoderErr != nil {
			return nil, errors.Join(fmt.Errorf("cannot create sender index %q: %w", indexName, encoderErr), closeIndexEncoders(encoders))
		}
		encoders[indexName] = encoder
	}
	return encoders, nil
}

func closeIndexEncoders(encoders map[string]sidx.SIDX) error {
	var closeErr error
	for indexName, encoder := range encoders {
		if encoderErr := encoder.Close(); encoderErr != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("cannot close sender index %q: %w", indexName, encoderErr))
		}
	}
	return closeErr
}

func encodeIndexes(encoders map[string]sidx.SIDX, fileSystem fs.FileSystem, senderRoot string, partID uint64,
	rows []storagetrace.PartEncoderRow,
) (map[string]string, error) {
	requests := map[string][]sidx.WriteRequest{
		"latency": make([]sidx.WriteRequest, 0, len(rows)), "start_time": make([]sidx.WriteRequest, 0, len(rows)),
	}
	minTimestamp, maxTimestamp := rows[0].Timestamp, rows[0].Timestamp
	for rowIdx := range rows {
		row := &rows[rowIdx]
		minTimestamp = min(minTimestamp, row.Timestamp)
		maxTimestamp = max(maxTimestamp, row.Timestamp)
		values := map[string]int64{"latency": 0, "start_time": row.Timestamp}
		for tagIdx := range row.Tags {
			tag := &row.Tags[tagIdx]
			if (tag.Name == "latency" || tag.Name == "start_time") && len(tag.RawValue) >= 8 {
				values[tag.Name] = convert.BytesToInt64(tag.RawValue)
			}
		}
		for _, indexName := range fixtureIndexNames {
			key := values[indexName]
			data := make([]byte, len(row.TraceID)+1)
			data[0] = traceIDEncodingV1
			copy(data[1:], row.TraceID)
			seriesID, ok := row.IndexSeries[indexName]
			if !ok {
				return nil, fmt.Errorf("trace %q is missing source index series %q", row.TraceID, indexName)
			}
			requests[indexName] = append(requests[indexName], sidx.WriteRequest{
				SeriesID: seriesID, Key: key, Data: data, Tags: cloneIndexTags(row.IndexTags[indexName]),
			})
		}
	}
	paths := make(map[string]string, len(encoders))
	for _, indexName := range fixtureIndexNames {
		memPart, convertErr := encoders[indexName].ConvertToMemPart(requests[indexName], 0, &minTimestamp, &maxTimestamp)
		if convertErr != nil {
			return nil, fmt.Errorf("cannot encode index %q: %w", indexName, convertErr)
		}
		partPath := filepath.Join(senderRoot, "sidx", indexName, fmt.Sprintf("%016x", partID))
		memPart.MustFlush(fileSystem, partPath)
		sidx.ReleaseMemPart(memPart)
		paths[indexName] = partPath
	}
	return paths, nil
}

func removeSenderParts(corePath string, indexPaths map[string]string) error {
	var removeErr error
	if coreErr := os.RemoveAll(corePath); coreErr != nil {
		removeErr = errors.Join(removeErr, coreErr)
	}
	for _, indexPath := range indexPaths {
		if indexErr := os.RemoveAll(indexPath); indexErr != nil {
			removeErr = errors.Join(removeErr, indexErr)
		}
	}
	return removeErr
}

func writeSchedule(root string, plan Plan) (string, error) {
	path := filepath.Join(root, "schedule.json")
	data, marshalErr := json.MarshalIndent(plan, "", "  ")
	if marshalErr != nil {
		return "", fmt.Errorf("cannot marshal fixture schedule: %w", marshalErr)
	}
	data = append(data, '\n')
	if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
		return "", fmt.Errorf("cannot write fixture schedule: %w", writeErr)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func buildArtifact(receiverRoot string, source Source, plan Plan, options GenerateOptions, rowCount uint64, scheduleHash string) (Artifact, error) {
	coreManifest, coreErr := benchmark.TreeManifest(receiverRoot)
	if coreErr != nil {
		return Artifact{}, fmt.Errorf("cannot manifest generated fixture: %w", coreErr)
	}
	indexManifests := make(map[string]benchmark.Manifest, 2)
	indexCompressedBytes := make(map[string]uint64, 2)
	for _, indexName := range fixtureIndexNames {
		indexRoot := filepath.Join(receiverRoot, "sidx", indexName)
		indexManifest, indexErr := benchmark.TreeManifest(indexRoot)
		if indexErr != nil {
			return Artifact{}, fmt.Errorf("cannot manifest generated index %q: %w", indexName, indexErr)
		}
		indexManifests[indexName] = indexManifest
		indexPartIDs, indexDiscoverErr := dump.DiscoverPartIDs(indexRoot)
		if indexDiscoverErr != nil {
			return Artifact{}, fmt.Errorf("cannot discover generated index %q parts: %w", indexName, indexDiscoverErr)
		}
		for _, indexPartID := range indexPartIDs {
			metadata, metadataErr := sidx.ParsePartMetadata(fs.NewLocalFileSystem(), filepath.Join(indexRoot, fmt.Sprintf("%016x", indexPartID)))
			if metadataErr != nil {
				return Artifact{}, fmt.Errorf("cannot parse generated index %q part %016x: %w", indexName, indexPartID, metadataErr)
			}
			indexCompressedBytes[indexName] += metadata.CompressedSizeBytes
		}
	}
	partIDs, discoverErr := dump.DiscoverPartIDs(receiverRoot)
	if discoverErr != nil {
		return Artifact{}, fmt.Errorf("cannot discover generated core parts: %w", discoverErr)
	}
	fileSystem := fs.NewLocalFileSystem()
	var coreCompressedBytes uint64
	for _, partID := range partIDs {
		partPath := filepath.Join(receiverRoot, fmt.Sprintf("%016x", partID))
		metadata, metadataErr := storagetrace.ParsePartMetadata(fileSystem, partPath)
		if metadataErr != nil {
			return Artifact{}, fmt.Errorf("cannot parse generated core part %016x: %w", partID, metadataErr)
		}
		coreCompressedBytes += metadata.CompressedSizeBytes
	}
	partialTails := uint64(0)
	for writeIdx := range plan.Writes {
		if plan.Writes[writeIdx].PartialTail {
			partialTails++
		}
	}
	return Artifact{
		CoreManifest: coreManifest, CoreCompressedBytes: coreCompressedBytes, IndexManifests: indexManifests,
		IndexCompressedBytes:       indexCompressedBytes,
		SourceIndexCompressedBytes: source.IndexCompressedBytes,
		SourceManifestSHA256:       source.Catalog.SourceManifestSHA256,
		SourceCatalogSHA256:        hashJSON(source.Catalog),
		ClosureAllowlistSHA256:     closureAllowlistHash(source),
		CopyAllowlistSHA256:        copyAllowlistHash(plan.Instances),
		GeneratedIDManifestSHA256:  generatedIDManifest(plan.Instances), ScheduleSHA256: scheduleHash,
		DayStart: options.DayStart, DayDuration: options.DayDuration, MergeGrace: options.MergeGrace,
		TraceCount: uint64(len(plan.Instances)), RowCount: rowCount, WriteCount: uint64(len(plan.Writes)), PartialTailWrites: partialTails, Version: fixtureVersion,
	}, nil
}

func hashJSON(value any) string {
	data, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		panic(fmt.Sprintf("cannot marshal in-memory reproducibility input: %v", marshalErr))
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func closureAllowlistHash(source Source) string {
	traceIDs := append([]string(nil), source.Catalog.Mature.ClosureTraceIDs...)
	traceIDs = append(traceIDs, source.Catalog.Small.ClosureTraceIDs...)
	sort.Strings(traceIDs)
	return hashLines(traceIDs)
}

func copyAllowlistHash(instances []Instance) string {
	traceIDs := make([]string, 0, CopyTraceCount)
	for instanceIdx := range instances {
		if instances[instanceIdx].Class == TraceClassCopy {
			traceIDs = append(traceIDs, instances[instanceIdx].SourceID)
		}
	}
	sort.Strings(traceIDs)
	return hashLines(traceIDs)
}

func hashLines(lines []string) string {
	digest := sha256.New()
	for _, line := range lines {
		mustWriteHashString(digest, line+"\n")
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func generatedIDManifest(instances []Instance) string {
	ordered := append([]Instance(nil), instances...)
	sort.Slice(ordered, func(leftIdx, rightIdx int) bool { return ordered[leftIdx].GeneratedID < ordered[rightIdx].GeneratedID })
	digest := sha256.New()
	for instanceIdx := range ordered {
		instance := &ordered[instanceIdx]
		mustWriteHashString(digest, fmt.Sprintf("%s\t%s\t%s\t%d\n", instance.GeneratedID, instance.SourceID, instance.Class, instance.CopyOrdinal))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func validateArtifactSizes(artifact Artifact, source Source) error {
	var gateErr error
	if artifact.TraceCount != source.Catalog.Core.TraceCount {
		gateErr = errors.Join(gateErr, fmt.Errorf("generated trace count mismatch: got %d, want %d", artifact.TraceCount, source.Catalog.Core.TraceCount))
	}
	if !withinTolerance(artifact.RowCount, source.Catalog.Core.RowCount, coreSizeTolerance) {
		gateErr = errors.Join(gateErr,
			fmt.Errorf("generated row count %d is outside %.0f%% of source %d", artifact.RowCount, coreSizeTolerance*100, source.Catalog.Core.RowCount))
	}
	if !withinTolerance(artifact.CoreConsolidatedBytes, source.Catalog.Core.CompressedBytes, coreSizeTolerance) {
		gateErr = errors.Join(gateErr, fmt.Errorf("generated consolidated core bytes %d are outside %.0f%% of source %d", artifact.CoreConsolidatedBytes,
			coreSizeTolerance*100, source.Catalog.Core.CompressedBytes))
	}
	for indexName := range source.Catalog.Indexes {
		generatedBytes := artifact.IndexConsolidatedBytes[indexName]
		sourceBytes := source.IndexCompressedBytes[indexName]
		if !withinTolerance(generatedBytes, sourceBytes, indexSizeTolerance) {
			gateErr = errors.Join(gateErr, fmt.Errorf("generated consolidated index %q compressed bytes %d are outside %.0f%% of source %d", indexName,
				generatedBytes, indexSizeTolerance*100, sourceBytes))
		}
	}
	var generatedIndexTotal, sourceIndexTotal uint64
	for indexName := range source.Catalog.Indexes {
		generatedIndexTotal += artifact.IndexConsolidatedBytes[indexName]
		sourceIndexTotal += source.IndexCompressedBytes[indexName]
	}
	if !withinTolerance(generatedIndexTotal, sourceIndexTotal, indexTotalTolerance) {
		gateErr = errors.Join(gateErr, fmt.Errorf("generated consolidated index total %d is outside %.0f%% of source %d", generatedIndexTotal,
			indexTotalTolerance*100, sourceIndexTotal))
	}
	if artifact.PartialTailWrites > 1 {
		gateErr = errors.Join(gateErr, fmt.Errorf("generated %d partial-tail writes, want at most one", artifact.PartialTailWrites))
	}
	return gateErr
}

func withinTolerance(actual, expected uint64, tolerance float64) bool {
	delta := float64(actual) - float64(expected)
	if delta < 0 {
		delta = -delta
	}
	return delta <= float64(expected)*tolerance
}

func writeJSON(path string, value any) error {
	data, marshalErr := json.MarshalIndent(value, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("cannot marshal JSON: %w", marshalErr)
	}
	data = append(data, '\n')
	if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
		return fmt.Errorf("cannot write JSON %q: %w", path, writeErr)
	}
	return nil
}
