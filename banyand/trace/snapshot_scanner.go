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

package trace

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// SnapshotScanOptions bounds scratch metadata, merge fan-in and assembled trace bytes.
type SnapshotScanOptions struct {
	TempDir          string
	MaxMetadataBytes int64
	MaxTraceBytes    int64
	MaxOpenReaders   int
}

// SnapshotTrace is one whole local trace. Oversized traces have no projected payload.
// The returned columns are independently owned and survive subsequent scanner calls.
type SnapshotTrace struct {
	Trace         sdk.TraceBlock
	SpanCount     uint64
	InputConflict bool
	Oversized     bool
	CoverageKnown bool
}

// ProjectedSnapshotScanner assembles immutable source parts in trace-ID order.
// It owns temporary merge runs but does not own or remove the source snapshot.
type ProjectedSnapshotScanner struct {
	input      *os.File
	decoder    *gob.Decoder
	pending    *snapshotMetadata
	scratch    string
	projection sdk.Projection
	options    SnapshotScanOptions
}

var errSnapshotTraceOversized = errors.New("projected trace exceeds budget")

type snapshotMetadata struct {
	TagTypes map[string]pbv1.ValueType
	TraceID  string
	PartPath string
	Metadata []byte
}

// NewProjectedSnapshotScanner prepares bounded external metadata merge runs.
func NewProjectedSnapshotScanner(ctx context.Context, partPaths []string, projection sdk.Projection,
	options SnapshotScanOptions,
) (scanner *ProjectedSnapshotScanner, err error) {
	if options.MaxMetadataBytes <= 0 {
		options.MaxMetadataBytes = 8 << 20
	}
	if options.MaxTraceBytes <= 0 {
		options.MaxTraceBytes = 16 << 20
	}
	if options.MaxOpenReaders <= 0 {
		options.MaxOpenReaders = 16
	}
	if options.MaxOpenReaders < 2 {
		return nil, errors.New("snapshot merge requires at least two readers")
	}
	scratch, err := os.MkdirTemp(options.TempDir, "trace-snapshot-scan-")
	if err != nil {
		return nil, err
	}
	scanner = &ProjectedSnapshotScanner{projection: projection, options: options, scratch: scratch}
	cleanupScanner := scanner
	defer func() {
		if err != nil {
			_ = cleanupScanner.Close()
		}
	}()
	var runs []string
	for _, partPath := range partPaths {
		run, runErr := scanner.buildRun(ctx, partPath)
		if runErr != nil {
			return nil, fmt.Errorf("snapshot part %s: %w", partPath, runErr)
		}
		runs = append(runs, run)
	}
	for len(runs) > 1 {
		var next []string
		for offset := 0; offset < len(runs); offset += options.MaxOpenReaders {
			end := min(offset+options.MaxOpenReaders, len(runs))
			run, mergeErr := scanner.mergeRuns(ctx, runs[offset:end])
			if mergeErr != nil {
				return nil, mergeErr
			}
			next = append(next, run)
			for _, old := range runs[offset:end] {
				if removeErr := os.Remove(old); removeErr != nil {
					return nil, removeErr
				}
			}
		}
		runs = next
	}
	if len(runs) == 0 {
		return scanner, nil
	}
	scanner.input, err = os.Open(runs[0])
	if err != nil {
		return nil, err
	}
	scanner.decoder = gob.NewDecoder(scanner.input)
	return scanner, nil
}

func (s *ProjectedSnapshotScanner) newRun() (*os.File, error) {
	return os.CreateTemp(s.scratch, "run-")
}

func readSnapshotBytes(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("metadata exceeds %d-byte budget", limit)
	}
	return data, nil
}

func (s *ProjectedSnapshotScanner) decompressMetadata(data []byte) ([]byte, error) {
	return zstd.DecompressBounded(data, int(s.options.MaxMetadataBytes))
}

func (s *ProjectedSnapshotScanner) buildRun(ctx context.Context, path string) (run string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid snapshot metadata: %v", recovered)
		}
	}()
	typesData, err := readSnapshotBytes(filepath.Join(path, tagTypeFilename), s.options.MaxMetadataBytes)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	types := make(tagType)
	if len(typesData) > 0 {
		if err = types.unmarshal(typesData); err != nil {
			return "", err
		}
	}
	meta, err := readSnapshotBytes(filepath.Join(path, metaFilename), s.options.MaxMetadataBytes)
	if err != nil {
		return "", err
	}
	meta, err = s.decompressMetadata(meta)
	if err != nil {
		return "", err
	}
	index, err := unmarshalPrimaryBlockMetadata(nil, meta)
	if err != nil {
		return "", err
	}
	primary, err := os.Open(filepath.Join(path, primaryFilename))
	if err != nil {
		return "", err
	}
	defer primary.Close()
	output, err := s.newRun()
	if err != nil {
		return "", err
	}
	defer output.Close()
	encoder := gob.NewEncoder(output)
	previous := ""
	for _, entry := range index {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		if entry.size > uint64(s.options.MaxMetadataBytes) {
			return "", errors.New("primary metadata exceeds budget")
		}
		compressed := make([]byte, int(entry.size))
		if _, err = primary.ReadAt(compressed, int64(entry.offset)); err != nil {
			return "", err
		}
		data, decodeErr := s.decompressMetadata(compressed)
		if decodeErr != nil {
			return "", decodeErr
		}
		for len(data) > 0 {
			var metadata blockMetadata
			tail, parseErr := metadata.unmarshal(data, types)
			if parseErr != nil {
				return "", parseErr
			}
			if metadata.traceID < previous {
				return "", errors.New("source blocks are not trace-ID ordered")
			}
			previous = metadata.traceID
			record := snapshotMetadata{TraceID: metadata.traceID, PartPath: path, Metadata: metadata.marshal(nil), TagTypes: types}
			if err = encoder.Encode(record); err != nil {
				return "", err
			}
			data = tail
		}
	}
	if err = output.Close(); err != nil {
		return "", err
	}
	return output.Name(), nil
}

func (s *ProjectedSnapshotScanner) mergeRuns(ctx context.Context, paths []string) (string, error) {
	readers := make([]*os.File, 0, len(paths))
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()
	decoders := make([]*gob.Decoder, len(paths))
	heads := make([]*snapshotMetadata, len(paths))
	for index, path := range paths {
		reader, err := os.Open(path)
		if err != nil {
			return "", err
		}
		readers = append(readers, reader)
		decoders[index] = gob.NewDecoder(reader)
		heads[index] = &snapshotMetadata{}
		if err = decoders[index].Decode(heads[index]); errors.Is(err, io.EOF) {
			heads[index] = nil
		} else if err != nil {
			return "", err
		}
	}
	output, err := s.newRun()
	if err != nil {
		return "", err
	}
	defer output.Close()
	encoder := gob.NewEncoder(output)
	for {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		best := -1
		for index, head := range heads {
			if head != nil && (best < 0 || head.TraceID < heads[best].TraceID) {
				best = index
			}
		}
		if best < 0 {
			break
		}
		if err = encoder.Encode(heads[best]); err != nil {
			return "", err
		}
		*heads[best] = snapshotMetadata{}
		if err = decoders[best].Decode(heads[best]); errors.Is(err, io.EOF) {
			heads[best] = nil
		} else if err != nil {
			return "", err
		}
	}
	if err = output.Close(); err != nil {
		return "", err
	}
	return output.Name(), nil
}

func (s *ProjectedSnapshotScanner) nextMetadata() (*snapshotMetadata, error) {
	if s.pending != nil {
		metadata := s.pending
		s.pending = nil
		return metadata, nil
	}
	if s.decoder == nil {
		return nil, io.EOF
	}
	var metadata snapshotMetadata
	if err := s.decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// Next returns the next whole trace, or io.EOF after the immutable inventory ends.
func (s *ProjectedSnapshotScanner) Next(ctx context.Context) (result *SnapshotTrace, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("invalid snapshot payload: %v", recovered)
		}
	}()
	first, err := s.nextMetadata()
	if err != nil {
		return nil, err
	}
	result = &SnapshotTrace{Trace: sdk.TraceBlock{TraceID: first.TraceID}, CoverageKnown: true}
	used := int64(len(first.TraceID))
	result.Oversized = used > s.options.MaxTraceBytes
	current := first
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var metadata blockMetadata
		if _, err = metadata.unmarshal(current.Metadata, current.TagTypes); err != nil {
			return nil, err
		}
		oldRows := int(result.SpanCount)
		if metadata.count > ^uint64(0)-result.SpanCount {
			return nil, errors.New("snapshot span count overflows")
		}
		result.SpanCount += metadata.count
		if !metadata.timestamps.known {
			result.CoverageKnown = false
		}
		if current == first {
			result.Trace.MinTS = metadata.timestamps.min
			result.Trace.MaxTS = metadata.timestamps.max
		} else {
			result.Trace.MinTS = min(result.Trace.MinTS, metadata.timestamps.min)
			result.Trace.MaxTS = max(result.Trace.MaxTS, metadata.timestamps.max)
		}
		if !result.Oversized {
			perRow := uint64(32 * (len(s.projection.Tags) + 1))
			estimate := uint64(s.options.MaxTraceBytes) + 1
			if result.SpanCount <= uint64(s.options.MaxTraceBytes)/perRow {
				estimate = metadata.count * perRow
			}
			if s.projection.Spans || s.projection.SpanIDs {
				if metadata.uncompressedSpanSizeBytes > uint64(s.options.MaxTraceBytes) || estimate > uint64(s.options.MaxTraceBytes)-metadata.uncompressedSpanSizeBytes {
					estimate = uint64(s.options.MaxTraceBytes) + 1
				} else {
					estimate += metadata.uncompressedSpanSizeBytes
				}
			}
			if estimate > uint64(s.options.MaxTraceBytes-used) {
				result.Oversized = true
			} else {
				remaining := s.options.MaxTraceBytes - used
				padding := snapshotProjectionPadding(result.Trace.Tags, metadata.tags, s.projection, oldRows)
				var payload sdk.TraceBlock
				var payloadErr error
				if padding > remaining {
					payloadErr = errSnapshotTraceOversized
				} else {
					payload, payloadErr = s.decodeProjection(current, &metadata, remaining-padding)
				}
				if errors.Is(payloadErr, errSnapshotTraceOversized) || errors.Is(payloadErr, zstd.ErrDecompressionLimit) {
					result.Oversized = true
				} else if payloadErr != nil {
					return nil, payloadErr
				}
				for _, column := range payload.Tags {
					for _, previousColumn := range result.Trace.Tags {
						if column.Name == previousColumn.Name && column.ValueType != previousColumn.ValueType {
							result.InputConflict = true
							result.CoverageKnown = false
						}
					}
				}
				if !result.Oversized {
					used += appendSnapshotProjection(&result.Trace, payload, oldRows, int(metadata.count), s.projection)
				}
				if used > s.options.MaxTraceBytes {
					result.Oversized = true
				}
			}
			if result.Oversized {
				result.Trace.Tags = nil
				result.Trace.SpanIDs = nil
				result.Trace.Spans = nil
			}
		}
		current, err = s.nextMetadata()
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if current.TraceID != first.TraceID {
			s.pending = current
			return result, nil
		}
	}
}

func (s *ProjectedSnapshotScanner) decodeProjection(record *snapshotMetadata, metadata *blockMetadata, remaining int64) (sdk.TraceBlock, error) {
	result := sdk.TraceBlock{}
	decoder := &encoding.BytesBlockDecoder{}
	decoder.SetMaxBytes(int(remaining))
	readRange := func(name string, data *dataBlock) ([]byte, error) {
		if data.size > uint64(remaining) {
			return nil, errSnapshotTraceOversized
		}
		file, err := os.Open(filepath.Join(record.PartPath, name))
		if err != nil {
			return nil, err
		}
		defer file.Close()
		buf := make([]byte, int(data.size))
		_, err = file.ReadAt(buf, int64(data.offset))
		return buf, err
	}
	if s.projection.Spans || s.projection.SpanIDs {
		data, err := readRange(spansFilename, metadata.spans)
		if err != nil {
			return result, err
		}
		result, err = decodeSnapshotSpans(data, metadata.count, s.projection, &remaining)
		if err != nil {
			return result, err
		}
	}
	names := make([]string, 0, len(metadata.tags))
	for name := range metadata.tags {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, projected := range s.projection.Tags {
		for _, name := range names {
			if decodeTypedTag(name) != projected {
				continue
			}
			tagMeta, err := readRange(name+tagsMetadataFilenameExt, metadata.tags[name])
			if err != nil {
				return result, err
			}
			var tm tagMetadata
			if err = tm.unmarshal(tagMeta); err != nil {
				return result, err
			}
			if tm.size > uint64(remaining) {
				return result, errSnapshotTraceOversized
			}
			valuesData, err := readRange(name+tagsFilenameExt, &tm.dataBlock)
			if err != nil {
				return result, err
			}
			if remaining <= 0 {
				return result, errSnapshotTraceOversized
			}
			decoder.Reset()
			decoder.SetMaxBytes(int(remaining))
			var values [][]byte
			valueType := record.TagTypes[name]
			switch {
			case len(valuesData) > 0 && encoding.EncodeType(valuesData[0]) == encoding.EncodeTypePlain:
				values, err = decoder.Decode(nil, valuesData[1:], metadata.count)
			case valueType == pbv1.ValueTypeInt64 || valueType == pbv1.ValueTypeFloat64:
				values, err = internalencoding.DecodeTagValues(nil, decoder, &pkgbytes.Buffer{Buf: valuesData}, valueType, int(metadata.count))
			case len(valuesData) > 0 && encoding.EncodeType(valuesData[0]) == encoding.EncodeTypeDictionary:
				values, err = encoding.NewDictionary().DecodeWithLimit(nil, valuesData[1:], metadata.count, int(remaining))
			case len(valuesData) > 0:
				values, err = decoder.Decode(nil, valuesData[1:], metadata.count)
			}
			if err != nil {
				return result, err
			}
			if uint64(len(values)) != metadata.count {
				return result, fmt.Errorf("projected tag %s has %d values for %d rows", projected, len(values), metadata.count)
			}
			if err = consumeSnapshotValues(values, 24, &remaining); err != nil {
				return result, err
			}
			column := sdk.TagColumn{Name: projected, ValueType: record.TagTypes[name]}
			for _, value := range values {
				var copied []byte
				if value != nil {
					copied = make([]byte, len(value))
					copy(copied, value)
				}
				column.Values = append(column.Values, copied)
			}
			result.Tags = append(result.Tags, column)
			break
		}
	}
	return result, nil
}

func appendSnapshotProjection(target *sdk.TraceBlock, source sdk.TraceBlock, oldRows, rows int, projection sdk.Projection) int64 {
	var used int64
	target.SpanIDs = append(target.SpanIDs, source.SpanIDs...)
	target.Spans = append(target.Spans, source.Spans...)
	for _, id := range source.SpanIDs {
		used += int64(len(id) + 16)
	}
	for _, span := range source.Spans {
		used += int64(len(span) + 24)
	}
	for _, name := range projection.Tags {
		index := -1
		for columnIdx := range target.Tags {
			if target.Tags[columnIdx].Name == name {
				index = columnIdx
				break
			}
		}
		var column *sdk.TagColumn
		for sourceIdx := range source.Tags {
			if source.Tags[sourceIdx].Name == name {
				column = &source.Tags[sourceIdx]
				break
			}
		}
		if index < 0 && column != nil {
			target.Tags = append(target.Tags, sdk.TagColumn{Name: name, ValueType: column.ValueType, Values: make([][]byte, oldRows)})
			index = len(target.Tags) - 1
			used += int64(oldRows * 24)
		}
		if index < 0 {
			continue
		}
		if column == nil {
			target.Tags[index].Values = append(target.Tags[index].Values, make([][]byte, rows)...)
			used += int64(rows * 24)
			continue
		}
		target.Tags[index].Values = append(target.Tags[index].Values, column.Values...)
		for _, value := range column.Values {
			used += int64(len(value) + 24)
		}
	}
	return used
}

// Close releases scan handles and temporary runs. Source parts are preserved.
func (s *ProjectedSnapshotScanner) Close() error {
	var closeErr error
	if s.input != nil {
		closeErr = s.input.Close()
		s.input = nil
	}
	s.decoder = nil
	s.pending = nil
	if s.scratch != "" {
		closeErr = errors.Join(closeErr, os.RemoveAll(s.scratch))
		s.scratch = ""
	}
	return closeErr
}

// GuardSnapshotTrace applies the existing fragment guard to a proposed snapshot drop.
// It inherits the storage runtime's grace and completeness semantics for the pinned inventory.
func GuardSnapshotTrace(ctx context.Context, trace SnapshotTrace, sourceMin, sourceMax int64, grace time.Duration, proposedKeep bool) (bool, string) {
	catalog := traceFragmentGuardCatalog{
		Complete: true, CoverageKnown: true,
		CoverageMinTimestamp: sourceMin, CoverageMaxTimestamp: sourceMax,
		TemporalSafety: traceFragmentTemporalSafetyMaxGapEnforced, EnforcedMaxFragmentGap: grace, Pin: snapshotGuardPin{},
	}
	guard := newTraceFragmentGuard(traceFragmentGuardConfig{Grace: grace}, catalog)
	defer guard.Close()
	action := traceFragmentSamplerActionDrop
	if proposedKeep {
		action = traceFragmentSamplerActionKeep
	}
	decision := guard.Resolve(ctx, traceFragmentGuardTrace{
		TraceID:  trace.Trace.TraceID,
		Complete: !trace.Oversized && trace.CoverageKnown,
		Blocks:   []traceFragmentGuardBlock{{MinTimestamp: trace.Trace.MinTS, MaxTimestamp: trace.Trace.MaxTS, BoundsKnown: trace.CoverageKnown}},
	}, action)
	return decision.Action != traceFragmentGuardActionDrop, string(decision.Reason)
}

type snapshotGuardPin struct{}

func (snapshotGuardPin) Release() {}

func consumeSnapshotValues(values [][]byte, referenceBytes int64, remaining *int64) error {
	for _, value := range values {
		size := int64(len(value)) + referenceBytes
		if size > *remaining {
			return errSnapshotTraceOversized
		}
		*remaining -= size
	}
	return nil
}

func snapshotProjectionPadding(columns []sdk.TagColumn, tags map[string]*dataBlock, projection sdk.Projection, oldRows int) int64 {
	var padding int64
	for _, projected := range projection.Tags {
		exists := false
		for _, column := range columns {
			if column.Name == projected {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		for encoded := range tags {
			if decodeTypedTag(encoded) == projected {
				padding += int64(oldRows) * 24
				break
			}
		}
	}
	return padding
}

func decodeSnapshotSpans(data []byte, count uint64, projection sdk.Projection, remaining *int64) (sdk.TraceBlock, error) {
	result := sdk.TraceBlock{}
	decoder := &encoding.BytesBlockDecoder{}
	decoder.SetMaxBytes(int(*remaining))
	ids, tail, err := decoder.DecodeWithTail(nil, data, count)
	if err != nil {
		return result, err
	}
	if projection.SpanIDs {
		if err = consumeSnapshotValues(ids, 16, remaining); err != nil {
			return result, err
		}
		for _, id := range ids {
			result.SpanIDs = append(result.SpanIDs, string(id))
		}
	}
	if projection.Spans {
		if *remaining <= 0 {
			return result, errSnapshotTraceOversized
		}
		decoder.Reset()
		decoder.SetMaxBytes(int(*remaining))
		spans, decodeErr := decoder.Decode(nil, tail, count)
		if decodeErr != nil {
			return result, decodeErr
		}
		if decodeErr = consumeSnapshotValues(spans, 24, remaining); decodeErr != nil {
			return result, decodeErr
		}
		for _, span := range spans {
			result.Spans = append(result.Spans, append([]byte(nil), span...))
		}
	}
	return result, nil
}
