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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

func snapshotTestPart(t *testing.T, root string, id uint64, ids ...string) string {
	t.Helper()
	rows := make([]PartEncoderRow, 0, len(ids))
	for index, traceID := range ids {
		rows = append(rows, PartEncoderRow{
			TraceID: traceID, SpanID: traceID + "-span", Span: []byte("payload"),
			Timestamp: int64(index + 1), Tags: []PartEncoderTag{{Name: "status", Value: "error"}},
		})
	}
	path, release := EncodePart(root, fs.NewLocalFileSystem(), id, rows)
	release()
	return path
}

func TestSnapshotScannerWholeTraceExternalMerge(t *testing.T) {
	root := t.TempDir()
	var paths []string
	for index := 0; index < 9; index++ {
		paths = append(paths, snapshotTestPart(t, root, uint64(index+1), "b", "a"))
	}
	scanner, err := NewProjectedSnapshotScanner(context.Background(), paths, sdk.Projection{Tags: []string{"status"}, SpanIDs: true, Spans: true},
		SnapshotScanOptions{MaxOpenReaders: 2, TempDir: root})
	require.NoError(t, err)
	defer scanner.Close()
	first, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "a", first.Trace.TraceID)
	require.Equal(t, uint64(9), first.SpanCount)
	require.Len(t, first.Trace.Spans, 9)
	require.Len(t, first.Trace.Tags, 1)
	require.Len(t, first.Trace.Tags[0].Values, 9)
	require.Equal(t, []byte("error"), first.Trace.Tags[0].Values[0])
	second, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "b", second.Trace.TraceID)
	require.Equal(t, uint64(9), second.SpanCount)
	require.Equal(t, []byte("payload"), first.Trace.Spans[0], "previous projected storage survives Next")
	_, err = scanner.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	scratch := scanner.scratch
	require.NoError(t, scanner.Close())
	_, err = os.Stat(scratch)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSnapshotScannerMetadataOnlyDoesNotReadSpans(t *testing.T) {
	root := t.TempDir()
	part := snapshotTestPart(t, root, 1, "a")
	require.NoError(t, os.Remove(filepath.Join(part, spansFilename)))
	scanner, err := NewProjectedSnapshotScanner(context.Background(), []string{part}, sdk.Projection{}, SnapshotScanOptions{})
	require.NoError(t, err)
	defer scanner.Close()
	trace, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), trace.SpanCount)
	require.Nil(t, trace.Trace.Spans)
	require.Nil(t, trace.Trace.SpanIDs)
}

func TestSnapshotScannerOversizedSkipsPayloadAndKeepsWholeTrace(t *testing.T) {
	root := t.TempDir()
	part := snapshotTestPart(t, root, 1, "a", "a", "b")
	require.NoError(t, os.Remove(filepath.Join(part, spansFilename)))
	scanner, err := NewProjectedSnapshotScanner(context.Background(), []string{part}, sdk.Projection{Spans: true}, SnapshotScanOptions{MaxTraceBytes: 2})
	require.NoError(t, err)
	defer scanner.Close()
	trace, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.True(t, trace.Oversized)
	require.Equal(t, uint64(2), trace.SpanCount)
	require.Nil(t, trace.Trace.Spans)
	next, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "b", next.Trace.TraceID)
}

func TestSnapshotScannerRejectsCorruptMetadataAndCleansScratch(t *testing.T) {
	root := t.TempDir()
	part := snapshotTestPart(t, root, 1, "a")
	require.NoError(t, os.WriteFile(filepath.Join(part, primaryFilename), []byte("broken"), 0o600))
	_, err := NewProjectedSnapshotScanner(context.Background(), []string{part}, sdk.Projection{}, SnapshotScanOptions{TempDir: root})
	require.Error(t, err)
	scratch, err := filepath.Glob(filepath.Join(root, "trace-snapshot-scan-*"))
	require.NoError(t, err)
	require.Empty(t, scratch)
}

func TestSnapshotGuardPreservesRuntimeCompleteness(t *testing.T) {
	trace := SnapshotTrace{Trace: sdk.TraceBlock{TraceID: "a", MinTS: 100, MaxTS: 200}, CoverageKnown: true}
	keep, reason := GuardSnapshotTrace(context.Background(), trace, 0, 1000, time.Nanosecond, false)
	require.False(t, keep)
	require.Equal(t, string(traceFragmentGuardReasonNoCandidate), reason)
	keep, reason = GuardSnapshotTrace(context.Background(), trace, 100, 1000, time.Nanosecond, false)
	require.True(t, keep)
	require.Equal(t, string(traceFragmentGuardReasonSegmentBoundary), reason)
	keep, reason = GuardSnapshotTrace(context.Background(), trace, 0, 1000, time.Nanosecond, true)
	require.True(t, keep)
	require.Equal(t, string(traceFragmentGuardReasonSamplerKeep), reason)
}

func TestSnapshotScannerConflictingProjectedTypesFailOpen(t *testing.T) {
	root := t.TempDir()
	first := snapshotTestPart(t, root, 1, "a")
	second, release := EncodePart(root, fs.NewLocalFileSystem(), 2, []PartEncoderRow{{
		TraceID: "a", SpanID: "other", Span: []byte("payload"), Timestamp: 2,
		Tags: []PartEncoderTag{{Name: "status", Value: int64(500)}},
	}})
	release()
	scanner, err := NewProjectedSnapshotScanner(context.Background(), []string{first, second}, sdk.Projection{Tags: []string{"status"}}, SnapshotScanOptions{})
	require.NoError(t, err)
	defer scanner.Close()
	result, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.True(t, result.InputConflict)
	require.False(t, result.CoverageKnown)
	keep, reason := GuardSnapshotTrace(context.Background(), *result, 0, 1000, time.Nanosecond, false)
	require.True(t, keep)
	require.Equal(t, string(traceFragmentGuardReasonTraceIncomplete), reason)
}

func TestSnapshotScannerCompressedProjectedTagOverBudgetRetains(t *testing.T) {
	root := t.TempDir()
	part, release := EncodePart(root, fs.NewLocalFileSystem(), 1, []PartEncoderRow{{
		TraceID: "a", SpanID: "one", Span: []byte("body"), Timestamp: 1,
		Tags: []PartEncoderTag{{Name: "status", Value: strings.Repeat("x", 1<<20)}},
	}})
	release()
	scanner, err := NewProjectedSnapshotScanner(context.Background(), []string{part}, sdk.Projection{Tags: []string{"status"}}, SnapshotScanOptions{MaxTraceBytes: 1024})
	require.NoError(t, err)
	defer scanner.Close()
	result, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.True(t, result.Oversized)
	require.Nil(t, result.Trace.Tags)
	require.Equal(t, uint64(1), result.SpanCount)
}

func TestSnapshotScannerAggregateProjectionBudgetRetainsWholeTrace(t *testing.T) {
	root := t.TempDir()
	part, release := EncodePart(root, fs.NewLocalFileSystem(), 1, []PartEncoderRow{{
		TraceID: "a", SpanID: "one", Span: []byte("body"), Timestamp: 1,
		Tags: []PartEncoderTag{{Name: "one", Value: strings.Repeat("x", 700)}, {Name: "two", Value: strings.Repeat("y", 700)}},
	}})
	release()
	scanner, err := NewProjectedSnapshotScanner(context.Background(), []string{part}, sdk.Projection{Tags: []string{"one", "two"}}, SnapshotScanOptions{MaxTraceBytes: 1024})
	require.NoError(t, err)
	defer scanner.Close()
	result, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.True(t, result.Oversized)
	require.Nil(t, result.Trace.Tags)
	require.Equal(t, uint64(1), result.SpanCount)
}
