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

package lifecyclereal

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

const verificationBatchSize = 128

type verificationReport struct {
	BodySignature  string `json:"body_signature_sha256_xor"`
	DuplicateSpans uint64 `json:"duplicate_spans"`
	InputTraces    uint64 `json:"input_traces"`
	KeptTraces     uint64 `json:"kept_traces"`
	DroppedTraces  uint64 `json:"dropped_traces"`
	InputSpans     uint64 `json:"input_spans"`
	KeptSpans      uint64 `json:"kept_spans"`
	DroppedSpans   uint64 `json:"dropped_spans"`
}

type verificationManifest struct {
	Group        string   `json:"group"`
	TableDigest  string   `json:"table_digest"`
	Schema       []byte   `json:"schema"`
	Parts        []string `json:"parts"`
	Traces       uint64   `json:"traces"`
	KeptTraces   uint64   `json:"kept_traces"`
	Spans        uint64   `json:"spans"`
	KeptSpans    uint64   `json:"kept_spans"`
	SegmentStart int64    `json:"segment_start"`
	SegmentEnd   int64    `json:"segment_end"`
}

type expectedTrace struct {
	spans map[string]int
	id    string
	keep  bool
}

// verifyRealTraceMigration checks every captured decision against public warm
// queries, then traverses the timestamp index without the trace-ID fast path.
func verifyRealTraceMigration(t *testing.T, client tracev1.TraceServiceClient, shardPath, artifactDir string,
	schema *databasev1.Trace,
) verificationReport {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(artifactDir, "manifest.json"))
	require.NoError(t, err)
	var manifest verificationManifest
	require.NoError(t, json.Unmarshal(payload, &manifest))
	require.Equal(t, schema.Metadata.Group, manifest.Group)
	pinned := &databasev1.Trace{}
	require.NoError(t, proto.Unmarshal(manifest.Schema, pinned))
	require.True(t, proto.Equal(verificationSchema(schema), verificationSchema(pinned)), "verification must use pinned trace schema")
	decisions, err := os.Open(filepath.Join(artifactDir, "decisions"))
	require.NoError(t, err)
	defer decisions.Close()
	digest := sha256.New()
	_, err = io.Copy(digest, decisions)
	require.NoError(t, err)
	require.Equal(t, manifest.TableDigest, hex.EncodeToString(digest.Sum(nil)))
	_, err = decisions.Seek(0, io.SeekStart)
	require.NoError(t, err)
	reader := bufio.NewReader(decisions)
	parts := make([]string, len(manifest.Parts))
	for index, part := range manifest.Parts {
		parts[index] = filepath.Join(shardPath, filepath.Base(part))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	scanner, err := trace.NewProjectedSnapshotScanner(ctx, parts, sdk.Projection{SpanIDs: true, Spans: true}, trace.SnapshotScanOptions{
		MaxOpenReaders: 16, MaxTraceBytes: 128 << 20, TempDir: t.TempDir(),
	})
	require.NoError(t, err)
	defer scanner.Close()
	var report verificationReport
	var signature [sha256.Size]byte
	batch := make([]expectedTrace, 0, verificationBatchSize)
	for {
		assembled, scanErr := scanner.Next(ctx)
		if errors.Is(scanErr, io.EOF) {
			break
		}
		require.NoError(t, scanErr)
		require.False(t, assembled.Oversized, "verification trace exceeded its projection budget")
		id, keep, decisionErr := readVerificationDecision(reader)
		require.NoError(t, decisionErr)
		require.Equal(t, id, assembled.Trace.TraceID, "decision order must match immutable source trace order")
		require.Equal(t, len(assembled.Trace.SpanIDs), len(assembled.Trace.Spans))
		expected := expectedTrace{id: id, keep: keep, spans: make(map[string]int)}
		report.InputTraces++
		report.InputSpans += assembled.SpanCount
		if keep {
			report.KeptTraces++
			report.KeptSpans += assembled.SpanCount
			for index, body := range assembled.Trace.Spans {
				spanID := assembled.Trace.SpanIDs[index]
				expected.spans[verificationSpanKey(spanID, body)]++
				addVerificationSignature(&signature, id, spanID, body)
			}
		}
		batch = append(batch, expected)
		if len(batch) == verificationBatchSize {
			report.DuplicateSpans += verifyPublicBatch(t, client, schema, manifest, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		report.DuplicateSpans += verifyPublicBatch(t, client, schema, manifest, batch)
	}
	_, _, err = readVerificationDecision(reader)
	require.ErrorIs(t, err, io.EOF, "decision table must cover exactly the source trace inventory")
	require.Equal(t, manifest.Traces, report.InputTraces)
	require.Equal(t, manifest.Spans, report.InputSpans)
	require.Equal(t, manifest.KeptTraces, report.KeptTraces)
	require.Equal(t, manifest.KeptSpans, report.KeptSpans)
	report.DroppedTraces = report.InputTraces - report.KeptTraces
	report.DroppedSpans = report.InputSpans - report.KeptSpans
	require.Positive(t, report.DroppedTraces, "real sampler verification requires intentional drops")
	report.BodySignature = hex.EncodeToString(signature[:])
	verifyPublicTimestampIndex(t, client, schema, manifest, report)
	return report
}

func verificationSchema(schema *databasev1.Trace) *databasev1.Trace {
	normalized := proto.Clone(schema).(*databasev1.Trace)
	normalized.Metadata.Id = 0
	normalized.Metadata.CreateRevision = 0
	normalized.Metadata.ModRevision = 0
	normalized.CreatedAt = nil
	normalized.UpdatedAt = nil
	return normalized
}

func readVerificationDecision(reader io.Reader) (string, bool, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return "", false, err
	}
	length := binary.LittleEndian.Uint32(header[:4])
	if length > 1<<20 || header[4] > 1 {
		return "", false, fmt.Errorf("invalid durable decision record")
	}
	id := make([]byte, length)
	if _, err := io.ReadFull(reader, id); err != nil {
		return "", false, err
	}
	return string(id), header[4] == 1, nil
}

func verificationSpanKey(id string, body []byte) string {
	digest := sha256.Sum256(body)
	return id + "\x00" + hex.EncodeToString(digest[:])
}

func verificationRecordDigest(traceID, spanID string, body []byte) [sha256.Size]byte {
	digest := sha256.New()
	var size [8]byte
	for _, value := range [][]byte{[]byte(traceID), []byte(spanID), body} {
		binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
		digest.Write(size[:])
		digest.Write(value)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func addVerificationSignature(signature *[sha256.Size]byte, traceID, spanID string, body []byte) {
	for index, value := range verificationRecordDigest(traceID, spanID, body) {
		signature[index] ^= value
	}
}

func publicVerificationRequest(schema *databasev1.Trace, manifest verificationManifest) *tracev1.QueryRequest {
	return &tracev1.QueryRequest{
		Groups: []string{manifest.Group}, Name: schema.Metadata.Name, Stages: []string{"warm"}, Limit: verificationBatchSize,
		TimeRange:     &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, manifest.SegmentStart)), End: timestamppb.New(time.Unix(0, manifest.SegmentEnd))},
		TagProjection: []string{schema.SpanIdTagName},
	}
}

func verifyPublicBatch(t *testing.T, client tracev1.TraceServiceClient, schema *databasev1.Trace,
	manifest verificationManifest, batch []expectedTrace,
) uint64 {
	t.Helper()
	ids := make([]string, len(batch))
	expected := make(map[string]expectedTrace, len(batch))
	for index, item := range batch {
		ids[index] = item.id
		expected[item.id] = item
	}
	request := publicVerificationRequest(schema, manifest)
	request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: schema.TraceIdTagName,
		Op:   modelv1.Condition_BINARY_OP_IN, Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: ids}}},
	}}}
	var duplicateSpans uint64
	for _, ordered := range []bool{false, true} {
		if ordered {
			request.OrderBy = &modelv1.QueryOrder{IndexRuleName: "start_time", Sort: modelv1.Sort_SORT_ASC}
		}
		var observedDuplicates uint64
		require.EventuallyWithT(t, func(collector *assert.CollectT) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			response, err := client.Query(ctx, request)
			if !assert.NoError(collector, err) {
				return
			}
			copies, compareErr := publicBatchDuplicates(expected, response)
			if assert.NoError(collector, compareErr) {
				observedDuplicates = copies
			}
		}, time.Minute, 500*time.Millisecond, "public retained/dropped inventory mismatch (ordered=%t)", ordered)
		if !ordered {
			duplicateSpans = observedDuplicates
		}
	}
	return duplicateSpans
}

func comparePublicBatch(expected map[string]expectedTrace, response *tracev1.QueryResponse) error {
	_, err := publicBatchDuplicates(expected, response)
	return err
}

func publicBatchDuplicates(expected map[string]expectedTrace, response *tracev1.QueryResponse) (uint64, error) {
	actual := make(map[string]map[string]int)
	for _, result := range response.Traces {
		item, exists := expected[result.TraceId]
		if !exists || !item.keep {
			fingerprint := sha256.Sum256([]byte(result.TraceId))
			return 0, fmt.Errorf("destination returned unexpected or dropped trace: trace_sha256=%x unexpected=%t", fingerprint[:8], !exists)
		}
		if actual[result.TraceId] == nil {
			actual[result.TraceId] = make(map[string]int)
		}
		for _, span := range result.Spans {
			actual[result.TraceId][verificationSpanKey(span.SpanId, span.Span)]++
		}
	}
	var duplicates uint64
	for id, item := range expected {
		if !item.keep {
			continue
		}
		expectedCopies, actualCopies, missing, extra := 0, 0, 0, 0
		for key, count := range item.spans {
			expectedCopies += count
			if actual[id][key] < count {
				missing += count - actual[id][key]
			}
		}
		for key, count := range actual[id] {
			actualCopies += count
			if item.spans[key] == 0 {
				extra += count
			}
		}
		if missing > 0 || extra > 0 {
			fingerprint := sha256.Sum256([]byte(id))
			return 0, fmt.Errorf("retained span inventory differs: trace_sha256=%x expected=%d actual=%d missing=%d conflicting_or_unexpected=%d",
				fingerprint[:8], expectedCopies, actualCopies, missing, extra)
		}
		// Receiver acknowledgements may precede a durable checkpoint. Retries may
		// replay exact copies; all unique identities and bodies must still match.
		duplicates += uint64(actualCopies - expectedCopies)
	}
	return duplicates, nil
}

func verifyPublicTimestampIndex(t *testing.T, client tracev1.TraceServiceClient, schema *databasev1.Trace,
	manifest verificationManifest, expected verificationReport,
) {
	t.Helper()
	const indexPageSize = 512
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	started := time.Now()
	// Scan once rather than restarting hundreds of successful windows on a
	// global polling deadline. Only individual failed query pages are retried.
	seenTraces := make(map[[sha256.Size]byte]struct{})
	seenSpans := make(map[[sha256.Size]byte]struct{})
	var signature [sha256.Size]byte
	var windows, pages uint64
	for begin := manifest.SegmentStart; begin < manifest.SegmentEnd; {
		require.NoError(t, ctx.Err(), "index scan budget exhausted: windows=%d pages=%d traces=%d spans=%d elapsed=%s",
			windows, pages, len(seenTraces), len(seenSpans), time.Since(started))
		end := begin + int64(5*time.Minute)
		if end > manifest.SegmentEnd {
			end = manifest.SegmentEnd
		}
		request := publicVerificationRequest(schema, manifest)
		request.Limit = indexPageSize
		request.OrderBy = &modelv1.QueryOrder{IndexRuleName: "start_time", Sort: modelv1.Sort_SORT_ASC}
		// API bounds require millisecond precision. Inclusive window edges may
		// repeat records; the fingerprints below remove those copies.
		request.TimeRange.Begin = timestamppb.New(time.Unix(0, begin))
		request.TimeRange.End = timestamppb.New(time.Unix(0, end))
		maxPages := expected.KeptTraces/indexPageSize + 2
		for page := uint64(0); ; page++ {
			require.Less(t, page, maxPages, "index pagination exceeded cap: window=[%d,%d) offset=%d traces=%d spans=%d",
				begin, end, request.Offset, len(seenTraces), len(seenSpans))
			var response *tracev1.QueryResponse
			require.EventuallyWithT(t, func(collector *assert.CollectT) {
				callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
				defer callCancel()
				queried, queryErr := client.Query(callCtx, request)
				if assert.NoError(collector, queryErr, "index query failed: window=[%d,%d) page=%d offset=%d elapsed=%s",
					begin, end, page, request.Offset, time.Since(started)) {
					response = queried
				}
			}, 30*time.Second, 500*time.Millisecond, "timestamp index page must be queryable")
			pages++
			for _, result := range response.Traces {
				seenTraces[sha256.Sum256([]byte(result.TraceId))] = struct{}{}
				require.LessOrEqual(t, uint64(len(seenTraces)), expected.KeptTraces,
					"index returned excess unique traces: window=[%d,%d) page=%d spans=%d", begin, end, page, len(seenSpans))
				for _, span := range result.Spans {
					fingerprint := verificationRecordDigest(result.TraceId, span.SpanId, span.Span)
					if _, exists := seenSpans[fingerprint]; exists {
						continue
					}
					seenSpans[fingerprint] = struct{}{}
					require.LessOrEqual(t, uint64(len(seenSpans)), expected.KeptSpans,
						"index returned excess unique spans: window=[%d,%d) page=%d traces=%d", begin, end, page, len(seenTraces))
					for index, value := range fingerprint {
						signature[index] ^= value
					}
				}
			}
			if len(response.Traces) < indexPageSize {
				break
			}
			request.Offset += indexPageSize
		}
		windows++
		begin = end
	}
	t.Logf("timestamp index scan: windows=%d pages=%d traces=%d spans=%d elapsed=%s",
		windows, pages, len(seenTraces), len(seenSpans), time.Since(started))
	require.Equal(t, expected.KeptTraces, uint64(len(seenTraces)), "timestamp index unique trace count: windows=%d pages=%d", windows, pages)
	require.Equal(t, expected.KeptSpans, uint64(len(seenSpans)), "timestamp index unique span count: windows=%d pages=%d", windows, pages)
	require.Equal(t, expected.BodySignature, hex.EncodeToString(signature[:]), "timestamp index unique body signature: windows=%d pages=%d", windows, pages)
}

// verifyRealHotSource checks one complete source trace through both public
// trace-ID lookup and the independently rebuilt timestamp index before migration.
func verifyRealHotSource(t *testing.T, client tracev1.TraceServiceClient, schema *databasev1.Trace, shardPath string) {
	t.Helper()
	entries, err := os.ReadDir(shardPath)
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 16 {
			continue
		}
		if _, parseErr := strconv.ParseUint(entry.Name(), 16, 64); parseErr == nil {
			paths = append(paths, filepath.Join(shardPath, entry.Name()))
		}
	}
	sort.Strings(paths)
	require.NotEmpty(t, paths, "real source must contain native core parts")
	scanner, err := trace.NewProjectedSnapshotScanner(context.Background(), paths, sdk.Projection{SpanIDs: true, Spans: true},
		trace.SnapshotScanOptions{MaxOpenReaders: 16, MaxTraceBytes: 128 << 20, TempDir: t.TempDir()})
	require.NoError(t, err)
	defer scanner.Close()
	first, err := scanner.Next(context.Background())
	require.NoError(t, err)
	require.False(t, first.Oversized)
	expected := expectedTrace{id: first.Trace.TraceID, keep: true, spans: make(map[string]int)}
	require.Equal(t, len(first.Trace.SpanIDs), len(first.Trace.Spans))
	for index, body := range first.Trace.Spans {
		expected.spans[verificationSpanKey(first.Trace.SpanIDs[index], body)]++
	}
	manifest := verificationManifest{
		Group:        schema.Metadata.Group,
		SegmentStart: first.Trace.MinTS - int64(time.Millisecond),
		SegmentEnd:   first.Trace.MaxTS + int64(time.Millisecond),
	}
	request := publicVerificationRequest(schema, manifest)
	request.Stages = []string{"hot"}
	request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: schema.TraceIdTagName,
		Op:   modelv1.Condition_BINARY_OP_IN, Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{first.Trace.TraceID}}}},
	}}}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		response, queryErr := client.Query(ctx, request)
		return queryErr == nil && comparePublicBatch(map[string]expectedTrace{expected.id: expected}, response) == nil
	}, time.Minute, 500*time.Millisecond, "real source trace must be visible before lifecycle starts")
	request.Criteria = nil
	request.OrderBy = &modelv1.QueryOrder{IndexRuleName: "start_time", Sort: modelv1.Sort_SORT_ASC}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		response, queryErr := client.Query(ctx, request)
		return queryErr == nil && len(response.GetTraces()) > 0
	}, time.Minute, 500*time.Millisecond, "real hot source timestamp index must be visible before lifecycle starts")
}
