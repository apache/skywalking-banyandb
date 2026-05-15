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

// Package main implements the soak-driver CLI for the G5d soak harness.
// It provides three subcommands: record-baseline, replay-and-diff, and pprof-grab.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// Soak fixture constants — used by seed-fixture and the default query
// catalog so the harness does not depend on OAP-driven measure naming.
const (
	soakGroup       = "soak"
	soakMeasure     = "soak_metric"
	soakTagFamily   = "default"
	soakTagService  = "service"
	soakTagInstance = "instance_id"
	soakFieldVal    = "value"
	soakFieldCount  = "count"
)

// catalogEntry holds one query template from the JSON catalog. ID is a
// catalog-unique label used as the baseline key (the proto measure name
// is the same for every entry, so it cannot key the baseline). Request is
// a proto-JSON QueryRequest so proto oneofs (Criteria, TagValue) and
// enums (AggregationFunction, Sort) round-trip correctly — stdlib
// encoding/json cannot populate proto oneof interface fields. TimeRange
// and Limit are injected at runtime by buildQueryRequest.
type catalogEntry struct {
	Request *measurev1.QueryRequest
	ID      string
}

// rawCatalogEntry is the on-disk shape: an "id" label plus a proto-JSON
// "request" object decoded separately via protojson.
type rawCatalogEntry struct {
	ID      string          `json:"id"`
	Request json.RawMessage `json:"request"`
}

// baselineRecord is persisted to disk after record-baseline runs.
type baselineRecord struct {
	QueryName  string            `json:"query_name"`
	DataPoints []json.RawMessage `json:"data_points"`
	Groups     []string          `json:"groups"`
	UntilMs    int64             `json:"until_ms"`
}

// diffReport is written by replay-and-diff.
type diffReport struct {
	RunAt       string       `json:"run_at"`
	Divergences []divergence `json:"divergences"`
	QueriesRun  int          `json:"queries_run"`
	Pass        bool         `json:"pass"`
}

type divergence struct {
	QueryName   string      `json:"query_name"`
	FirstDiffs  []pointDiff `json:"first_diffs,omitempty"`
	BaselineLen int         `json:"baseline_len"`
	ReplayLen   int         `json:"replay_len"`
}

type pointDiff struct {
	Baseline string `json:"baseline"`
	Replay   string `json:"replay"`
	Index    int    `json:"index"`
}

func main() {
	root := &cobra.Command{
		Use:   "soak-driver",
		Short: "G5d soak harness driver — baseline, diff, and pprof capture",
	}
	root.AddCommand(newSeedFixtureCmd(), newRecordBaselineCmd(), newReplayAndDiffCmd(), newPprofGrabCmd())
	if execErr := root.Execute(); execErr != nil {
		_, _ = fmt.Fprintln(os.Stderr, execErr)
		os.Exit(1)
	}
}

// dialInsecure opens an unauthenticated gRPC connection to addr.
func dialInsecure(addr string) (*grpc.ClientConn, error) {
	conn, connErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil {
		return nil, fmt.Errorf("grpc.NewClient %s: %w", addr, connErr)
	}
	return conn, nil
}

// loadCatalog reads the JSON catalog file and returns its entries. The
// catalog is a JSON array of QueryRequest objects; each element is parsed
// with protojson so proto oneofs/enums round-trip. stdlib json splits the
// array, protojson decodes each element.
func loadCatalog(path string) ([]catalogEntry, error) {
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, fmt.Errorf("read catalog %s: %w", path, readErr)
	}
	var rawEntries []rawCatalogEntry
	if unmarshalErr := json.Unmarshal(raw, &rawEntries); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshal catalog array: %w", unmarshalErr)
	}
	entries := make([]catalogEntry, 0, len(rawEntries))
	for idx, rawEntry := range rawEntries {
		if rawEntry.ID == "" {
			return nil, fmt.Errorf("catalog entry %d: missing id", idx)
		}
		req := new(measurev1.QueryRequest)
		if protoErr := protojson.Unmarshal(rawEntry.Request, req); protoErr != nil {
			return nil, fmt.Errorf("unmarshal catalog entry %q: %w", rawEntry.ID, protoErr)
		}
		entries = append(entries, catalogEntry{ID: rawEntry.ID, Request: req})
	}
	return entries, nil
}

// buildQueryRequest constructs a MeasureService QueryRequest from a catalog entry.
// The time range covers [begin, untilMs] where begin defaults to 1 hour before untilMs.
// Limit is set high so a 1000-row seed isn't truncated to BanyanDB's default 100.
func buildQueryRequest(entry catalogEntry, untilMs int64) *measurev1.QueryRequest {
	untilTime := time.UnixMilli(untilMs)
	beginTime := untilTime.Add(-1 * time.Hour)
	req, _ := proto.Clone(entry.Request).(*measurev1.QueryRequest)
	req.TimeRange = &modelv1.TimeRange{
		Begin: timestamppb.New(beginTime),
		End:   timestamppb.New(untilTime),
	}
	if req.GetLimit() == 0 {
		req.Limit = 100000
	}
	return req
}

// newRecordBaselineCmd returns the record-baseline subcommand.
func newRecordBaselineCmd() *cobra.Command {
	var addr, catalogPath, outPath string
	var untilMs int64

	cmd := &cobra.Command{
		Use:   "record-baseline",
		Short: "Query measures up to --until and write a deterministic baseline JSON",
		RunE: func(_ *cobra.Command, _ []string) error {
			entries, loadErr := loadCatalog(catalogPath)
			if loadErr != nil {
				return loadErr
			}
			conn, dialErr := dialInsecure(addr)
			if dialErr != nil {
				return dialErr
			}
			defer conn.Close()

			client := measurev1.NewMeasureServiceClient(conn)
			var records []baselineRecord
			succeeded, failed := 0, 0

			for _, entry := range entries {
				req := buildQueryRequest(entry, untilMs)
				queryName := entry.ID
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				resp, queryErr := client.Query(ctx, req)
				cancel()
				if queryErr != nil {
					failed++
					fmt.Printf("[record-baseline] %s: SKIP (%v)\n", queryName, queryErr)
					continue
				}
				rec := baselineRecord{
					QueryName: queryName,
					Groups:    entry.Request.GetGroups(),
					UntilMs:   untilMs,
				}
				for _, dp := range resp.GetDataPoints() {
					raw, marshalErr := protojson.Marshal(dp)
					if marshalErr != nil {
						return fmt.Errorf("marshal data point for %s: %w", queryName, marshalErr)
					}
					rec.DataPoints = append(rec.DataPoints, json.RawMessage(raw))
				}
				records = append(records, rec)
				succeeded++
				fmt.Printf("[record-baseline] %s: %d data points\n", queryName, len(rec.DataPoints))
			}
			if succeeded == 0 {
				return fmt.Errorf("record-baseline: all %d catalog queries failed (no usable baseline)", failed)
			}
			fmt.Printf("[record-baseline] %d queries succeeded, %d skipped\n", succeeded, failed)

			out, createErr := os.Create(outPath)
			if createErr != nil {
				return fmt.Errorf("create output %s: %w", outPath, createErr)
			}
			defer out.Close()

			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if encErr := enc.Encode(records); encErr != nil {
				return fmt.Errorf("encode baseline: %w", encErr)
			}
			fmt.Printf("[record-baseline] written to %s\n", outPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "localhost:17912", "BanyanDB gRPC address")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Path to query catalog JSON")
	cmd.Flags().Int64Var(&untilMs, "until", 0, "Upper bound for time range (unix milliseconds)")
	cmd.Flags().StringVar(&outPath, "out", "baseline.json", "Output path for baseline JSON")
	_ = cmd.MarkFlagRequired("catalog")
	_ = cmd.MarkFlagRequired("until")
	return cmd
}

// newReplayAndDiffCmd returns the replay-and-diff subcommand.
func newReplayAndDiffCmd() *cobra.Command {
	var addr, catalogPath, baselinePath, reportPath string

	cmd := &cobra.Command{
		Use:   "replay-and-diff",
		Short: "Re-run catalog queries and compare against baseline; exit non-zero on divergence",
		RunE: func(_ *cobra.Command, _ []string) error {
			raw, readErr := os.ReadFile(baselinePath)
			if readErr != nil {
				return fmt.Errorf("read baseline %s: %w", baselinePath, readErr)
			}
			var records []baselineRecord
			if unmarshalErr := json.Unmarshal(raw, &records); unmarshalErr != nil {
				return fmt.Errorf("unmarshal baseline: %w", unmarshalErr)
			}

			entries, loadErr := loadCatalog(catalogPath)
			if loadErr != nil {
				return loadErr
			}

			conn, dialErr := dialInsecure(addr)
			if dialErr != nil {
				return dialErr
			}
			defer conn.Close()

			client := measurev1.NewMeasureServiceClient(conn)
			report := diffReport{
				RunAt: time.Now().UTC().Format(time.RFC3339),
				Pass:  true,
			}

			// Build a map from name to baseline record for O(1) lookup.
			baselineMap := make(map[string]baselineRecord, len(records))
			for _, rec := range records {
				baselineMap[rec.QueryName] = rec
			}

			for _, entry := range entries {
				queryName := entry.ID
				rec, ok := baselineMap[queryName]
				if !ok {
					// Catalog has a query the baseline doesn't (e.g. baseline
					// skipped it because the measure wasn't installed yet).
					// Skip the diff for this entry rather than failing the
					// whole replay.
					fmt.Printf("[replay-and-diff] %s: SKIP (no baseline)\n", queryName)
					continue
				}
				req := buildQueryRequest(entry, rec.UntilMs)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				resp, queryErr := client.Query(ctx, req)
				cancel()
				if queryErr != nil {
					// Treat replay-side query failure as a divergence so the
					// pass/fail signal still flips, but don't abort early —
					// we want to attempt every catalog entry.
					report.Divergences = append(report.Divergences, divergence{
						QueryName: queryName,
					})
					report.Pass = false
					fmt.Printf("[replay-and-diff] %s: FAIL (%v)\n", queryName, queryErr)
					continue
				}
				report.QueriesRun++

				replayDPs := resp.GetDataPoints()
				if len(replayDPs) != len(rec.DataPoints) {
					div := divergence{
						QueryName:   queryName,
						BaselineLen: len(rec.DataPoints),
						ReplayLen:   len(replayDPs),
					}
					report.Divergences = append(report.Divergences, div)
					report.Pass = false
					continue
				}

				div := divergence{QueryName: queryName, BaselineLen: len(rec.DataPoints), ReplayLen: len(replayDPs)}
				hasDiff := false
				for idx, baselineRaw := range rec.DataPoints {
					baselineDP := new(measurev1.DataPoint)
					if parseErr := protojson.Unmarshal(baselineRaw, baselineDP); parseErr != nil {
						return fmt.Errorf("unmarshal baseline dp %d for %s: %w", idx, queryName, parseErr)
					}
					if !proto.Equal(baselineDP, replayDPs[idx]) {
						hasDiff = true
						if len(div.FirstDiffs) < 3 {
							div.FirstDiffs = append(div.FirstDiffs, pointDiff{
								Index:    idx,
								Baseline: baselineDP.String(),
								Replay:   replayDPs[idx].String(),
							})
						}
					}
				}
				if hasDiff {
					report.Divergences = append(report.Divergences, div)
					report.Pass = false
				}
			}

			outFile, createErr := os.Create(reportPath)
			if createErr != nil {
				return fmt.Errorf("create report %s: %w", reportPath, createErr)
			}
			defer outFile.Close()

			enc := json.NewEncoder(outFile)
			enc.SetIndent("", "  ")
			if encErr := enc.Encode(report); encErr != nil {
				return fmt.Errorf("encode report: %w", encErr)
			}
			fmt.Printf("[replay-and-diff] %d queries run, %d divergences — %s\n",
				report.QueriesRun, len(report.Divergences), map[bool]string{true: "PASS", false: "FAIL"}[report.Pass])

			if !report.Pass {
				return fmt.Errorf("parity check failed: %d divergences found", len(report.Divergences))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "localhost:17912", "BanyanDB gRPC address")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Path to query catalog JSON")
	cmd.Flags().StringVar(&baselinePath, "baseline", "baseline.json", "Path to baseline JSON produced by record-baseline")
	cmd.Flags().StringVar(&reportPath, "report", "diff.json", "Output path for diff report JSON")
	_ = cmd.MarkFlagRequired("catalog")
	return cmd
}

// newPprofGrabCmd returns the pprof-grab subcommand.
func newPprofGrabCmd() *cobra.Command {
	var addr, outDir string

	cmd := &cobra.Command{
		Use:   "pprof-grab",
		Short: "Capture heap and goroutine pprof profiles; print goroutine count to stdout",
		RunE: func(_ *cobra.Command, _ []string) error {
			if mkdirErr := os.MkdirAll(outDir, 0o750); mkdirErr != nil {
				return fmt.Errorf("mkdir %s: %w", outDir, mkdirErr)
			}
			ts := strconv.FormatInt(time.Now().Unix(), 10)

			heapPath := filepath.Join(outDir, fmt.Sprintf("heap-%s.pb.gz", ts))
			goroutinePath := filepath.Join(outDir, fmt.Sprintf("goroutine-%s.txt", ts))

			baseURL := fmt.Sprintf("http://%s/debug/pprof", addr)
			if fetchErr := fetchGzip(baseURL+"/heap", heapPath); fetchErr != nil {
				return fmt.Errorf("fetch heap profile: %w", fetchErr)
			}

			goroutineBody, fetchErr := fetchBytes(baseURL + "/goroutine?debug=1")
			if fetchErr != nil {
				return fmt.Errorf("fetch goroutine profile: %w", fetchErr)
			}
			if writeErr := os.WriteFile(goroutinePath, goroutineBody, 0o600); writeErr != nil {
				return fmt.Errorf("write goroutine file: %w", writeErr)
			}

			count := countGoroutines(goroutineBody)
			fmt.Printf("%d\n", count)
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "localhost:6060", "BanyanDB pprof/debug HTTP address (host:port)")
	cmd.Flags().StringVar(&outDir, "out-dir", ".", "Directory to write profile files into")
	return cmd
}

// newSeedFixtureCmd creates a deterministic Group + Measure schema and
// writes N data points into it, so the parity-diff path does not depend
// on OAP-managed measure naming. Idempotent on the schema side: if the
// group/measure already exist, we proceed straight to the writes.
//
// Layout:
//
//	group   = "soak"
//	measure = "soak_metric"
//	tags    = service (string), instance_id (int)   — family "default"
//	fields  = value (int), count (int)
//	rows    = N rows at 1-second intervals ending now-1 minute
//
// Writes use the streaming MeasureService.Write RPC. After completion,
// the highest timestamp written is printed to stdout — pass it as
// --until to record-baseline so the time-bounded queries hit only this
// fixture.
func newSeedFixtureCmd() *cobra.Command {
	var addr string
	var rows int

	cmd := &cobra.Command{
		Use:   "seed-fixture",
		Short: "Create a deterministic group/measure and write N rows",
		RunE: func(_ *cobra.Command, _ []string) error {
			conn, dialErr := dialInsecure(addr)
			if dialErr != nil {
				return dialErr
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			groupClient := databasev1.NewGroupRegistryServiceClient(conn)
			if _, createErr := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: &commonv1.Group{
					Metadata: &commonv1.Metadata{Name: soakGroup},
					Catalog:  commonv1.Catalog_CATALOG_MEASURE,
					ResourceOpts: &commonv1.ResourceOpts{
						ShardNum: 1,
						SegmentInterval: &commonv1.IntervalRule{
							Unit: commonv1.IntervalRule_UNIT_DAY,
							Num:  1,
						},
						Ttl: &commonv1.IntervalRule{
							Unit: commonv1.IntervalRule_UNIT_DAY,
							Num:  7,
						},
					},
				},
			}); createErr != nil && !strings.Contains(createErr.Error(), "already exist") {
				return fmt.Errorf("create group: %w", createErr)
			}

			measureClient := databasev1.NewMeasureRegistryServiceClient(conn)
			if _, createErr := measureClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: &databasev1.Measure{
					Metadata: &commonv1.Metadata{Name: soakMeasure, Group: soakGroup},
					TagFamilies: []*databasev1.TagFamilySpec{{
						Name: soakTagFamily,
						Tags: []*databasev1.TagSpec{
							{Name: soakTagService, Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: soakTagInstance, Type: databasev1.TagType_TAG_TYPE_INT},
						},
					}},
					Fields: []*databasev1.FieldSpec{
						{
							Name:              soakFieldVal,
							FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
							EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
							CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
						},
						{
							Name:              soakFieldCount,
							FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
							EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
							CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
						},
					},
					Entity: &databasev1.Entity{TagNames: []string{soakTagService, soakTagInstance}},
				},
			}); createErr != nil && !strings.Contains(createErr.Error(), "already exist") {
				return fmt.Errorf("create measure: %w", createErr)
			}

			writeClient := measurev1.NewMeasureServiceClient(conn)
			writeStream, streamErr := writeClient.Write(ctx)
			if streamErr != nil {
				return fmt.Errorf("open write stream: %w", streamErr)
			}

			// BanyanDB's timestamp validator rejects sub-millisecond
			// precision (Status_STATUS_INVALID_TIMESTAMP). Truncate to
			// millisecond before deriving the seed window.
			now := time.Now().Truncate(time.Millisecond).Add(-1 * time.Minute)
			lowestMs := now.Add(-time.Duration(rows) * time.Second).UnixMilli()
			highestMs := now.Add(-time.Second).UnixMilli()
			md := &commonv1.Metadata{Name: soakMeasure, Group: soakGroup}
			for i := range rows {
				ts := now.Add(-time.Duration(rows-1-i) * time.Second)
				dp := &measurev1.DataPointValue{
					Timestamp: timestamppb.New(ts),
					TagFamilies: []*modelv1.TagFamilyForWrite{{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%d", i%4)}}},
							{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i % 8)}}},
						},
					}},
					Fields: []*modelv1.FieldValue{
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(1000 + i)}}},
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(1)}}},
					},
				}
				if sendErr := writeStream.Send(&measurev1.WriteRequest{
					Metadata:  md,
					DataPoint: dp,
					MessageId: uint64(time.Now().UnixNano()),
				}); sendErr != nil {
					return fmt.Errorf("send dp %d: %w", i, sendErr)
				}
			}
			if closeErr := writeStream.CloseSend(); closeErr != nil {
				return fmt.Errorf("close write stream: %w", closeErr)
			}
			// Drain server responses; status != STATUS_SUCCEED means the
			// write was rejected (e.g. unknown measure, schema mismatch).
			succeeded, badStatus := 0, 0
			var firstBadStatus string
			for {
				resp, recvErr := writeStream.Recv()
				if recvErr == io.EOF {
					break
				}
				if recvErr != nil {
					return fmt.Errorf("recv ack: %w", recvErr)
				}
				if resp.GetStatus() == modelv1.Status_STATUS_SUCCEED.String() {
					succeeded++
					continue
				}
				badStatus++
				if firstBadStatus == "" {
					firstBadStatus = resp.GetStatus()
				}
			}
			fmt.Printf("[seed-fixture] write acks: %d succeeded, %d failed (first status: %q)\n",
				succeeded, badStatus, firstBadStatus)
			if succeeded == 0 {
				return fmt.Errorf("seed-fixture: 0 of %d writes succeeded; first bad status: %q", rows, firstBadStatus)
			}

			fmt.Printf("[seed-fixture] wrote %d rows to %s/%s, ts range [%d, %d] ms\n",
				rows, soakGroup, soakMeasure, lowestMs, highestMs)

			// Wait until the writes are queryable. BanyanDB buffers
			// measure data in memory and flushes on a timer (default
			// 5s) — querying immediately can return zero results. Poll
			// the measure with a 1-hour time range covering the seed
			// window until at least 90% of the rows are visible, with a
			// 60s overall cap.
			queryClient := measurev1.NewMeasureServiceClient(conn)
			deadline := time.Now().Add(60 * time.Second)
			req := &measurev1.QueryRequest{
				Name:   soakMeasure,
				Groups: []string{soakGroup},
				TimeRange: &modelv1.TimeRange{
					Begin: timestamppb.New(time.UnixMilli(lowestMs).Add(-time.Second)),
					End:   timestamppb.New(time.UnixMilli(highestMs).Add(time.Second)),
				},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{soakFieldVal, soakFieldCount}},
				Limit:           uint32(rows * 2),
			}
			minVisible := int(float64(rows) * 0.9)
			var lastSeen int
			for time.Now().Before(deadline) {
				qctx, qcancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, qerr := queryClient.Query(qctx, req)
				qcancel()
				if qerr == nil && resp != nil {
					lastSeen = len(resp.GetDataPoints())
					if lastSeen >= minVisible {
						fmt.Printf("[seed-fixture] %d rows visible to query\n", lastSeen)
						fmt.Printf("%d\n", highestMs)
						return nil
					}
				}
				time.Sleep(2 * time.Second)
			}
			return fmt.Errorf("seed-fixture: only %d of %d rows visible after 60s — flush/index lag", lastSeen, rows)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "localhost:17912", "BanyanDB gRPC address")
	cmd.Flags().IntVar(&rows, "rows", 1000, "number of data points to write")
	return cmd
}

// fetchGzip downloads url and writes the body as a gzip file at dst.
// The pprof heap endpoint already returns a gzip-compressed protobuf, so we
// just pipe the bytes through.
func fetchGzip(url, dst string) error {
	body, fetchErr := fetchBytes(url)
	if fetchErr != nil {
		return fetchErr
	}
	f, createErr := os.Create(dst)
	if createErr != nil {
		return fmt.Errorf("create %s: %w", dst, createErr)
	}
	defer f.Close()

	// The pprof heap endpoint returns raw gzip; write it directly.
	gw := gzip.NewWriter(f)
	if _, writeErr := gw.Write(body); writeErr != nil {
		return fmt.Errorf("gzip write: %w", writeErr)
	}
	return gw.Close()
}

// fetchBytes issues a GET request and returns the response body.
func fetchBytes(url string) ([]byte, error) {
	resp, getErr := http.Get(url) //nolint:gosec,noctx
	if getErr != nil {
		return nil, fmt.Errorf("GET %s: %w", url, getErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read body from %s: %w", url, readErr)
	}
	return data, nil
}

// countGoroutines parses the text output of /debug/pprof/goroutine?debug=1
// and returns the total goroutine count from the first "goroutine N [" lines.
func countGoroutines(body []byte) int {
	count := 0
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "goroutine ") && strings.Contains(line, " [") {
			count++
		}
	}
	return count
}
