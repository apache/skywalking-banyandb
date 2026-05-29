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

// Package lifecycle_test is the test cases for the lifecycle package.
package lifecycle_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/lifecycle"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measureTestData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	streamTestData "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	topNTestData "github.com/apache/skywalking-banyandb/test/cases/topn/data"
	traceTestData "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

// SharedContext is the shared context for the snapshot test cases.
var SharedContext helpers.LifecycleSharedContext

var _ = ginkgo.Describe("Lifecycle", func() {
	ginkgo.It("should migrate data once correctly", func() {
		dir, err := os.MkdirTemp("", "lifecycle-restore-dest")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(dir)
		pf := filepath.Join(dir, "progress.json")
		rf := filepath.Join(dir, "report")
		lifecycleCmd := lifecycle.NewCommand()
		args := []string{
			"--grpc-addr", SharedContext.DataAddr,
			"--stream-root-path", SharedContext.SrcDir,
			"--measure-root-path", SharedContext.SrcDir,
			"--trace-root-path", SharedContext.SrcDir,
			"--progress-file", pf,
			"--report-dir", rf,
		}
		args = append(args, SharedContext.MetadataFlags...)
		lifecycleCmd.SetArgs(args)
		err = lifecycleCmd.Execute()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifySourceDirectoriesAfterMigration()
		verifyDestinationDirectoriesAfterMigration()
		verifyMigrationReport(rf)
		conn, err := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer func() {
			if conn != nil {
				_ = conn.Close()
			}
		}()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		sc := helpers.SharedContext{
			Connection: conn,
			BaseTime:   SharedContext.BaseTime,
		}
		// Verify measure data lifecycle stages
		verifyLifecycleStages(sc, measureTestData.VerifyFn, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})

		// Verify stream data lifecycle stages
		verifyLifecycleStages(sc, streamTestData.VerifyFn, helpers.Args{
			Input:           "all",
			Duration:        time.Hour,
			IgnoreElementID: true,
		})

		// Verify topN data lifecycle stages
		verifyLifecycleStages(sc, topNTestData.VerifyFn, helpers.Args{
			Input:    "aggr_desc",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})

		// Verify trace data lifecycle stages
		verifyLifecycleStages(sc, traceTestData.VerifyFn, helpers.Args{
			Input:    "having_query_tag",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})
	})
	ginkgo.It("should migrate data correctly with a scheduler", func() {
		dir, err := os.MkdirTemp("", "lifecycle-restore-dest")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(dir)
		pf := filepath.Join(dir, "progress.json")
		rf := filepath.Join(dir, "report")
		lifecycleCmd := lifecycle.NewCommand()
		args := []string{
			"--grpc-addr", SharedContext.DataAddr,
			"--stream-root-path", SharedContext.SrcDir,
			"--measure-root-path", SharedContext.SrcDir,
			"--trace-root-path", SharedContext.SrcDir,
			"--progress-file", pf,
			"--report-dir", rf,
			"--schedule", "@every 5s",
			"--max-execution-times", "2",
		}
		args = append(args, SharedContext.MetadataFlags...)
		lifecycleCmd.SetArgs(args)
		err = lifecycleCmd.Execute()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifySourceDirectoriesAfterMigration()
		verifyDestinationDirectoriesAfterMigration()
		verifyMigrationReport(rf)
		conn, err := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer func() {
			if conn != nil {
				_ = conn.Close()
			}
		}()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		sc := helpers.SharedContext{
			Connection: conn,
			BaseTime:   SharedContext.BaseTime,
		}
		// Verify measure data lifecycle stages
		verifyLifecycleStages(sc, measureTestData.VerifyFn, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})

		// Verify stream data lifecycle stages
		verifyLifecycleStages(sc, streamTestData.VerifyFn, helpers.Args{
			Input:           "all",
			Duration:        time.Hour,
			IgnoreElementID: true,
		})

		// Verify topN data lifecycle stages
		verifyLifecycleStages(sc, topNTestData.VerifyFn, helpers.Args{
			Input:    "aggr_desc",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})

		// Verify trace data lifecycle stages
		verifyLifecycleStages(sc, traceTestData.VerifyFn, helpers.Args{
			Input:    "having_query_tag",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})
	})
})

func verifyLifecycleStages(sc helpers.SharedContext, verifyFn func(gomega.Gomega, helpers.SharedContext, helpers.Args), args helpers.Args) {
	// Initial verification expecting error before migration
	verifyFn(gomega.Default, sc, helpers.Args{
		Input:    args.Input,
		Duration: args.Duration,
		Offset:   args.Offset,
		WantErr:  true,
		Stages:   args.Stages,
	})

	// Verify hot+warm stages exist after migration
	gomega.Eventually(func(innerGm gomega.Gomega) {
		verifyFn(innerGm, sc, helpers.Args{
			Input:           args.Input,
			Duration:        args.Duration,
			Offset:          args.Offset,
			Stages:          []string{"hot", "warm"},
			IgnoreElementID: args.IgnoreElementID,
		})
	}, flags.EventuallyTimeout).Should(gomega.Succeed())

	// Verify warm stage only after retention
	gomega.Eventually(func(innerGm gomega.Gomega) {
		verifyFn(innerGm, sc, helpers.Args{
			Input:           args.Input,
			Duration:        args.Duration,
			Offset:          args.Offset,
			Stages:          []string{"warm"},
			IgnoreElementID: args.IgnoreElementID,
		})
	}, flags.EventuallyTimeout).Should(gomega.Succeed())

	// Verify hot stage is empty after retention
	verifyFn(gomega.Default, sc, helpers.Args{
		Input:           args.Input,
		Duration:        args.Duration,
		Offset:          args.Offset,
		WantEmpty:       true,
		Stages:          []string{"hot"},
		IgnoreElementID: args.IgnoreElementID,
	})
}

func verifySourceDirectoriesAfterMigration() {
	streamSrcPath := filepath.Join(SharedContext.SrcDir, "stream", "data", "default")
	streamEntries, err := os.ReadDir(streamSrcPath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Stream source directory should exist")

	hasLockFileOnly := verifyOnlyLockFileExists(streamEntries)
	gomega.Expect(hasLockFileOnly).To(gomega.BeTrue(), "Stream source directory should only contain a lock file")

	measureSrcPath := filepath.Join(SharedContext.SrcDir, "measure", "data", "sw_metric")
	measureEntries, err := os.ReadDir(measureSrcPath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Measure source directory should exist")

	hasLockFileOnly = verifyOnlyLockFileExists(measureEntries)
	gomega.Expect(hasLockFileOnly).To(gomega.BeTrue(), "Measure source directory should only contain a lock file")
}

func verifyDestinationDirectoriesAfterMigration() {
	streamDestPath := filepath.Join(SharedContext.DestDir, "stream", "data", "default")
	streamEntries, err := os.ReadDir(streamDestPath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Stream destination directory should exist")

	hasLockFile, hasSegFolder := verifyLockFileAndSegFolder(streamEntries)
	gomega.Expect(hasLockFile).To(gomega.BeTrue(), "Stream destination should have a lock file")
	gomega.Expect(hasSegFolder).To(gomega.BeTrue(), "Stream destination should have a seg-xxx folder")

	measureDestPath := filepath.Join(SharedContext.DestDir, "measure", "data", "sw_metric")
	measureEntries, err := os.ReadDir(measureDestPath)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Measure destination directory should exist")

	hasLockFile, hasSegFolder = verifyLockFileAndSegFolder(measureEntries)
	gomega.Expect(hasLockFile).To(gomega.BeTrue(), "Measure destination should have a lock file")
	gomega.Expect(hasSegFolder).To(gomega.BeTrue(), "Measure destination should have a seg-xxx folder")
}

func verifyOnlyLockFileExists(entries []fs.DirEntry) bool {
	if len(entries) != 1 {
		return false
	}

	return !entries[0].IsDir() && entries[0].Name() == "lock"
}

func verifyLockFileAndSegFolder(entries []fs.DirEntry) (hasLockFile bool, hasSegFolder bool) {
	for _, entry := range entries {
		if !entry.IsDir() && entry.Name() == "lock" {
			hasLockFile = true
		}
		if entry.IsDir() && len(entry.Name()) >= 4 && entry.Name()[:4] == "seg-" {
			hasSegFolder = true
		}
	}
	return hasLockFile, hasSegFolder
}

// verifyMigrationReport reads every JSON report file generated under rf
// and asserts the invariants of the comprehensive migration report:
//
//   - migration_status.completion_rate is 100 when the cycle had scheduled
//     groups; the denominator equals the scheduled group set.
//   - Every per-resource block (parts / series / element_index) reaches
//     100 % when the catalog had work to do, or 0 % when the cycle had
//     nothing to migrate (total == 0).
//   - The trace_migration block is present with parts + series fields
//     aligned to stream_migration / measure_migration; snapshot_info
//     includes trace_dir; errors includes trace_parts / trace_series keys.
//   - All errors.* maps are empty for a clean cycle.
func verifyMigrationReport(rf string) {
	rEntries, err := os.ReadDir(rf)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Report directory should exist")
	gomega.Expect(rEntries).NotTo(gomega.BeEmpty(), "Report directory should contain files")

	jsonReports := 0
	for _, entry := range rEntries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		jsonReports++
		path := filepath.Join(rf, entry.Name())
		raw, readErr := os.ReadFile(path)
		gomega.Expect(readErr).NotTo(gomega.HaveOccurred(), "Report file %s should be readable", path)

		var report map[string]interface{}
		gomega.Expect(json.Unmarshal(raw, &report)).To(gomega.Succeed(), "Report file %s should be valid JSON", path)

		gomega.Expect(report).To(gomega.HaveKey("summary"), "report %s missing summary", path)
		gomega.Expect(report).To(gomega.HaveKey("errors"), "report %s missing errors", path)
		gomega.Expect(report).To(gomega.HaveKey("snapshot_info"), "report %s missing snapshot_info", path)
		gomega.Expect(report["report_version"]).To(gomega.Equal("2.1"), "report %s has unexpected report_version", path)

		// snapshot_info: trace_dir must surface alongside stream/measure.
		snap, ok := report["snapshot_info"].(map[string]interface{})
		gomega.Expect(ok).To(gomega.BeTrue(), "snapshot_info must be a map in %s", path)
		gomega.Expect(snap).To(gomega.HaveKey("trace_dir"), "snapshot_info must include trace_dir in %s", path)

		summary, ok := report["summary"].(map[string]interface{})
		gomega.Expect(ok).To(gomega.BeTrue(), "summary must be a map in %s", path)

		// migration_status: when total_groups=0 the cycle had no scheduled
		// work (e.g. snapshots were empty); the rate is 0 by construction.
		// Otherwise the report reflects a fully completed cycle, so the
		// rate must be 100.
		ms, ok := summary["migration_status"].(map[string]interface{})
		gomega.Expect(ok).To(gomega.BeTrue(), "summary.migration_status must be a map in %s", path)
		if asInt(ms["total_groups"]) == 0 {
			gomega.Expect(asFloat(ms["completion_rate"])).To(gomega.BeNumerically("~", 0.0, 1e-9),
				"migration_status.completion_rate must be 0 when total_groups=0 in %s", path)
		} else {
			gomega.Expect(asFloat(ms["completion_rate"])).To(gomega.BeNumerically("~", 100.0, 1e-9),
				"migration_status.completion_rate should be 100 in %s", path)
		}

		// Per-resource invariants.
		verifyAllRatesAt100(summary, "stream_migration", []string{"parts", "series", "element_index"}, path)
		verifyAllRatesAt100(summary, "measure_migration", []string{"parts", "series"}, path)

		// trace_migration: block must exist with parts + series.
		verifyAllRatesAt100(summary, "trace_migration", []string{"parts", "series"}, path)

		// errors must include trace_* keys and stay empty.
		errs, ok := report["errors"].(map[string]interface{})
		gomega.Expect(ok).To(gomega.BeTrue(), "errors must be a map in %s", path)
		for _, key := range []string{
			"stream_parts", "stream_series", "stream_element_index",
			"measure_parts", "measure_series",
			"trace_parts", "trace_series",
		} {
			v, found := errs[key]
			gomega.Expect(found).To(gomega.BeTrue(), "errors.%s must be present in %s", key, path)
			errMap, isMap := v.(map[string]interface{})
			gomega.Expect(isMap).To(gomega.BeTrue(), "errors.%s must be a map in %s", key, path)
			gomega.Expect(errMap).To(gomega.BeEmpty(), "errors.%s must be empty for a clean cycle in %s", key, path)
		}

		// report_version 2.1: every catalog carries a sync_breakdown block, and
		// for measure/stream (where chunk-sync and row-replay share the part
		// unit) chunk_sync_parts + row_replay_parts must reconcile to the
		// catalog's completed parts. Trace is shard-batched so the sum check
		// is skipped.
		verifySyncBreakdown(summary, "stream_migration", "chunk_sync_parts", true, path)
		verifySyncBreakdown(summary, "measure_migration", "chunk_sync_parts", true, path)
		verifySyncBreakdown(summary, "trace_migration", "chunk_sync_shards", false, path)

		// row_replay_node_errors must surface and stay empty for a clean cycle.
		nodeErrs, found := errs["row_replay_node_errors"]
		gomega.Expect(found).To(gomega.BeTrue(), "errors.row_replay_node_errors must be present in %s", path)
		nodeErrMap, isMap := nodeErrs.(map[string]interface{})
		gomega.Expect(isMap).To(gomega.BeTrue(), "errors.row_replay_node_errors must be a map in %s", path)
		gomega.Expect(nodeErrMap).To(gomega.BeEmpty(), "errors.row_replay_node_errors must be empty for a clean cycle in %s", path)
	}
	gomega.Expect(jsonReports).To(gomega.BeNumerically(">", 0), "no JSON report files found under %s", rf)
}

// verifySyncBreakdown asserts a catalog's sync_breakdown block has a _total
// entry with the chunk key (chunk_sync_parts or chunk_sync_shards) plus
// row_replay_parts / row_replay_rows. When sumInvariant is set the _total
// chunk + row-replay parts must equal the catalog's completed parts.
func verifySyncBreakdown(summary map[string]interface{}, catalog, chunkKey string, sumInvariant bool, path string) {
	cat, ok := summary[catalog].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s must be a map in %s", catalog, path)
	sb, ok := cat["sync_breakdown"].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s.sync_breakdown must be a map in %s", catalog, path)
	total, ok := sb["_total"].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s.sync_breakdown._total must be a map in %s", catalog, path)
	for _, key := range []string{chunkKey, "row_replay_parts", "row_replay_rows"} {
		gomega.Expect(total).To(gomega.HaveKey(key), "summary.%s.sync_breakdown._total missing %s in %s", catalog, key, path)
	}
	if !sumInvariant {
		return
	}
	parts, ok := cat["parts"].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s.parts must be a map in %s", catalog, path)
	completed := asInt(parts["completed"])
	chunk := asInt(total[chunkKey])
	replay := asInt(total["row_replay_parts"])
	gomega.Expect(chunk+replay).To(gomega.Equal(completed),
		"summary.%s sync_breakdown chunk(%d)+row_replay(%d) must equal parts.completed(%d) in %s",
		catalog, chunk, replay, completed, path)
}

// verifyCrossSegmentReport asserts the locked per-group sync_breakdown of a
// cross-segment migration run. The seeded data is deterministic, so the
// row-replay row count is an absolute value; part/shard counts are pinned too
// because every group here uses shard_num=1 (or a single entity) and one flush.
func verifyCrossSegmentReport(reportDir, catalog, group, chunkKey string, wantChunk, wantReplayParts, wantReplayRows int) {
	entries, err := os.ReadDir(reportDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "cross-segment report dir %s must exist", reportDir)
	latest := ""
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" && e.Name() > latest {
			latest = e.Name()
		}
	}
	gomega.Expect(latest).NotTo(gomega.BeEmpty(), "no JSON report under %s", reportDir)
	raw, readErr := os.ReadFile(filepath.Join(reportDir, latest))
	gomega.Expect(readErr).NotTo(gomega.HaveOccurred(), "report %s must be readable", latest)
	var report map[string]interface{}
	gomega.Expect(json.Unmarshal(raw, &report)).To(gomega.Succeed(), "report %s must be valid JSON", latest)

	summary, ok := report["summary"].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary must be a map in %s", latest)
	cat, ok := summary[catalog].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s must be a map in %s", catalog, latest)
	sb, ok := cat["sync_breakdown"].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s.sync_breakdown must be a map in %s", catalog, latest)
	entry, ok := sb[group].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "sync_breakdown[%s] must exist in %s", group, latest)

	gomega.Expect(asInt(entry[chunkKey])).To(gomega.Equal(wantChunk),
		"sync_breakdown[%s].%s must be %d in %s", group, chunkKey, wantChunk, latest)
	gomega.Expect(asInt(entry["row_replay_parts"])).To(gomega.Equal(wantReplayParts),
		"sync_breakdown[%s].row_replay_parts must be %d in %s", group, wantReplayParts, latest)
	gomega.Expect(asInt(entry["row_replay_rows"])).To(gomega.Equal(wantReplayRows),
		"sync_breakdown[%s].row_replay_rows must be %d in %s", group, wantReplayRows, latest)
}

// verifyAllRatesAt100 enforces the per-resource accounting invariant: when
// total == 0 the cycle had no work for that resource and the rate stays at
// 0; otherwise completed must equal total and the rate must be 100.
func verifyAllRatesAt100(summary map[string]interface{}, catalog string, resources []string, path string) {
	cat, ok := summary[catalog].(map[string]interface{})
	gomega.Expect(ok).To(gomega.BeTrue(), "summary.%s must be a map in %s", catalog, path)
	for _, r := range resources {
		res, isMap := cat[r].(map[string]interface{})
		gomega.Expect(isMap).To(gomega.BeTrue(), "summary.%s.%s must be a map in %s", catalog, r, path)
		gomega.Expect(res).To(gomega.HaveKey("total"), "summary.%s.%s missing total in %s", catalog, r, path)
		gomega.Expect(res).To(gomega.HaveKey("completed"), "summary.%s.%s missing completed in %s", catalog, r, path)
		gomega.Expect(res).To(gomega.HaveKey("errors"), "summary.%s.%s missing errors in %s", catalog, r, path)
		gomega.Expect(res).To(gomega.HaveKey("completion_rate"), "summary.%s.%s missing completion_rate in %s", catalog, r, path)

		total := asInt(res["total"])
		completed := asInt(res["completed"])
		errors := asInt(res["errors"])
		rate := asFloat(res["completion_rate"])

		gomega.Expect(errors).To(gomega.Equal(0), "summary.%s.%s.errors must be 0 in %s", catalog, r, path)
		if total == 0 {
			gomega.Expect(rate).To(gomega.BeNumerically("~", 0.0, 1e-9),
				"summary.%s.%s.completion_rate must be 0 when total=0 in %s", catalog, r, path)
			continue
		}
		gomega.Expect(completed).To(gomega.Equal(total),
			"summary.%s.%s.completed must equal total in %s", catalog, r, path)
		gomega.Expect(rate).To(gomega.BeNumerically("~", 100.0, 1e-9),
			"summary.%s.%s.completion_rate must be 100 in %s", catalog, r, path)
	}
}

// asFloat coerces a JSON-decoded numeric value to float64.
func asFloat(v interface{}) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

// asInt coerces a JSON-decoded numeric value to int.
func asInt(v interface{}) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

// crossSegmentTimestamps returns three probe timestamps shared by the
// cross-segment suites: single sits in a source segment fully inside one target
// segment, while left/right sit in a source segment that straddles a target
// boundary (left and right of the crossing). The picked source day N satisfies
// N%6==2 (LCM of the 2-day source and 3-day target grids), shifted >=8 local
// days back so it clears the 5-day source TTL.
func crossSegmentTimestamps() (single, left, right time.Time) {
	nowLocal := time.Now().Local().Truncate(24 * time.Hour)
	nowDay := nowLocal.Unix() / (24 * 3600)
	crossSrcDayStart := nowDay - 8
	for crossSrcDayStart%6 != 2 {
		crossSrcDayStart--
	}
	crossSrcStart := nowLocal.AddDate(0, 0, int(crossSrcDayStart-nowDay))
	return crossSrcStart.Add(-12 * time.Hour), crossSrcStart.Add(12 * time.Hour), crossSrcStart.Add(36 * time.Hour)
}

// runLifecycleMigration runs a single hot->warm lifecycle migration, pointing
// every root path at the shared source dir and writing its report to reportDir.
func runLifecycleMigration(progressFile, reportDir string) {
	lifecycleCmd := lifecycle.NewCommand()
	args := []string{
		"--grpc-addr", SharedContext.DataAddr,
		"--stream-root-path", SharedContext.SrcDir,
		"--measure-root-path", SharedContext.SrcDir,
		"--trace-root-path", SharedContext.SrcDir,
		"--progress-file", progressFile,
		"--report-dir", reportDir,
	}
	args = append(args, SharedContext.MetadataFlags...)
	lifecycleCmd.SetArgs(args)
	gomega.Expect(lifecycleCmd.Execute()).To(gomega.Succeed())
}

// drainWriteAcks closes a client-streaming write and fails the spec if any
// server-side ack reports a non-success status, surfacing per-row rejections
// that a bare CloseSend would swallow.
func drainWriteAcks[R interface{ GetStatus() string }](recv func() (R, error), closeSend func() error) {
	ackErrs := make(chan error, 8)
	go func() {
		defer close(ackErrs)
		for {
			resp, recvErr := recv()
			if recvErr != nil {
				return
			}
			if status := resp.GetStatus(); status != "" && status != "STATUS_SUCCEED" {
				ackErrs <- fmt.Errorf("write rejected: %s", status)
			}
		}
	}()
	gomega.Expect(closeSend()).To(gomega.Succeed())
	for ackErr := range ackErrs {
		gomega.Expect(ackErr).NotTo(gomega.HaveOccurred(), "write ack must succeed")
	}
}

// assertCrossSegmentPartMetadata validates the physical target-segment layout
// after a cross-segment migration. It reads every part's metadata.json under
// destGroupRoot and asserts (retrying until consistent):
//   - exactly wantSegCount target segment directories exist;
//   - every part's [min,max] stays within its own segment's [start, start+3d);
//   - the `single` and `left` probe timestamps land in ONE segment (segA) while
//     `right` lands in a DIFFERENT segment (segB);
//   - segA and segB are contiguous on the 3-day grid (segB.start == segA.end),
//     pinning their start/end values; and the boundary segA.end falls strictly
//     inside (left, right] — i.e. it separates the left and right probes.
func assertCrossSegmentPartMetadata(destGroupRoot string, single, left, right time.Time, wantSegCount int) {
	const warmSegDays = 3
	within := func(t time.Time, minTS, maxTS int64) bool {
		n := t.UnixNano()
		return n >= minTS && n <= maxTS
	}
	type segBounds struct {
		start, end time.Time
	}
	gomega.Eventually(func() error {
		segs := map[string]segBounds{}
		var segSingle, segLeft, segRight string
		segDirs, e := os.ReadDir(destGroupRoot)
		if e != nil {
			return e
		}
		for _, sd := range segDirs {
			if !sd.IsDir() || !strings.HasPrefix(sd.Name(), "seg-") {
				continue
			}
			segStart, perr := time.ParseInLocation("20060102", strings.TrimPrefix(sd.Name(), "seg-"), time.Local)
			if perr != nil {
				return fmt.Errorf("parse seg date %s: %w", sd.Name(), perr)
			}
			segEnd := segStart.AddDate(0, 0, warmSegDays)
			segs[sd.Name()] = segBounds{start: segStart, end: segEnd}
			shardDirs, _ := os.ReadDir(filepath.Join(destGroupRoot, sd.Name()))
			for _, shd := range shardDirs {
				if !shd.IsDir() || !strings.HasPrefix(shd.Name(), "shard-") {
					continue
				}
				shardPath := filepath.Join(destGroupRoot, sd.Name(), shd.Name())
				partDirs, _ := os.ReadDir(shardPath)
				for _, pd := range partDirs {
					if !pd.IsDir() {
						continue
					}
					raw, rerr := os.ReadFile(filepath.Join(shardPath, pd.Name(), "metadata.json"))
					if rerr != nil {
						continue
					}
					var pm struct {
						MinTimestamp int64 `json:"minTimestamp"`
						MaxTimestamp int64 `json:"maxTimestamp"`
					}
					if jerr := json.Unmarshal(raw, &pm); jerr != nil {
						return fmt.Errorf("parse %s/%s metadata: %w", sd.Name(), pd.Name(), jerr)
					}
					if pm.MinTimestamp < segStart.UnixNano() || pm.MaxTimestamp >= segEnd.UnixNano() {
						return fmt.Errorf("part %s/%s [%d,%d] escapes seg [%d,%d)",
							sd.Name(), pd.Name(), pm.MinTimestamp, pm.MaxTimestamp, segStart.UnixNano(), segEnd.UnixNano())
					}
					if within(single, pm.MinTimestamp, pm.MaxTimestamp) {
						segSingle = sd.Name()
					}
					if within(left, pm.MinTimestamp, pm.MaxTimestamp) {
						segLeft = sd.Name()
					}
					if within(right, pm.MinTimestamp, pm.MaxTimestamp) {
						segRight = sd.Name()
					}
				}
			}
		}
		if segSingle == "" || segLeft == "" || segRight == "" {
			return fmt.Errorf("probe rows not all present in part metadata yet: single=%q left=%q right=%q", segSingle, segLeft, segRight)
		}
		if len(segs) != wantSegCount {
			return fmt.Errorf("want %d target segment dirs, got %d", wantSegCount, len(segs))
		}
		if segSingle != segLeft {
			return fmt.Errorf("single (%s) and left (%s) must share one target segment", segSingle, segLeft)
		}
		if segRight == segSingle {
			return fmt.Errorf("right (%s) must be in a different target segment from single/left (%s)", segRight, segSingle)
		}
		segA, segB := segs[segSingle], segs[segRight]
		if !segB.start.Equal(segA.end) {
			return fmt.Errorf("segments not contiguous on the 3-day grid: %s ends %s but %s starts %s",
				segSingle, segA.end.Format(time.RFC3339), segRight, segB.start.Format(time.RFC3339))
		}
		if !segA.end.After(left) || !segA.end.Before(right) {
			return fmt.Errorf("segment boundary %s must fall strictly inside (left=%s, right=%s)",
				segA.end.Format(time.RFC3339), left.Format(time.RFC3339), right.Format(time.RFC3339))
		}
		return nil
	}, flags.EventuallyTimeout).Should(gomega.Succeed(),
		"migrated parts must form exactly the expected contiguous target segments with correct bounds")
}

// Measure cross-segment migration regression.
//
// The sw_cross_segment group is 2d source / 3d target — coprime grids aligned to
// the local-time epoch, so the source segment [Day 6k+2, Day 6k+4) always
// straddles the target boundary at Day 6k+3 (row-replay path) while other source
// segments stay within a single target segment (chunk-sync). We write three data
// points (entity-a):
//
//	T0: t=T_SINGLE, value=100 → source seg-A (chunk-sync)  → target seg-α
//	T1: t=T_A,      value=200 → source seg-B (row-replay) → target seg-α
//	T2: t=T_B,      value=300 → source seg-B (row-replay) → target seg-β
//
// After migration the straddling source segment must split into two target
// segments (seg-α and seg-β); pre-fix code copied every row into one segment. We
// verify the physical part layout, the report's sync breakdown, and that each
// timestamp returns its correct field value across same- and cross-segment
// warm-stage queries.
var _ = ginkgo.Describe("Measure cross-segment migration", ginkgo.Ordered, func() {
	const (
		crossGroup   = "sw_cross_segment"
		crossMeasure = "cross_segment_metric"
	)

	ginkgo.It("source segments that span multiple target segments are split via row-replay", func() {
		// === 1. Pick timestamps that exercise both single- and multi-target source segments. ===
		// T0 sits in a single-target source segment; T1/T2 share a source segment
		// that straddles a target boundary (left/right of the crossing).
		singleTargetTS, crossLeftTS, crossRightTS := crossSegmentTimestamps()

		ginkgo.By(fmt.Sprintf("seeding sw_cross_segment with T0=%s T1=%s T2=%s",
			singleTargetTS.Format(time.RFC3339),
			crossLeftTS.Format(time.RFC3339),
			crossRightTS.Format(time.RFC3339)))

		// === 2. Open a Write stream to liaison and send three rows.
		//        SharedContext.Connection points at the data node (dataAddr); writes
		//        must go through the liaison so it shard-locates and forwards. ===
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		liaisonConn, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred(), "dial liaison")
		defer func() { _ = liaisonConn.Close() }()
		measureClient := measurev1.NewMeasureServiceClient(liaisonConn)
		writeStream, writeErr := measureClient.Write(ctx)
		gomega.Expect(writeErr).NotTo(gomega.HaveOccurred(), "open Write stream")

		md := &commonv1.Metadata{Group: crossGroup, Name: crossMeasure}
		sendRow := func(ts time.Time, entityID string, value int64, first bool) {
			req := &measurev1.WriteRequest{
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(ts),
					Version:   ts.UnixNano(),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityID}}},
						}},
					},
					Fields: []*modelv1.FieldValue{
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: value}}},
					},
				},
				MessageId: uint64(time.Now().UnixNano()),
			}
			if first {
				req.Metadata = md
			}
			gomega.Expect(writeStream.Send(req)).To(gomega.Succeed())
		}
		sendRow(singleTargetTS, "entity-a", 100, true)
		sendRow(crossLeftTS, "entity-a", 200, false)
		sendRow(crossRightTS, "entity-a", 300, false)
		// Collect server-side acks before CloseSend so any per-row rejection surfaces.
		drainWriteAcks(writeStream.Recv, writeStream.CloseSend)
		time.Sleep(flags.ConsistentlyTimeout)
		// Diagnostic: dump the source group root so a regression makes it obvious
		// whether the data wrote at all or whether the migration is what's missing.
		// Layout on disk: measure/data/<group>/seg-YYYYMMDD/shard-N/...
		srcGroupRoot := filepath.Join(SharedContext.SrcDir, "measure", "data", crossGroup)
		srcEntries, srcErr := os.ReadDir(srcGroupRoot)
		ginkgo.By(fmt.Sprintf("source group root %s exists=%v entries=%d err=%v",
			srcGroupRoot, srcErr == nil, len(srcEntries), srcErr))
		if srcErr == nil {
			for _, e := range srcEntries {
				ginkgo.By(fmt.Sprintf("  src entry: %s isDir=%v", e.Name(), e.IsDir()))
			}
		}

		// === 3. Run lifecycle migration. The shared destination already holds older
		//        migrated data from previous Describe blocks; we re-run migration so
		//        sw_cross_segment's freshly-written parts flow hot → warm. ===
		ginkgo.By("running lifecycle migration")
		dir, err := os.MkdirTemp("", "lifecycle-cross-segment")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(dir)
		rf := filepath.Join(dir, "report")
		runLifecycleMigration(filepath.Join(dir, "progress.json"), rf)

		// === 4. Verify the destination directory contains multiple seg-* folders for
		//        sw_cross_segment. Pre-fix, the cross-segment part would copy entirely
		//        into one target segment, so only one seg-* folder would exist. ===
		ginkgo.By("verifying multiple target segments were created")
		// Layout on disk: measure/data/<group>/seg-YYYYMMDD/shard-N/...
		destGroupRoot := filepath.Join(SharedContext.DestDir, "measure", "data", crossGroup)
		destEntries, destErr := os.ReadDir(destGroupRoot)
		ginkgo.By(fmt.Sprintf("dest group root %s exists=%v entries=%d err=%v",
			destGroupRoot, destErr == nil, len(destEntries), destErr))
		if destErr == nil {
			for _, e := range destEntries {
				ginkgo.By(fmt.Sprintf("  dest entry: %s isDir=%v", e.Name(), e.IsDir()))
			}
		}
		// Physical-layer validation: exactly two contiguous target segments — T0 &
		// T1 in seg-α, T2 in seg-β — with every part inside its own segment and the
		// boundary falling between T1 and T2 (the split the pre-fix bug missed).
		assertCrossSegmentPartMetadata(destGroupRoot, singleTargetTS, crossLeftTS, crossRightTS, 2)

		// Locked sync_breakdown for this group: T0 (single-target segment) → 1
		// chunk-sync part; T1+T2 (straddling segment, same entity_id → same
		// source shard/part) → 1 row-replay part carrying exactly 2 rows.
		verifyCrossSegmentReport(rf, "measure_migration", crossGroup, "chunk_sync_parts", 1, 1, 2)

		// === 5. Query the warm stage and validate both placement AND values:
		//        same-segment single/multi-row queries, the cross-segment
		//        aggregate, and that each timestamp carries its correct field
		//        value (a count-only check would miss a row-replay that swaps or
		//        corrupts a field/timestamp). ===
		ginkgo.By("querying warm stage for each timestamp")
		queryClient := measurev1.NewMeasureServiceClient(liaisonConn)
		runQuery := func(start, end time.Time, wantCount int) []*measurev1.DataPoint {
			req := &measurev1.QueryRequest{
				Groups: []string{crossGroup},
				Name:   crossMeasure,
				TimeRange: &modelv1.TimeRange{
					Begin: timestamppb.New(start),
					End:   timestamppb.New(end),
				},
				TagProjection: &modelv1.TagProjection{
					TagFamilies: []*modelv1.TagProjection_TagFamily{
						{Name: "default", Tags: []string{"entity_id"}},
					},
				},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{
					Names: []string{"value"},
				},
				Stages: []string{"warm"},
			}
			var resp *measurev1.QueryResponse
			gomega.Eventually(func() error {
				var qErr error
				resp, qErr = queryClient.Query(ctx, req)
				if qErr != nil {
					return qErr
				}
				if len(resp.DataPoints) != wantCount {
					return fmt.Errorf("want %d data points, got %d", wantCount, len(resp.DataPoints))
				}
				return nil
			}, flags.EventuallyTimeout).Should(gomega.Succeed())
			return resp.DataPoints
		}
		// expectValues asserts the returned points are exactly the wanted
		// timestamp(ms) -> field-value set.
		expectValues := func(dps []*measurev1.DataPoint, want map[int64]int64, desc string) {
			got := make(map[int64]int64, len(dps))
			for _, dp := range dps {
				gomega.Expect(dp.GetFields()).To(gomega.HaveLen(1), desc+": each point has one field")
				got[dp.GetTimestamp().AsTime().UnixMilli()] = dp.GetFields()[0].GetValue().GetInt().GetValue()
			}
			gomega.Expect(got).To(gomega.Equal(want), desc)
		}
		msVal := func(t time.Time) int64 { return t.UnixMilli() }

		// Q1: same-segment single row — T0 via chunk-sync, value 100.
		expectValues(runQuery(singleTargetTS, singleTargetTS.Add(time.Millisecond), 1),
			map[int64]int64{msVal(singleTargetTS): 100}, "Q1 single-target row")
		// Q2: same-segment single row — T1 via row-replay (left of boundary), value 200.
		expectValues(runQuery(crossLeftTS, crossLeftTS.Add(time.Millisecond), 1),
			map[int64]int64{msVal(crossLeftTS): 200}, "Q2 row-replay left side")
		// Q3: same-segment single row — T2 via row-replay (right of boundary, the
		//     segment the pre-fix bug failed to create), value 300.
		expectValues(runQuery(crossRightTS, crossRightTS.Add(time.Millisecond), 1),
			map[int64]int64{msVal(crossRightTS): 300}, "Q3 row-replay right side")
		// Q5: same-segment MULTI-row — [T0,T1] both land in one target segment
		//     (T0 chunk-sync + T1 row-replay); the query must return both.
		expectValues(runQuery(singleTargetTS, crossLeftTS.Add(time.Millisecond), 2),
			map[int64]int64{msVal(singleTargetTS): 100, msVal(crossLeftTS): 200},
			"Q5 same-segment two rows (chunk-sync + row-replay co-located)")
		// Q4: cross-segment aggregate — [T0,T2] spans both target segments; must
		//     return all 3 rows with correct values.
		expectValues(runQuery(singleTargetTS, crossRightTS.Add(time.Millisecond), 3),
			map[int64]int64{msVal(singleTargetTS): 100, msVal(crossLeftTS): 200, msVal(crossRightTS): 300},
			"Q4 cross-segment aggregate")
	})
})

// Stream cross-segment migration regression and ElementID consistency.
//
// Combines chunk-sync vs row-replay distribution and raw_element_id
// path consistency. The sw_cross_segment_stream group is 2d source / 3d target, same
// coprime layout as the measure variant. We write three elements:
//
//	E0: t=T_SINGLE, eid="shared-id"   → source seg-A (chunk-sync)  → target seg-α
//	E1: t=T_A,      eid="shared-id"   → source seg-B (row-replay) → target seg-α
//	E2: t=T_B,      eid="other-id"    → source seg-B (row-replay) → target seg-β
//
// After migration the warm-stage query layer dedups by storage eID
// (query_by_ts.go:255). Without the raw_element_id fix, E0 (via chunk-sync) and E1
// (via row-replay) would land at the target with different eIDs and the query would
// return 2 rows for "shared-id" instead of 1. The fix forces both paths to keep
// the source-side uint64 eID so dedup converges.
var _ = ginkgo.Describe("Stream cross-segment migration", ginkgo.Ordered, func() {
	const (
		streamGroup    = "sw_cross_segment_stream"
		streamResource = "cross_segment_log"
	)

	ginkgo.It("stream cross-segment + raw_element_id consistency", func() {
		singleTargetTS, crossLeftTS, crossRightTS := crossSegmentTimestamps()
		ginkgo.By(fmt.Sprintf("seeding stream with T_SINGLE=%s T_A=%s T_B=%s",
			singleTargetTS.Format(time.RFC3339),
			crossLeftTS.Format(time.RFC3339),
			crossRightTS.Format(time.RFC3339)))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		liaisonConn, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer func() { _ = liaisonConn.Close() }()
		streamClient := streamv1.NewStreamServiceClient(liaisonConn)
		writeStream, writeErr := streamClient.Write(ctx)
		gomega.Expect(writeErr).NotTo(gomega.HaveOccurred())

		md := &commonv1.Metadata{Group: streamGroup, Name: streamResource}
		sendElement := func(ts time.Time, elementID, businessID, entityID string, first bool) {
			req := &streamv1.WriteRequest{
				Element: &streamv1.ElementValue{
					ElementId: elementID,
					Timestamp: timestamppb.New(ts),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: businessID}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityID}}},
						}},
					},
				},
				MessageId: uint64(time.Now().UnixNano()),
			}
			if first {
				req.Metadata = md
			}
			gomega.Expect(writeStream.Send(req)).To(gomega.Succeed())
		}
		sendElement(singleTargetTS, "shared-id", "biz-shared", "ent-1", true)
		sendElement(crossLeftTS, "shared-id", "biz-shared", "ent-2", false)
		sendElement(crossRightTS, "other-id", "biz-other", "ent-1", false)
		drainWriteAcks(writeStream.Recv, writeStream.CloseSend)
		time.Sleep(flags.ConsistentlyTimeout)

		ginkgo.By("running stream lifecycle migration")
		tmpDir, err := os.MkdirTemp("", "lifecycle-stream-cross")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(tmpDir)
		runLifecycleMigration(filepath.Join(tmpDir, "progress.json"), filepath.Join(tmpDir, "report"))

		ginkgo.By("verifying physical target segment part metadata + cross-path eID dedup")
		destGroupRoot := filepath.Join(SharedContext.DestDir, "stream", "data", streamGroup)
		// Physical-layer validation: exactly two contiguous target segments — E0+E1
		// (T_SINGLE & T_A) in seg-α, E2 (T_B) in seg-β — with every part inside its
		// own segment and the boundary falling between T_A and T_B.
		assertCrossSegmentPartMetadata(destGroupRoot, singleTargetTS, crossLeftTS, crossRightTS, 2)

		// Locked sync_breakdown: E0 → 1 chunk-sync part; E1+E2 (straddling
		// segment, same entity_id → same source shard/part) → 1 row-replay part
		// carrying exactly 2 rows.
		verifyCrossSegmentReport(filepath.Join(tmpDir, "report"), "stream_migration", streamGroup, "chunk_sync_parts", 1, 1, 2)

		queryClient := streamv1.NewStreamServiceClient(liaisonConn)
		// runStreamQuery issues a warm-stage query ordered by timestamp; an empty
		// index-rule name plus the given sort means "sort by event time"
		// (SORT_UNSPECIFIED is the server default, ascending). It waits until exactly
		// wantCount elements are returned.
		runStreamQuery := func(start, end time.Time, sort modelv1.Sort, wantCount int) []*streamv1.Element {
			req := &streamv1.QueryRequest{
				Groups: []string{streamGroup},
				Name:   streamResource,
				TimeRange: &modelv1.TimeRange{
					Begin: timestamppb.New(start),
					End:   timestamppb.New(end),
				},
				Projection: &modelv1.TagProjection{
					TagFamilies: []*modelv1.TagProjection_TagFamily{
						{Name: "searchable", Tags: []string{"business_id", "entity_id"}},
					},
				},
				OrderBy: &modelv1.QueryOrder{Sort: sort},
				Stages:  []string{"warm"},
			}
			var resp *streamv1.QueryResponse
			gomega.Eventually(func() error {
				var qErr error
				resp, qErr = queryClient.Query(ctx, req)
				if qErr != nil {
					return qErr
				}
				if len(resp.Elements) != wantCount {
					return fmt.Errorf("want %d elements, got %d", wantCount, len(resp.Elements))
				}
				return nil
			}, flags.EventuallyTimeout).Should(gomega.Succeed())
			return resp.Elements
		}
		// Stream stores element_id as a derived (hashed) identifier, so we assert
		// the tag values plus element_id distinctness/consistency rather than the
		// raw client-supplied string.
		tagsOf := func(el *streamv1.Element) [2]string {
			var biz, ent string
			for _, tf := range el.GetTagFamilies() {
				for _, tg := range tf.GetTags() {
					switch tg.GetKey() {
					case "business_id":
						biz = tg.GetValue().GetStr().GetValue()
					case "entity_id":
						ent = tg.GetValue().GetStr().GetValue()
					}
				}
			}
			return [2]string{biz, ent}
		}
		// Q1 (eID consistency core): "shared-id" written via chunk-sync (T_SINGLE) +
		// row-replay (T_A) must dedup to a single element with intact tags.
		q1 := runStreamQuery(singleTargetTS, crossLeftTS.Add(time.Millisecond), modelv1.Sort_SORT_UNSPECIFIED, 1)
		gomega.Expect(tagsOf(q1[0])).To(gomega.Equal([2]string{"biz-shared", "ent-1"}),
			"Q1 shared-id dedup across chunk-sync+row-replay keeps tags intact (proves raw_element_id fix)")
		sharedID := q1[0].GetElementId()
		// Q2 (row-replay other-id): T_B alone returns 1 element with its own tags.
		q2 := runStreamQuery(crossRightTS, crossRightTS.Add(time.Millisecond), modelv1.Sort_SORT_UNSPECIFIED, 1)
		gomega.Expect(tagsOf(q2[0])).To(gomega.Equal([2]string{"biz-other", "ent-1"}),
			"Q2 other-id row-replay right side keeps tags intact")
		otherID := q2[0].GetElementId()
		gomega.Expect(sharedID).NotTo(gomega.Equal(otherID),
			"shared-id and other-id must be distinct logical elements")
		// Q3 (cross-segment aggregate): exactly the two distinct elements, tags intact.
		q3 := runStreamQuery(singleTargetTS, crossRightTS.Add(time.Millisecond), modelv1.Sort_SORT_UNSPECIFIED, 2)
		gotElements := make(map[string][2]string, len(q3))
		for _, el := range q3 {
			gotElements[el.GetElementId()] = tagsOf(el)
		}
		gomega.Expect(gotElements).To(gomega.Equal(map[string][2]string{
			sharedID: {"biz-shared", "ent-1"},
			otherID:  {"biz-other", "ent-1"},
		}), "Q3 cross-segment aggregate must return shared-id (deduped) + other-id with intact tags")

		// Q4: "shared-id" exists under two entity_ids (ent-1 @T_SINGLE, ent-2 @T_A)
		// sharing one eID, so a range covering both dedups to ONE element — stably the
		// T_SINGLE row, regardless of sort. (+1ms: query timestamps must be ms-aligned.)
		q4 := runStreamQuery(singleTargetTS, crossLeftTS.Add(time.Millisecond), modelv1.Sort_SORT_ASC, 1)
		gomega.Expect(tagsOf(q4[0])[1]).To(gomega.Equal("ent-1"),
			"Q4 cross-entity same-eID dedup keeps the T_SINGLE (chunk-sync) row")
		gomega.Expect(q4[0].GetTimestamp().AsTime().UnixMilli()).To(gomega.Equal(singleTargetTS.UnixMilli()),
			"Q4 survivor carries the T_SINGLE timestamp")
	})
})

// Trace cross-segment migration regression.
//
// The sw_cross_segment_trace group is 2d source / 3d target, same coprime layout
// as the measure variant. Trace routing is by traceID hash (not entity hash), so
// the cross-segment guard depends only on the source-segment time range
// straddling target boundaries. trace-1 carries two spans (a chunk-sync copy and
// a row-replay span) in seg-α; trace-2 carries one row-replay span in seg-β:
//
//	span-1: t=T_SINGLE, trace-1 → source seg-A (chunk-sync)  → target seg-α
//	span-2: t=T_A,      trace-1 → source seg-B (row-replay) → target seg-α
//	span-3: t=T_B,      trace-2 → source seg-B (row-replay) → target seg-β
//
// After migration we validate physically that the straddling source segment is
// split into two target segments (span-1 & span-2 in seg-α, span-3 in seg-β) and,
// via warm-stage queries, that trace-1 surfaces BOTH its chunk-sync and row-replay
// spans and a cross-segment range returns both distinct traces.
var _ = ginkgo.Describe("Trace cross-segment migration", ginkgo.Ordered, func() {
	const (
		traceGroup    = "sw_cross_segment_trace"
		traceResource = "cross_segment_trace_item"
	)

	ginkgo.It("trace cross-segment row-replay routes spans by per-row timestamp", func() {
		singleTargetTS, crossLeftTS, crossRightTS := crossSegmentTimestamps()
		ginkgo.By(fmt.Sprintf("seeding trace with T_SINGLE=%s T_A=%s T_B=%s",
			singleTargetTS.Format(time.RFC3339),
			crossLeftTS.Format(time.RFC3339),
			crossRightTS.Format(time.RFC3339)))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		liaisonConn, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer func() { _ = liaisonConn.Close() }()
		traceClient := tracev1.NewTraceServiceClient(liaisonConn)
		writeStream, writeErr := traceClient.Write(ctx)
		gomega.Expect(writeErr).NotTo(gomega.HaveOccurred())

		md := &commonv1.Metadata{Group: traceGroup, Name: traceResource}
		sendSpan := func(ts time.Time, traceID, spanID string, version uint64, first bool) {
			tags := []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: spanID}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}},
			}
			req := &tracev1.WriteRequest{
				Tags:    tags,
				Span:    []byte("span-" + spanID),
				Version: version,
			}
			if first {
				req.Metadata = md
			}
			gomega.Expect(writeStream.Send(req)).To(gomega.Succeed())
		}
		sendSpan(singleTargetTS, "trace-1", "span-1", 1, true)
		sendSpan(crossLeftTS, "trace-1", "span-2", 2, false)
		sendSpan(crossRightTS, "trace-2", "span-3", 3, false)
		drainWriteAcks(writeStream.Recv, writeStream.CloseSend)
		time.Sleep(flags.ConsistentlyTimeout)

		ginkgo.By("running trace lifecycle migration")
		tmpDir, err := os.MkdirTemp("", "lifecycle-trace-cross")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(tmpDir)
		runLifecycleMigration(filepath.Join(tmpDir, "progress.json"), filepath.Join(tmpDir, "report"))

		ginkgo.By("verifying physical trace target segment part metadata")
		// Trace row-replay publishes per-row InternalWriteRequests; the receiver's
		// writeCallback must place each row in the target segment matching its
		// timestamp. We validate physically (every part's metadata.json range stays
		// inside its own segment, span-1 & span-2 share one target segment, span-3
		// lands in a different one) and then via warm-stage queries below.
		destGroupRoot := filepath.Join(SharedContext.DestDir, "trace", "data", traceGroup)
		// Exactly two contiguous target segments — span-1 & span-2 in seg-α, span-3
		// in seg-β — with every part inside its own segment and the boundary falling
		// between T_A and T_B.
		assertCrossSegmentPartMetadata(destGroupRoot, singleTargetTS, crossLeftTS, crossRightTS, 2)

		// Locked sync_breakdown: span-1 (single-target segment) → 1 chunk-sync
		// shard; span-2+span-3 (straddling segment, shard_num=1 → both in source
		// shard 0 → 1 part) → 1 row-replay part carrying exactly 2 rows.
		verifyCrossSegmentReport(filepath.Join(tmpDir, "report"), "trace_migration", traceGroup, "chunk_sync_shards", 1, 1, 2)

		// Query the warm stage and validate both placement AND span identity:
		// per-timestamp single-span queries, the same-segment multi-span query,
		// and the cross-segment aggregate. The OrderBy timestamp index rule lets a
		// pure time-range query return every span in range (no trace_id filter).
		ginkgo.By("querying warm stage for trace spans")
		queryClient := tracev1.NewTraceServiceClient(liaisonConn)
		runTraceQuery := func(start, end time.Time, wantCount int) []*tracev1.Trace {
			req := &tracev1.QueryRequest{
				Groups: []string{traceGroup},
				Name:   traceResource,
				TimeRange: &modelv1.TimeRange{
					Begin: timestamppb.New(start),
					End:   timestamppb.New(end),
				},
				TagProjection: []string{"trace_id", "span_id"},
				OrderBy: &modelv1.QueryOrder{
					IndexRuleName: "cross_segment_timestamp",
					Sort:          modelv1.Sort_SORT_ASC,
				},
				Stages: []string{"warm"},
			}
			var resp *tracev1.QueryResponse
			gomega.Eventually(func() error {
				var qErr error
				resp, qErr = queryClient.Query(ctx, req)
				if qErr != nil {
					return qErr
				}
				if len(resp.Traces) != wantCount {
					return fmt.Errorf("want %d traces, got %d", wantCount, len(resp.Traces))
				}
				return nil
			}, flags.EventuallyTimeout).Should(gomega.Succeed())
			return resp.Traces
		}
		// expectSpans asserts the returned trace_id -> sorted span_id set, catching a
		// row-replay that drops a span, corrupts an id, or misroutes a segment.
		expectSpans := func(traces []*tracev1.Trace, want map[string][]string, desc string) {
			got := make(map[string][]string, len(traces))
			for _, tr := range traces {
				ids := make([]string, 0, len(tr.GetSpans()))
				for _, sp := range tr.GetSpans() {
					ids = append(ids, sp.GetSpanId())
				}
				sort.Strings(ids)
				got[tr.GetTraceId()] = ids
			}
			gomega.Expect(got).To(gomega.Equal(want), desc)
		}
		// trace-1 carries span-1 (copy/chunk-sync @T_SINGLE) and span-2 (row-replay
		// @T_A), both in seg-α; trace-2 carries span-3 (row-replay @T_B) in seg-β. A
		// trace query returns the full matching trace within the scanned segment(s),
		// so any window overlapping trace-1 returns BOTH its spans.
		// Q1: copy span's window -> trace-1 with both spans (copy + replay).
		expectSpans(runTraceQuery(singleTargetTS, singleTargetTS.Add(time.Millisecond), 1),
			map[string][]string{"trace-1": {"span-1", "span-2"}},
			"Q1 copy-span window returns trace-1's copy + replay spans")
		// Q2: replay span's window -> the SAME full trace-1, proving the copy span is
		// reachable from the replay span's window too.
		expectSpans(runTraceQuery(crossLeftTS, crossLeftTS.Add(time.Millisecond), 1),
			map[string][]string{"trace-1": {"span-1", "span-2"}},
			"Q2 replay-span window also returns both of trace-1's spans")
		// Q3: window covering both of trace-1's span timestamps -> still both spans.
		expectSpans(runTraceQuery(singleTargetTS, crossLeftTS.Add(time.Millisecond), 1),
			map[string][]string{"trace-1": {"span-1", "span-2"}},
			"Q3 [T_SINGLE,T_A] returns trace-1 with both spans")
		// Q4: seg-β window -> isolated trace-2 with its row-replay span.
		expectSpans(runTraceQuery(crossRightTS, crossRightTS.Add(time.Millisecond), 1),
			map[string][]string{"trace-2": {"span-3"}}, "Q4 isolated trace-2 (row-replay)")
		// Q5: cross-segment range -> both distinct traces (trace-1 with 2 spans,
		// trace-2 with 1).
		expectSpans(runTraceQuery(singleTargetTS, crossRightTS.Add(time.Millisecond), 2),
			map[string][]string{"trace-1": {"span-1", "span-2"}, "trace-2": {"span-3"}},
			"Q5 cross-segment aggregate (2 distinct traces)")
	})
})
