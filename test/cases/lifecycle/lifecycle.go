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
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
		gomega.Expect(report["report_version"]).To(gomega.Equal("2.0"), "report %s has unexpected report_version", path)

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
	}
	gomega.Expect(jsonReports).To(gomega.BeNumerically(">", 0), "no JSON report files found under %s", rf)
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
