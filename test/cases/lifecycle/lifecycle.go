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
)

// SharedContext is the shared context for the snapshot test cases.
var SharedContext helpers.LifecycleSharedContext

var _ = ginkgo.Describe("Lifecycle", func() {
	ginkgo.It("should migrate data correctly", func() {
		dir, err := os.MkdirTemp("", "lifecycle-restore-dest")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(dir)
		pf := filepath.Join(dir, "progress.json")
		lifecycleCmd := lifecycle.NewCommand()
		lifecycleCmd.SetArgs([]string{
			"--grpc-addr", SharedContext.DataAddr,
			"--stream-root-path", SharedContext.SrcDir,
			"--measure-root-path", SharedContext.SrcDir,
			"--etcd-endpoints", SharedContext.EtcdAddr,
			"--progress-file", pf,
		})
		err = lifecycleCmd.Execute()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		verifySourceDirectoriesAfterMigration()
		verifyDestinationDirectoriesAfterMigration()
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
