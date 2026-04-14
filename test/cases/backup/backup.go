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

// Package backup_test provides the test cases for the backup command-line tool.
package backup_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// SharedContext is the shared context for the snapshot test cases.
var SharedContext helpers.BackupSharedContext

var _ = ginkgo.Describe("Backup", func() {
	lfs := fs.NewLocalFileSystem()

	verifySnapshot := func(snpName string, entries []fs.DirEntry) int {
		for _, entry := range entries {
			if entry.Name() == snpName {
				return len(entries)
			}
		}
		ginkgo.Fail("snapshot not found")
		return 0
	}

	const schemaPropertyKey = -1
	resolveSnapshotDir := func(snp *databasev1.Snapshot) (string, commonv1.Catalog, bool) {
		if strings.HasPrefix(snp.Name, snapshot.SchemaPropertyCatalogName+"/") {
			return filepath.Join(SharedContext.RootDir, "schema-property", "snapshots"), commonv1.Catalog(schemaPropertyKey), true
		}
		switch snp.Catalog {
		case commonv1.Catalog_CATALOG_MEASURE:
			return filepath.Join(SharedContext.RootDir, "measure", "snapshots"), snp.Catalog, true
		case commonv1.Catalog_CATALOG_STREAM:
			return filepath.Join(SharedContext.RootDir, "stream", "snapshots"), snp.Catalog, true
		case commonv1.Catalog_CATALOG_PROPERTY:
			return filepath.Join(SharedContext.RootDir, "property", "snapshots"), snp.Catalog, true
		case commonv1.Catalog_CATALOG_TRACE:
			return filepath.Join(SharedContext.RootDir, "trace", "snapshots"), snp.Catalog, true
		}
		return "", 0, false
	}

	ginkgo.It("should take a snapshot", func() {
		client := databasev1.NewSnapshotServiceClient(SharedContext.Connection)
		resp, err := client.Snapshot(context.Background(), &databasev1.SnapshotRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp).NotTo(gomega.BeNil())
		gomega.Expect(resp.Snapshots).To(gomega.HaveLen(5))
		catalogNumMap := make(map[commonv1.Catalog]int)
		for _, snp := range resp.Snapshots {
			snpDir, key, ok := resolveSnapshotDir(snp)
			if !ok {
				ginkgo.Fail("unexpected snapshot catalog")
			}
			entries := lfs.ReadDir(snpDir)
			actualName := strings.TrimPrefix(snp.Name, snapshot.SchemaPropertyCatalogName+"/")
			catalogNumMap[key] = verifySnapshot(actualName, entries)
		}
		resp, err = client.Snapshot(context.Background(), &databasev1.SnapshotRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp).NotTo(gomega.BeNil())
		gomega.Expect(resp.Snapshots).To(gomega.HaveLen(5))
		for _, snp := range resp.Snapshots {
			snpDir, key, ok := resolveSnapshotDir(snp)
			if !ok {
				ginkgo.Fail("unexpected snapshot catalog")
			}
			entries := lfs.ReadDir(snpDir)
			gomega.Expect(entries).To(gomega.HaveLen(catalogNumMap[key] + 1))
		}
	})

	ginkgo.It("should backup direct test files in root paths to remote destination", func() {
		destDir, err := os.MkdirTemp("", "backup-test")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(destDir)
		destURL := "file://" + destDir

		backupCmd := backup.NewBackupCommand()
		backupCmd.SetArgs([]string{
			"--grpc-addr", SharedContext.DataAddr,
			"--stream-root-path", SharedContext.RootDir,
			"--measure-root-path", SharedContext.RootDir,
			"--property-root-path", SharedContext.RootDir,
			"--trace-root-path", SharedContext.RootDir,
			"--schema-root-path", SharedContext.RootDir,
			"--dest", destURL,
			"--time-style", "daily",
		})
		err = backupCmd.Execute()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		timeDir := time.Now().Format("2006-01-02")
		entries := lfs.ReadDir(filepath.Join(destDir, timeDir))
		gomega.Expect(entries).To(gomega.HaveLen(5))
		for _, entry := range entries {
			gomega.Expect(entry.Name()).To(gomega.BeElementOf([]string{"stream", "measure", "property", "trace", snapshot.SchemaPropertyCatalogName}))
		}
	})
})
