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

// Package snapshot_test contains integration test cases of the taking the snapshots.
package snapshot_test

import (
	"context"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// SharedContext is the shared context for the snapshot test cases.
var SharedContext helpers.SnapshotSharedContext

var _ = ginkgo.Describe("Snapshot", func() {
	lfs := fs.NewLocalFileSystem()

	ginkgo.It("should take a snapshot", func() {
		client := databasev1.NewSnapshotServiceClient(SharedContext.Connection)
		resp, err := client.Snapshot(context.Background(), &databasev1.SnapshotRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp).NotTo(gomega.BeNil())
		gomega.Expect(resp.Snapshots).To(gomega.HaveLen(3))
		for _, snp := range resp.Snapshots {
			if snp.Catalog == commonv1.Catalog_CATALOG_MEASURE {
				measureSnapshotDir := filepath.Join(SharedContext.RootDir, "measure", "snapshots")
				entries := lfs.ReadDir(measureSnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(1))
				gomega.Expect(entries[0].Name()).To(gomega.Equal(snp.Name))
			} else if snp.Catalog == commonv1.Catalog_CATALOG_STREAM {
				streamSnapshotDir := filepath.Join(SharedContext.RootDir, "stream", "snapshots")
				entries := lfs.ReadDir(streamSnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(1))
				gomega.Expect(entries[0].Name()).To(gomega.Equal(snp.Name))
			} else if snp.Catalog == commonv1.Catalog_CATALOG_PROPERTY {
				propertySnapshotDir := filepath.Join(SharedContext.RootDir, "property", "snapshots")
				entries := lfs.ReadDir(propertySnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(1))
				gomega.Expect(entries[0].Name()).To(gomega.Equal(snp.Name))
			} else {
				ginkgo.Fail("unexpected snapshot catalog")
			}
		}
		resp, err = client.Snapshot(context.Background(), &databasev1.SnapshotRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp).NotTo(gomega.BeNil())
		gomega.Expect(resp.Snapshots).To(gomega.HaveLen(3))
		for _, snp := range resp.Snapshots {
			if snp.Catalog == commonv1.Catalog_CATALOG_MEASURE {
				measureSnapshotDir := filepath.Join(SharedContext.RootDir, "measure", "snapshots")
				entries := lfs.ReadDir(measureSnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(2))
			} else if snp.Catalog == commonv1.Catalog_CATALOG_STREAM {
				streamSnapshotDir := filepath.Join(SharedContext.RootDir, "stream", "snapshots")
				entries := lfs.ReadDir(streamSnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(2))
			} else if snp.Catalog == commonv1.Catalog_CATALOG_PROPERTY {
				propertySnapshotDir := filepath.Join(SharedContext.RootDir, "property", "snapshots")
				entries := lfs.ReadDir(propertySnapshotDir)
				gomega.Expect(entries).To(gomega.HaveLen(2))
			} else {
				ginkgo.Fail("unexpected snapshot catalog")
			}
		}
	})
})
