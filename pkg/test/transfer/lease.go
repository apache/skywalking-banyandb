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

package transfer

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/transfer/export"
)

// Catalogs lists the on-disk catalog directories of a data node that hold export snapshots.
var Catalogs = []string{"stream", "measure", "trace", "property"}

// exportSnapshotsDir mirrors storage.ExportSnapshotsDir, which this package cannot import.
const exportSnapshotsDir = "export-snapshots"

// SessionDir returns <dataDir>/<catalog>/export-snapshots/<id>, the directory a data node
// keeps one catalog's snapshot of an export session in.
func SessionDir(dataDir, catalog, id string) string {
	return filepath.Join(dataDir, catalog, exportSnapshotsDir, id)
}

// LeaseFile returns the .lease path of one catalog's copy of an export session.
func LeaseFile(dataDir, catalog, id string) string {
	return filepath.Join(SessionDir(dataDir, catalog, id), export.LeaseFileName)
}

// ExportEntries lists the entries of one catalog's export snapshot directory on a data node.
func ExportEntries(dataDir, catalog string) []string {
	entries, err := os.ReadDir(filepath.Join(dataDir, catalog, exportSnapshotsDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// ReadLease parses a data node's .lease file with the production reader.
func ReadLease(path string) export.Lease {
	lease, err := export.ReadLease(filepath.Dir(path))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return lease
}

// ReadLastHeartbeat returns the lastHeartbeatAt stamp of a .lease file.
func ReadLastHeartbeat(path string) int64 { return ReadLease(path).LastHeartbeatAt }

// RewriteLease applies mutate to a .lease document and replaces the file atomically, so a
// data node reading it concurrently never sees a torn document.
func RewriteLease(path string, mutate func(*export.Lease)) {
	lease := ReadLease(path)
	mutate(&lease)
	gomega.ExpectWithOffset(1, export.WriteLease(filepath.Dir(path), lease)).To(gomega.Succeed())
}

// ExpireLease rewrites a .lease file so that its expiresAt lies in the past.
func ExpireLease(path string) {
	RewriteLease(path, func(l *export.Lease) { l.ExpiresAt = time.Now().Add(-time.Second).UnixNano() })
}
