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
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/transfer/rowdiff"
)

const (
	// RoundTripTimeout bounds how long an import target may take to flush and commit what it
	// imported before an export of it holds every row.
	RoundTripTimeout = 3 * time.Minute
	// maxReportedDiffs bounds the differences an assertion failure prints.
	maxReportedDiffs = 20
)

// ExpectRoundTrip asserts with g that the artifact dirB, exported from an import target, holds
// the same logical rows as the source artifact dirA for the given groups (every group of dirA
// when none is given), and returns the comparison report for further assertions.
func ExpectRoundTrip(g gomega.Gomega, dirA, dirB string, groups ...string) *rowdiff.Report {
	report, err := rowdiff.Compare(dirA, dirB, rowdiff.Options{Groups: groups})
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	g.ExpectWithOffset(1, report.OK()).To(gomega.BeTrue(), "artifacts differ:\n%s", strings.Join(report.Summary(maxReportedDiffs), "\n"))
	return report
}

// NewExportDir returns a path below root that does not exist yet.
func NewExportDir(root string) string {
	dir, err := os.MkdirTemp(root, "export-")
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return filepath.Join(dir, "export")
}

// ExportFrom exports every catalog through the liaison at addr into a new directory below
// root and returns it.
func ExportFrom(addr, root string) string {
	out := NewExportDir(root)
	run := RunExport(context.Background(), addr, DefaultExportOptions(out), nil)
	gomega.ExpectWithOffset(1, run.Err).NotTo(gomega.HaveOccurred(), run.Log)
	return out
}

// ExportEventually exports through the liaison at addr into new directories below root until
// the export satisfies check, which sees the export directory, and returns that directory.
// A target flushes and commits what it imported on its own schedule (properties and
// index-mode documents are not reconciled by --verify), so the first exports may still miss
// rows.
func ExportEventually(addr, root string, check func(g gomega.Gomega, dir string)) string {
	var dir string
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		dir = NewExportDir(root)
		run := RunExport(context.Background(), addr, DefaultExportOptions(dir), nil)
		g.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		check(g, dir)
	}, RoundTripTimeout, 3*time.Second).Should(gomega.Succeed())
	return dir
}
