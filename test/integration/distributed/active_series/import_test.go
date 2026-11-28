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

package active_series_test

import (
	"context"
	"os"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
)

const (
	// importedMeasureSeries and importedStreamSeries are far enough above the tolerance of the normal series that counting them is visible.
	importedMeasureSeries = 1500
	importedStreamSeries  = 150
)

var _ = g.Describe("Active series of rows imported through a liaison", g.Ordered, g.Serial, func() {
	g.BeforeAll(func() {
		writer := startWriter(liaisonAddrs(), measureSeries, streamSeries, 0)
		g.DeferCleanup(func() { writer.Stop() })
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
	})

	// The import sends its rows to the data nodes directly, so neither the liaisons nor the data nodes count them.
	g.It("does not count the series of an import", func() {
		conn := liaisonConn()
		source := startPrefixedWriter("imp-", liaisonAddrs(), importedMeasureSeries, importedStreamSeries, 0)
		gomega.Eventually(func(gm gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{})
			gm.Expect(err).NotTo(gomega.HaveOccurred())
			gm.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_MEASURE, measureGroup)).To(gomega.BeNumerically(">=", importedMeasureSeries))
			gm.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, streamGroup)).To(gomega.BeNumerically(">=", importedStreamSeries))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		source.Stop()
		stoppedAt := time.Now()

		workDir, err := os.MkdirTemp("", "active-series-import-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		g.DeferCleanup(func() { gomega.Expect(os.RemoveAll(workDir)).To(gomega.Succeed()) })
		artifact := transfertest.ExportEventually(liaisons[0].GRPCAddr, workDir, func(gm gomega.Gomega, dir string) {
			a := transfertest.ReadArtifact(dir, false)
			gm.Expect(transfertest.RowsCount(a, "measure", measureGroup)).To(gomega.BeNumerically(">=", importedMeasureSeries))
			gm.Expect(transfertest.RowsCount(a, "stream", streamGroup)).To(gomega.BeNumerically(">=", importedStreamSeries))
		})

		// A liaison exports the last complete window. The window the source stopped in still holds its series and the next one is clean,
		// so the values are checked once the window after the stop has been exported.
		cleanWindowExported := stoppedAt.Truncate(activeSeriesWindow).Add(2*activeSeriesWindow + 2*time.Second)
		time.Sleep(time.Until(cleanWindowExported))
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())

		// The normal writer keeps writing to the same groups, so the row counts the verify step compares would never settle.
		opts := transfertest.DefaultImportOptions(artifact)
		opts.Verify = false
		run := transfertest.RunImport(context.Background(), liaisons[0].GRPCAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.Result.Push).NotTo(gomega.BeNil())
		var pushed uint64
		for _, rows := range run.Result.Push.Rows {
			pushed += rows
		}
		gomega.Expect(pushed).To(gomega.BeNumerically(">=", importedMeasureSeries+importedStreamSeries), "rows the import sent")

		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
			expectDataNodesWithoutActiveSeries(gm)
		}, observe, time.Second).Should(gomega.Succeed())
	})
})
