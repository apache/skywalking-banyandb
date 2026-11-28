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
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
)

const (
	measureSeries = 2000
	streamSeries  = 300
	indexSeries   = 500
	// importedMeasureSeries and importedStreamSeries are at least half of the normal series, so counting them exceeds the tolerance.
	importedMeasureSeries = 1500
	importedStreamSeries  = 200

	// firstExport bounds how long the first full window takes: the window the trackers are created in is not exported.
	firstExport = 6 * activeSeriesWindow
	// observe covers more than two windows.
	observe = 3 * activeSeriesWindow
)

var _ = g.Describe("Active series of a standalone node", g.Ordered, g.Serial, func() {
	var writer *seriesWriter

	g.BeforeAll(func() {
		writer = startWriter(node.conn, "n", measureSeries, streamSeries, 0)
		g.DeferCleanup(func() { writer.Stop() })
	})

	g.It("exports the measure series count per shard", func() {
		gomega.Eventually(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
		gomega.Consistently(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
		}, observe, time.Second).Should(gomega.Succeed())
		gomega.Expect(writer.Rejected()).To(gomega.BeZero(), "rows rejected")
	})

	g.It("labels the metrics with group and shard only, besides the labels every metric of the node has", func() {
		constant, err := labelNames(node.metricsURL, "banyandb_measure_total_written", measureGroup)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(constant).To(gomega.ContainElement("group"), "the reference metric is labeled by group")
		for _, metric := range []string{measureActiveSeries, streamActiveSeries} {
			group := measureGroup
			if metric == streamActiveSeries {
				group = streamGroup
			}
			names, nameErr := labelNames(node.metricsURL, metric, group)
			gomega.Expect(nameErr).NotTo(gomega.HaveOccurred())
			gomega.Expect(names).To(gomega.ContainElements("group", "shard"), metric)
			for _, name := range names {
				gomega.Expect(name == "shard" || slices.Contains(constant, name)).To(gomega.BeTrue(),
					"%s has the label %s, which is neither shard nor a label of the reference metric %v", metric, name, constant)
			}
		}
	})

	g.It("exports the stream series count per shard", func() {
		gomega.Eventually(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, streamActiveSeries, streamGroup, streamSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
	})

	g.It("counts an index-mode measure and keeps the other measures unchanged", func() {
		writer.SetIndexSeries(indexSeries)
		gomega.Eventually(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, indexModeGroup, indexSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
		gomega.Consistently(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
		}, activeSeriesWindow, time.Second).Should(gomega.Succeed())
	})

	g.It("does not change with the number of writes per series", func() {
		// acceptedRate observes the values while it counts the rows the node acknowledged, in rows per second.
		acceptedRate := func() float64 {
			rejectedBefore, acceptedBefore, begin := writer.Rejected(), writer.Accepted(), time.Now()
			gomega.Consistently(func(gm gomega.Gomega) {
				expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
				expectSum(gm, node.metricsURL, streamActiveSeries, streamGroup, streamSeries)
			}, observe, time.Second).Should(gomega.Succeed())
			gomega.Expect(writer.Rejected()).To(gomega.Equal(rejectedBefore), "rows rejected while observing")
			return float64(writer.Accepted()-acceptedBefore) / time.Since(begin).Seconds()
		}
		single := acceptedRate()
		gomega.Expect(single).To(gomega.BeNumerically(">", 0), "no row was accepted with one write per series")

		g.DeferCleanup(func() { writer.SetRepeat(1) })
		writer.SetRepeat(2)
		doubled := acceptedRate()
		gomega.Expect(doubled/single).To(gomega.BeNumerically("~", 2, 0.5), "accepted rows per second: %v with one write per series, %v with two", single, doubled)
	})

	// The rows of an import are written to the node while the normal writer keeps its series in every window: counting them would
	// lift the values above the series of the normal writer.
	g.It("does not count the series of an import", func() {
		source := startStandalone()
		g.DeferCleanup(source.stop)
		srcWriter := startWriter(source.conn, "imp", importedMeasureSeries, importedStreamSeries, 0)
		gomega.Eventually(func(gm gomega.Gomega) {
			frames, err := transfertest.Plan(source.conn, &transferv1.PlanRequest{})
			gm.Expect(err).NotTo(gomega.HaveOccurred())
			gm.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_MEASURE, measureGroup)).To(gomega.BeNumerically(">=", importedMeasureSeries))
			gm.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, streamGroup)).To(gomega.BeNumerically(">=", importedStreamSeries))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		srcWriter.Stop()

		workDir, err := os.MkdirTemp("", "active-series-import-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		g.DeferCleanup(func() { gomega.Expect(os.RemoveAll(workDir)).To(gomega.Succeed()) })
		var artifact string
		gomega.Eventually(func(gm gomega.Gomega) {
			artifact = transfertest.ExportFrom(source.addr, workDir)
			a := transfertest.ReadArtifact(artifact, false)
			gm.Expect(transfertest.RowsCount(a, "measure", measureGroup)).To(gomega.BeNumerically(">=", importedMeasureSeries))
			gm.Expect(transfertest.RowsCount(a, "stream", streamGroup)).To(gomega.BeNumerically(">=", importedStreamSeries))
		}, transfertest.RoundTripTimeout, 3*time.Second).Should(gomega.Succeed())

		// The normal writer's series fill a window before the import starts, so a counted import would show.
		gomega.Eventually(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
			expectSum(gm, node.metricsURL, streamActiveSeries, streamGroup, streamSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())

		monitor := startValueMonitor()
		g.DeferCleanup(func() { monitor.Stop() })
		// The normal writer keeps writing to the same groups, so the row counts the verify step compares would never settle.
		opts := transfertest.DefaultImportOptions(artifact)
		opts.Verify = false
		run := transfertest.RunImport(context.Background(), node.addr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)

		// The import wrote the rows: the first and the last imported series can be queried.
		gomega.Eventually(func(gm gomega.Gomega) {
			for _, i := range []int{0, importedMeasureSeries - 1} {
				gm.Expect(measureHits(node.conn, fmt.Sprintf("imp-%d", i))).To(gomega.BeNumerically(">", 0), "imported measure series %d", i)
			}
			for _, i := range []int{0, importedStreamSeries - 1} {
				gm.Expect(streamHits(node.conn, fmt.Sprintf("imp-svc-%d", i))).To(gomega.BeNumerically(">", 0), "imported stream series %d", i)
			}
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

		gomega.Consistently(func(gm gomega.Gomega) {
			expectSum(gm, node.metricsURL, measureActiveSeries, measureGroup, measureSeries)
			expectSum(gm, node.metricsURL, streamActiveSeries, streamGroup, streamSeries)
		}, observe, time.Second).Should(gomega.Succeed())
		gomega.Expect(monitor.Stop()).To(gomega.BeEmpty(), "values while the rows were imported")
	})
})

// valueMonitor scrapes the node in the background and remembers the first value outside the tolerance of the normal writer.
type valueMonitor struct {
	stop      chan struct{}
	done      chan struct{}
	violation string
	mu        sync.Mutex
	once      sync.Once
}

func startValueMonitor() *valueMonitor {
	m := &valueMonitor{stop: make(chan struct{}), done: make(chan struct{})}
	//panicdiag:allow-rawgo test helper goroutine that scrapes until the monitor is stopped
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				m.check()
			}
		}
	}()
	return m
}

func (m *valueMonitor) check() {
	for _, c := range []struct {
		metric, group string
		want          float64
	}{{measureActiveSeries, measureGroup, measureSeries}, {streamActiveSeries, streamGroup, streamSeries}} {
		values, err := seriesByShard(node.metricsURL, c.metric, c.group)
		if err != nil {
			continue
		}
		if got := sum(values); got > c.want*(1+tolerance/100) {
			m.mu.Lock()
			if m.violation == "" {
				m.violation = c.metric + " of " + c.group + " is above the series of the normal writer"
			}
			m.mu.Unlock()
		}
	}
}

// Stop ends the monitor and returns the first violation, or an empty string.
func (m *valueMonitor) Stop() string {
	m.once.Do(func() { close(m.stop) })
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.violation
}
