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
	"sync"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

const (
	measureSeries = 2000
	streamSeries  = 200
	indexSeries   = 500
	tolerance     = 3.0 // percent

	// firstExport bounds how long the first full window takes: the window the trackers are created in is not exported.
	firstExport = 6 * activeSeriesWindow
	// observe covers more than two windows.
	observe = 3 * activeSeriesWindow
	// perShardUpper bounds what one liaison exports for one shard of the measure group: a shard holds about half of the series of one window,
	// while a window that is not reset or that mixes in an adjacent window holds about all of them.
	perShardUpper = float64(measureSeries) / 2 * 1.2
	// shardCount is the number of shards of every group.
	shardCount = 2
)

// expectDataNodesWithoutActiveSeries asserts that no data node exports an active_series family while it exports other banyandb_ metrics.
func expectDataNodesWithoutActiveSeries(gm gomega.Gomega) {
	gm.Expect(dataNodes).NotTo(gomega.BeEmpty())
	for _, n := range dataNodes {
		names, banyandb, err := activeSeriesFamilies(n.MetricsURL)
		gm.Expect(err).NotTo(gomega.HaveOccurred())
		gm.Expect(banyandb).To(gomega.BeNumerically(">", 0), "data node %s exposes no banyandb_ metrics", n.GRPCAddr)
		gm.Expect(names).To(gomega.BeEmpty(), "data node %s", n.GRPCAddr)
	}
}

func liaisonAddrs() []string {
	addrs := make([]string, 0, len(liaisons))
	for _, n := range liaisons {
		addrs = append(addrs, n.GRPCAddr)
	}
	return addrs
}

// expectAggregate asserts every shard of group has exactly one exporting liaison and the shards add up to want within the tolerance.
func expectAggregate(gm gomega.Gomega, metric, group string, want float64) {
	agg, err := aggregateOf(metric, group)
	gm.Expect(err).NotTo(gomega.HaveOccurred())
	gm.Expect(agg.writers).To(gomega.HaveLen(shardCount), "shards that export %s of %s", metric, group)
	for shard, n := range agg.writers {
		gm.Expect(n).To(gomega.Equal(1), "liaisons that export shard %s of %s: %v", shard, group, agg.byLiaison)
	}
	gm.Expect(withinPercent(agg.sum, want, tolerance)).To(gomega.BeTrue(), "sum of %s of %s is %v, want %v +-%v%%", metric, group, agg.sum, want, tolerance)
}

var _ = g.Describe("Active series of liaisons in a cluster", g.Ordered, g.Serial, func() {
	var writer *seriesWriter

	g.BeforeAll(func() {
		writer = startWriter(liaisonAddrs(), measureSeries, streamSeries, 0)
		g.DeferCleanup(func() { writer.Stop() })
	})

	g.It("exports each measure shard from one liaison and sums to the series count", func() {
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		}, observe, time.Second).Should(gomega.Succeed())

		expectDataNodesWithoutActiveSeries(gomega.Default)
	})

	g.It("labels the metrics with group, shard and selector", func() {
		for _, n := range liaisons {
			families, err := scrape(n.MetricsURL)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, metric := range []string{measureActiveSeries, streamActiveSeries} {
				for _, m := range families[metric].GetMetric() {
					group, hasGroup := labelValue(m, "group")
					_, hasShard := labelValue(m, "shard")
					gomega.Expect(hasGroup && hasShard).To(gomega.BeTrue(), "%s of %s lacks group or shard: %v", metric, n.GRPCAddr, m.GetLabel())
					gomega.Expect(group).NotTo(gomega.BeEmpty())
				}
			}
		}
	})

	g.It("exports the stream series count", func() {
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
		expectDataNodesWithoutActiveSeries(gomega.Default)
	})

	// A Top-N result measure is not counted either. It is not covered here: the preloaded Top-N source measures need a
	// dedicated population of service and instance tags, and the result series would stay below the tolerance.
	g.It("counts an index-mode measure and keeps the other measures unchanged", func() {
		writer.SetIndexSeries(indexSeries)
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, indexModeGroup, indexSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		}, activeSeriesWindow, time.Second).Should(gomega.Succeed())
		expectDataNodesWithoutActiveSeries(gomega.Default)
	})

	g.It("keeps the data nodes free of active_series and the liaisons unchanged when rows reach a data node directly", directRowsSpec)

	g.It("does not change with the number of writes per series", func() {
		observeAggregate := func() {
			gomega.Consistently(func(gm gomega.Gomega) {
				expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
				expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
			}, observe, time.Second).Should(gomega.Succeed())
		}
		// acceptedRate observes the aggregate while it counts the rows the liaisons acknowledged, in rows per second.
		acceptedRate := func() float64 {
			rejectedBefore, acceptedBefore, begin := writer.Rejected(), writer.Accepted(), time.Now()
			observeAggregate()
			gomega.Expect(writer.Rejected()).To(gomega.Equal(rejectedBefore), "rows rejected while observing")
			return float64(writer.Accepted()-acceptedBefore) / time.Since(begin).Seconds()
		}
		single := acceptedRate()
		gomega.Expect(single).To(gomega.BeNumerically(">", 0), "no row was accepted with one write per series")

		g.DeferCleanup(func() { writer.SetRepeat(1) })
		writer.SetRepeat(2)
		doubled := acceptedRate()
		gomega.Expect(doubled/single).To(gomega.BeNumerically("~", 2, 0.4), "accepted rows per second: %v with one write per series, %v with two", single, doubled)
	})

	g.It("keeps the aggregate while a data node is stopped and after it returns", func() {
		stopped := dataNodes[0]
		stopped.Stop()
		dataNodes = dataNodes[1:]
		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
		}, observe, time.Second).Should(gomega.Succeed())

		dataNodes = append(dataNodes, startDataNode(stopped.Dir))
		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
		}, observe, time.Second).Should(gomega.Succeed())
	})

	g.It("never over-reports when a liaison leaves and returns", func() {
		leaving, staying := liaisons[1], liaisons[0]
		monitor := startBoundMonitor(liaisonURLs())
		g.DeferCleanup(func() { monitor.Stop() })

		writer.SetTargets([]string{staying.GRPCAddr})
		leaving.Stop()
		// The operator drops the stopped liaison from discovery, as a real deployment does.
		clusterConfig.NodeDiscovery.FileWriter.RemoveNode(leaving.DiscoveryAddr)
		liaisons = liaisons[:1]
		monitor.SetURLs(liaisonURLs())

		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())

		joined := startLiaison()
		liaisons = append(liaisons, joined)
		monitor.SetURLs(liaisonURLs())
		writer.SetTargets(liaisonAddrs())

		joinedExports := func(gm gomega.Gomega) {
			agg, err := aggregateOf(measureActiveSeries, measureGroup)
			gm.Expect(err).NotTo(gomega.HaveOccurred())
			gm.Expect(slicesOf(agg.byLiaison[joined.MetricsURL])).To(gomega.ContainElement(gomega.BeNumerically(">", 0)),
				"the joined liaison %s exports no shard", joined.GRPCAddr)
		}
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			joinedExports(gm)
		}, firstExport, time.Second).Should(gomega.Succeed())
		gomega.Consistently(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
			joinedExports(gm)
		}, observe, time.Second).Should(gomega.Succeed())

		gomega.Expect(monitor.Stop()).To(gomega.BeEmpty())
	})
})

func slicesOf(shards map[string]float64) []float64 {
	values := make([]float64, 0, len(shards))
	for _, v := range shards {
		values = append(values, v)
	}
	return values
}

// bound checks one scrape of the liaisons against the upper limits and returns what is exceeded, or an empty string.
func bound(agg aggregate) string {
	for url, shards := range agg.byLiaison {
		for shard, v := range shards {
			if v > perShardUpper {
				return fmt.Sprintf("liaison %s exports %v for shard %s, above %v", url, v, shard, perShardUpper)
			}
		}
	}
	if upper := float64(measureSeries) * (1 + tolerance/100); agg.sum > upper {
		return fmt.Sprintf("the shards add up to %v, above %v", agg.sum, upper)
	}
	return ""
}

// boundMonitor scrapes the liaisons in the background and remembers the first limit that is exceeded.
type boundMonitor struct {
	stop      chan struct{}
	done      chan struct{}
	violation string
	urls      []string
	mu        sync.Mutex
	once      sync.Once
}

func startBoundMonitor(urls []string) *boundMonitor {
	m := &boundMonitor{urls: urls, stop: make(chan struct{}), done: make(chan struct{})}
	//panicdiag:allow-rawgo test helper goroutine
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

// SetURLs replaces the metrics URLs the monitor scrapes.
func (m *boundMonitor) SetURLs(urls []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.urls = urls
}

func (m *boundMonitor) check() {
	m.mu.Lock()
	urls := append([]string(nil), m.urls...)
	m.mu.Unlock()
	reachable := make([]string, 0, len(urls))
	for _, url := range urls {
		// A liaison that is stopping or starting does not answer; only the answers are bounded.
		if _, err := activeSeriesByShard(url, measureActiveSeries, measureGroup); err == nil {
			reachable = append(reachable, url)
		}
	}
	agg, err := aggregateAt(reachable, measureActiveSeries, measureGroup)
	if err != nil {
		return
	}
	if v := bound(agg); v != "" {
		m.mu.Lock()
		if m.violation == "" {
			m.violation = v
		}
		m.mu.Unlock()
	}
}

// Stop ends the monitor and returns the first limit that was exceeded, or an empty string.
func (m *boundMonitor) Stop() string {
	m.once.Do(func() { close(m.stop) })
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.violation
}

var _ = g.Describe("Active series of a batch whose parity is chosen next to a window boundary", g.Ordered, g.Serial, func() {
	var writer *seriesWriter

	g.BeforeAll(func() {
		writer = startWriter(liaisonAddrs(), measureSeries, streamSeries, 0)
		g.DeferCleanup(func() { writer.Stop() })
		gomega.Eventually(func(gm gomega.Gomega) {
			expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		}, firstExport, time.Second).Should(gomega.Succeed())
	})

	g.It("keeps the sum within the tolerance when the batch arrives after the boundary", func() {
		windowOf := func(t time.Time) int64 { return t.UnixNano() / int64(activeSeriesWindow) }
		next := time.Unix(0, (windowOf(time.Now())+1)*int64(activeSeriesWindow))
		if time.Until(next) < time.Second {
			next = next.Add(activeSeriesWindow)
		}
		time.Sleep(time.Until(next.Add(-600 * time.Millisecond)))
		parity := trackerClock.begin()
		ts := timestamppb.New(time.Now().Add(-time.Minute).Truncate(time.Millisecond))
		rows := make([]*measurev1.WriteRequest, 0, batchRows)
		for i := range batchRows {
			rows = append(rows, writer.measureRow(ts, parity, i))
		}
		// The batch reaches the liaison after the boundary that follows the moment its series were chosen.
		time.Sleep(time.Until(next.Add(300 * time.Millisecond)))
		writer.sendMeasure(context.Background(), liaisonAddrs()[0], rows)
		trackerClock.end()

		time.Sleep(time.Until(next.Add(activeSeriesWindow + 4*time.Second)))
		expectAggregate(gomega.Default, measureActiveSeries, measureGroup, measureSeries)
	})
})
