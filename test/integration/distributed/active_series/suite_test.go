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

// Package active_series_test verifies the active_series metrics of liaisons in an in-process cluster with two liaison and two data nodes.
package active_series_test

import (
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"gopkg.in/yaml.v3"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/file"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

// activeSeriesWindow is the tracker window of every node in this suite. All nodes run in this process, so the hook applies to all of them.
const activeSeriesWindow = 10 * time.Second

func TestDistributedActiveSeries(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Distributed Active Series Suite", g.Label(integration_distributed.Labels...))
}

// node is one started banyandb node of the cluster.
type node struct {
	Stop func()
	// DiscoveryAddr is the address the node registered in the discovery file; it is empty for a data node.
	DiscoveryAddr string
	GRPCAddr      string
	MetricsURL    string
	Dir           string
}

var (
	clusterConfig *setup.ClusterConfig
	dataNodes     []*node
	liaisons      []*node
	cleanupFuncs  []func()
	goods         []gleak.Goroutine
	// trackerClock is the clock of every active series tracker of the suite.
	trackerClock *windowClock
)

// startLiaison starts a liaison whose sub server serves /metrics on its own HTTP port.
func startLiaison() *node {
	ports, err := test.AllocateFreePorts(1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	known := discoveryAddrs()
	grpcAddr, httpAddr, stop := setup.LiaisonNodeWithHTTP(clusterConfig,
		fmt.Sprintf("--liaison-server-http-port=%d", ports[0]), "--observability-metrics-interval=1s")
	ready := false
	defer func() {
		if !ready {
			stop()
		}
	}()
	registered := discoveryAddrs()
	gomega.Expect(registered).To(gomega.HaveLen(len(known)+1), "discovery entries before %v and after %v", known, registered)
	n := &node{GRPCAddr: grpcAddr, MetricsURL: fmt.Sprintf("http://127.0.0.1:%d/metrics", ports[0]), Stop: stop}
	// The port was released before the node allocated its own, so the scrape endpoint is checked directly instead of through the node's readiness.
	for _, addr := range []string{grpcAddr, httpAddr} {
		gomega.Expect(addr).NotTo(gomega.HaveSuffix(fmt.Sprintf(":%d", ports[0])), "the scrape port is also used by the liaison at %s", grpcAddr)
	}
	gomega.Eventually(func() error {
		_, err := scrape(n.MetricsURL)
		return err
	}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed(), "scrape endpoint %s", n.MetricsURL)
	for _, addr := range registered {
		if !slices.Contains(known, addr) {
			n.DiscoveryAddr = addr
		}
	}
	ready = true
	return n
}

// discoveryAddrs returns the addresses in the discovery file of the cluster.
func discoveryAddrs() []string {
	data, err := os.ReadFile(clusterConfig.NodeDiscovery.FileWriter.Path())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var cfg file.NodeFileConfig
	gomega.Expect(yaml.Unmarshal(data, &cfg)).To(gomega.Succeed())
	addrs := make([]string, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		addrs = append(addrs, n.Address)
	}
	return addrs
}

// startDataNode starts a data node on dir. Stopping it keeps dir, so the node can start again on the same data.
func startDataNode(dir string) *node {
	addr, _, httpURL, stop := setup.DataNodeFromDataDir(clusterConfig, dir, "--observability-metrics-interval=1s",
		"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms")
	return &node{GRPCAddr: addr, MetricsURL: httpURL + "/metrics", Dir: dir, Stop: stop}
}

// The specs stop and start nodes, so every node of this suite lives in the process that runs the specs.
var _ = g.BeforeSuite(func() {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())

	restore := measure.SetActiveSeriesWindowForTest(activeSeriesWindow)
	cleanupFuncs = append(cleanupFuncs, restore)
	trackerClock = newWindowClock()
	cleanupFuncs = append(cleanupFuncs, measure.SetActiveSeriesClockForTest(trackerClock))

	dir, releaseSpace, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cleanupFuncs = append(cleanupFuncs, releaseSpace)

	dfWriter := setup.NewDiscoveryFileWriter(dir)
	clusterConfig = setup.PropertyClusterConfig(dfWriter)

	for range 2 {
		dataDir, releaseDataDir, dirErr := test.NewSpace()
		gomega.Expect(dirErr).NotTo(gomega.HaveOccurred())
		cleanupFuncs = append(cleanupFuncs, releaseDataDir)
		dataNodes = append(dataNodes, startDataNode(dataDir))
	}
	for range 2 {
		liaisons = append(liaisons, startLiaison())
	}
	setup.PreloadSchemaViaProperty(clusterConfig, test_measure.PreloadSchema, test_stream.PreloadSchema)
})

var _ = g.AfterSuite(func() {
	for _, n := range liaisons {
		n.Stop()
	}
	for _, n := range dataNodes {
		n.Stop()
	}
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
