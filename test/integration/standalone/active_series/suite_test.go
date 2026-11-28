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

// Package active_series_test verifies the active_series metrics of a standalone node.
package active_series_test

import (
	"fmt"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

// activeSeriesWindow is the tracker window of every node of this suite. All nodes run in this process, so the hook applies to all of them.
const activeSeriesWindow = 10 * time.Second

func TestStandaloneActiveSeries(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Standalone Active Series Suite", g.Label(integration_standalone.Labels...))
}

// instance is one standalone node of the suite.
type instance struct {
	conn       *grpc.ClientConn
	stop       func()
	addr       string
	metricsURL string
}

var (
	// node is the node under test.
	node         *instance
	cleanupFuncs []func()
	goods        []gleak.Goroutine
)

// flushFlags make written rows reach parts quickly, so an export sees them.
var flushFlags = []string{"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms"}

// startStandalone starts a standalone node preloaded with the test schema that serves /metrics on its own port.
func startStandalone() *instance {
	dataDir, releaseSpace, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ports, err := test.AllocateFreePorts(6)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[5])
	flagsOf := append([]string{"--observability-listener-addr=" + metricsAddr, "--observability-metrics-interval=1s"}, flushFlags...)
	addr, _, closeFn := setup.ClosableStandalone(nil, dataDir, ports[:5], flagsOf...)
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	n := &instance{addr: addr, conn: conn, metricsURL: "http://" + metricsAddr + "/metrics"}
	n.stop = func() {
		_ = conn.Close()
		closeFn()
		releaseSpace()
	}
	gomega.Eventually(func() error {
		_, scrapeErr := scrape(n.metricsURL)
		return scrapeErr
	}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed(), "scrape endpoint %s", n.metricsURL)
	return n
}

var _ = g.BeforeSuite(func() {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	// The window applies to the trackers created afterwards, so it is set before any node starts.
	cleanupFuncs = append(cleanupFuncs, measure.SetActiveSeriesWindowForTest(activeSeriesWindow))
	node = startStandalone()
})

var _ = g.AfterSuite(func() {
	if node != nil {
		node.stop()
	}
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
