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

// Package write_backpressure_test verifies the trace and stream Write backpressure contract of a liaison in an in-process cluster.
package write_backpressure_test

import (
	"testing"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

// pressureTimeout is the liaison's --write-pressure-timeout in this suite. It is long enough for a short pressure
// window to clear and short enough to keep the timeout spec quick.
const pressureTimeout = "5s"

func TestDistributedWriteBackpressure(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Distributed Write Backpressure Suite", g.Label(integration_distributed.Labels...))
}

var (
	liaisonAddr  string
	cleanupFuncs []func()
	goods        []gleak.Goroutine
)

// The liaison's protector is forced through an in-process hook, so every node of this suite lives in the process
// that runs the specs; a plain BeforeSuite keeps it so under parallel runs too.
var _ = g.BeforeSuite(func() {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())

	dir, releaseSpace, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cleanupFuncs = append([]func(){releaseSpace}, cleanupFuncs...)

	dfWriter := setup.NewDiscoveryFileWriter(dir)
	clusterConfig := setup.PropertyClusterConfig(dfWriter)

	_, _, _, closeData := setup.DataNodeWithAddrAndDir(clusterConfig)
	cleanupFuncs = append(cleanupFuncs, closeData)

	var closeLiaison func()
	liaisonAddr, _, closeLiaison = setup.LiaisonNodeWithHTTP(clusterConfig, "--write-pressure-timeout="+pressureTimeout)
	cleanupFuncs = append(cleanupFuncs, closeLiaison)

	setup.PreloadSchemaViaProperty(clusterConfig,
		test_stream.PreloadSchema,
		test_measure.PreloadSchema,
		test_trace.PreloadSchema,
	)
})

var _ = g.AfterSuite(func() {
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
