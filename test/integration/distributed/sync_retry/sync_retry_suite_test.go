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

package integration_sync_retry_test

import (
	"encoding/json"
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
)

func TestDistributedSyncRetry(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Distributed Sync Retry Suite")
}

type suiteConfig struct {
	LiaisonAddr string   `json:"liaisonAddr"`
	DataPaths   []string `json:"dataPaths"`
}

var (
	liaisonAddr string
	dataPaths   []string

	cleanupFuncs []func()
	goods        []gleak.Goroutine
)

var _ = g.SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())

	dir, releaseSpace, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	dfWriter := setup.NewDiscoveryFileWriter(dir)
	clusterConfig := setup.PropertyClusterConfig(dfWriter)

	cleanupFuncs = append([]func(){releaseSpace}, cleanupFuncs...)

	// Start two data nodes to ensure replication targets exist
	startDataNode := func() (string, string) {
		addr, path, closeFn := setup.DataNodeWithAddrAndDir(clusterConfig)
		cleanupFuncs = append(cleanupFuncs, closeFn)
		return addr, path
	}

	_, path0 := startDataNode()
	_, path1 := startDataNode()
	paths := []string{path0, path1}

	liaisonAddrLocal, _, closeLiaison := setup.LiaisonNodeWithHTTP(clusterConfig)
	cleanupFuncs = append(cleanupFuncs, closeLiaison)

	setup.PreloadSchemaViaProperty(clusterConfig,
		test_stream.PreloadSchema,
		test_measure.PreloadSchema,
		test_trace.PreloadSchema,
	)

	cfg := suiteConfig{
		LiaisonAddr: liaisonAddrLocal,
		DataPaths:   paths,
	}
	payload, err := json.Marshal(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return payload
}, func(data []byte) {
	var cfg suiteConfig
	gomega.Expect(json.Unmarshal(data, &cfg)).To(gomega.Succeed())
	liaisonAddr = cfg.LiaisonAddr
	dataPaths = cfg.DataPaths
})

var _ = g.SynchronizedAfterSuite(func() {
	// Execute cleanups in reverse order
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
}, func() {})
