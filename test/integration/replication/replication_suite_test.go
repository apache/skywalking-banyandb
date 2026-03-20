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

package replication_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_replicated_measure "github.com/apache/skywalking-banyandb/pkg/test/replicated/measure"
	test_replicated_stream "github.com/apache/skywalking-banyandb/pkg/test/replicated/stream"
	test_replicated_trace "github.com/apache/skywalking-banyandb/pkg/test/replicated/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
)

func TestReplication(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replication Suite")
}

var (
	deferFunc       func()
	goods           []gleak.Goroutine
	now             time.Time
	connection      *grpc.ClientConn
	liaisonAddr     string
	dataNodeClosers []func()
	clusterConfig   *setup.ClusterConfig
	tmpDirCleanup   func()
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()

	By("Creating discovery file writer for DNS-based node discovery")
	tmpDir, tmpDirCleanup, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	clusterConfig = setup.PropertyClusterConfig(dfWriter)

	// Load schemas via property-based registry
	setup.PreloadSchemaViaProperty(clusterConfig,
		test_replicated_measure.PreloadSchema,
		test_replicated_stream.PreloadSchema,
		test_replicated_trace.PreloadSchema,
	)

	By("Starting 3 data nodes for replication test")
	dataNodeClosers = make([]func(), 0, 3)

	for i := 0; i < 3; i++ {
		closeDataNode := setup.DataNode(clusterConfig, "--node-labels", "role=data")
		dataNodeClosers = append(dataNodeClosers, closeDataNode)
	}

	By("Starting liaison node")
	liaisonAddr2, closerLiaisonNode := setup.LiaisonNode(clusterConfig, "--data-node-selector", "role=data")
	liaisonAddr = liaisonAddr2

	By("Waiting for liaison to discover all data nodes")
	waitConn, waitConnErr := grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(waitConnErr).NotTo(HaveOccurred())
	clusterClient := databasev1.NewClusterStateServiceClient(waitConn)
	Eventually(func(g Gomega) {
		state, stateErr := clusterClient.GetClusterState(
			context.Background(), &databasev1.GetClusterStateRequest{})
		g.Expect(stateErr).NotTo(HaveOccurred())
		tire2Table := state.GetRouteTables()["tire2"]
		g.Expect(tire2Table).NotTo(BeNil(), "tire2 route table not found")
		g.Expect(len(tire2Table.GetActive())).To(Equal(3),
			"should have 3 active data nodes in tire2 route table")
	}, flags.EventuallyTimeout).Should(Succeed())
	groupClient := databasev1.NewGroupRegistryServiceClient(waitConn)
	Eventually(func(g Gomega) {
		resp, listErr := groupClient.List(
			context.Background(), &databasev1.GroupRegistryServiceListRequest{})
		g.Expect(listErr).NotTo(HaveOccurred())
		g.Expect(resp.GetGroup()).NotTo(BeEmpty(),
			"no groups found in liaison schema registry")
	}, flags.EventuallyTimeout).Should(Succeed())
	Expect(waitConn.Close()).To(Succeed())

	By("Initializing test cases")
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))

	// Initialize test data - this loads data for all test types
	test_cases.Initialize(liaisonAddr, now)

	deferFunc = func() {
		closerLiaisonNode()
		for _, closeDataNode := range dataNodeClosers {
			closeDataNode()
		}
		tmpDirCleanup()
	}

	suiteConfig := map[string]interface{}{
		"liaison_addr": liaisonAddr,
		"now":          now.UnixNano(),
	}

	configBytes, err := json.Marshal(suiteConfig)
	Expect(err).NotTo(HaveOccurred())
	return configBytes
}, func(configBytes []byte) {
	var err error
	var config map[string]interface{}
	err = json.Unmarshal(configBytes, &config)
	Expect(err).NotTo(HaveOccurred())

	liaisonAddr = config["liaison_addr"].(string)
	now = time.Unix(0, int64(config["now"].(float64)))

	var err2 error
	connection, err2 = grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err2).NotTo(HaveOccurred())

	// Only setup context for measure tests since we're only testing measure replication
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Replication Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
