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

// Package clusterstate provides shared test setup for distributed cluster state integration tests.
package clusterstate

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var (
	stopFunc          func()
	dataConnection    *grpc.ClientConn
	liaisonConnection *grpc.ClientConn
	goods             []gleak.Goroutine
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	goods = gleak.Goroutines()
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	ginkgo.By("Starting data node")
	dataAddr, _, closeDataNode0 := setup.DataNodeWithAddrAndDir(config)
	ginkgo.By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config)
	stopFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		tmpDirCleanup()
	}
	time.Sleep(flags.ConsistentlyTimeout)
	var err error
	liaisonConnection, err = grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dataConnection, err = grpchelper.Conn(dataAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return nil
}, func(_ []byte) {
})

var _ = ginkgo.Describe("ClusterState API", func() {
	ginkgo.It("Check cluster state", func() {
		client := databasev1.NewClusterStateServiceClient(dataConnection)
		state, err := client.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(state.GetRouteTables()).To(gomega.HaveKey("property"))
		client = databasev1.NewClusterStateServiceClient(liaisonConnection)
		state, err = client.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(state.GetRouteTables()).To(gomega.HaveKey("tire1"))
		gomega.Expect(state.GetRouteTables()).To(gomega.HaveKey("tire2"))
	})
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if dataConnection != nil {
		gomega.Expect(dataConnection.Close()).To(gomega.Succeed())
	}
	if liaisonConnection != nil {
		gomega.Expect(liaisonConnection.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Distributed Lifecycle Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if stopFunc != nil {
			stopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	}
})
