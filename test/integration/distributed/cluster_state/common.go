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

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/fileformat"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/host"
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
	dataAddr, _, _, closeDataNode0 := setup.DataNodeWithAddrAndDir(config)
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

	// The import gates read the file format version and the time zone of every
	// data node out of this one call, so the fields have to survive registration
	// and show up on the liaison's tire2 table, not just on the node itself.
	ginkgo.It("Report the file format version and the time zone of each data node", func() {
		client := databasev1.NewClusterStateServiceClient(liaisonConnection)
		var dataNodes []*databasev1.Node
		gomega.Eventually(func(g gomega.Gomega) {
			state, err := client.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			dataNodes = state.GetRouteTables()["tire2"].GetRegistered()
			g.Expect(dataNodes).NotTo(gomega.BeEmpty())
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
		for _, n := range dataNodes {
			name := n.GetMetadata().GetName()
			gomega.Expect(n.GetRoles()).To(gomega.ContainElement(databasev1.Role_ROLE_DATA), "node %s in tire2 is not a data node", name)
			gomega.Expect(n.GetVersion()).NotTo(gomega.BeNil(), "node %s reports no version", name)
			gomega.Expect(n.GetVersion().GetFileFormatVersion()).To(gomega.Equal(fileformat.CurrentVersion))
			gomega.Expect(n.GetVersion().GetCompatibleFileFormatVersion()).To(gomega.ContainElement(fileformat.CurrentVersion))
			gomega.Expect(n.GetVersion().GetApiVersion()).To(gomega.Equal(apiversion.Version))
			// An unresolved zone would make both sides empty and the comparison
			// vacuous, so require a name first. CI pins the zone per matrix entry.
			gomega.Expect(n.GetTzName()).NotTo(gomega.BeEmpty(), "node %s resolved no time zone; set TZ or /etc/localtime", name)
			gomega.Expect(n.GetTzName()).To(gomega.Equal(host.TimeZoneName()))
		}
	})

	// GetCurrentNode answers for the liaison itself, and it carries the same two
	// fields because every node type goes through ToProtoNode.
	ginkgo.It("Report the file format version and the time zone on the current node", func() {
		client := databasev1.NewNodeQueryServiceClient(liaisonConnection)
		resp, err := client.GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetNode().GetVersion().GetFileFormatVersion()).To(gomega.Equal(fileformat.CurrentVersion))
		gomega.Expect(resp.GetNode().GetVersion().GetApiVersion()).To(gomega.Equal(apiversion.Version))
		gomega.Expect(resp.GetNode().GetTzName()).NotTo(gomega.BeEmpty())
		gomega.Expect(resp.GetNode().GetTzName()).To(gomega.Equal(host.TimeZoneName()))
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
