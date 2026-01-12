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

package cluster_state_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func TestClusterState(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Get Cluster State Suite")
}

var (
	dataConnection    *grpc.ClientConn
	liaisonConnection *grpc.ClientConn
	srcDir            string
	deferFunc         func()
	goods             []gleak.Goroutine
	dataAddr          string
	liaisonAddr       string
	ep                string
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	goods = gleak.Goroutines()
	By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	var spaceDef func()
	srcDir, spaceDef, err = test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep = fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(srcDir),
		embeddedetcd.AutoCompactionMode("periodic"),
		embeddedetcd.AutoCompactionRetention("1h"),
		embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
	)
	Expect(err).ShouldNot(HaveOccurred())
	<-server.ReadyNotify()
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(
		schema.Namespace(metadata.DefaultNamespace),
		schema.ConfigureServerEndpoints([]string{ep}),
	)
	Expect(err).NotTo(HaveOccurred())
	defer schemaRegistry.Close()
	By("Starting data node")
	var closeDataNode0 func()
	dataAddr, srcDir, closeDataNode0 = setup.DataNodeWithAddrAndDir(ep, "--property-repair-enabled=true")
	By("Starting liaison node")
	var closerLiaisonNode func()
	liaisonAddr, closerLiaisonNode = setup.LiaisonNode(ep)
	time.Sleep(flags.ConsistentlyTimeout)
	deferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}
	liaisonConnection, err = grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	dataConnection, err = grpchelper.Conn(dataAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	return nil
}, func(_ []byte) {
})

var _ = Describe("ClusterState API", func() {
	It("Check cluster state", func() {
		client := databasev1.NewClusterStateServiceClient(dataConnection)
		state, err := client.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(state.GetRouteTables()).To(HaveKey("property"))
		client = databasev1.NewClusterStateServiceClient(liaisonConnection)
		state, err = client.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(state.GetRouteTables()).To(HaveKey("tire1"))
		Expect(state.GetRouteTables()).To(HaveKey("tire2"))
	})
})

var _ = SynchronizedAfterSuite(func() {
	if dataConnection != nil {
		Expect(dataConnection.Close()).To(Succeed())
	}
	if liaisonConnection != nil {
		Expect(liaisonConnection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Distributed Lifecycle Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	}
})
