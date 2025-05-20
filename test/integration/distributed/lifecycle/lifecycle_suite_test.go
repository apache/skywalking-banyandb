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

package backup_test

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

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	caseslifecycle "github.com/apache/skywalking-banyandb/test/cases/lifecycle"
)

func TestLifecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Lifecycle Suite")
}

var (
	connection       *grpc.ClientConn
	srcDir           string
	destDir          string
	deferFunc        func()
	goods            []gleak.Goroutine
	dataAddr         string
	ep               string
	tenDaysBeforeNow time.Time
	liaisonAddr      string
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
	By("Loading schema")
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(
		schema.Namespace(metadata.DefaultNamespace),
		schema.ConfigureServerEndpoints([]string{ep}),
	)
	Expect(err).NotTo(HaveOccurred())
	defer schemaRegistry.Close()
	ctx := context.Background()
	test_stream.LoadSchemaWithStages(ctx, schemaRegistry)
	test_measure.LoadSchemaWithStages(ctx, schemaRegistry)
	By("Starting hot data node")
	var closeDataNode0 func()
	dataAddr, srcDir, closeDataNode0 = setup.DataNodeWithAddrAndDir(ep, "--node-labels", "type=hot", "--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s")
	By("Starting warm data node")
	var closeDataNode1 func()
	_, destDir, closeDataNode1 = setup.DataNodeWithAddrAndDir(ep, "--node-labels", "type=warm", "--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s")
	By("Starting liaison node")
	var closerLiaisonNode func()
	liaisonAddr, closerLiaisonNode = setup.LiaisonNode(ep, "--data-node-selector", "type=hot")
	By("Initializing test cases with 10 days before")
	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	tenDaysBeforeNow = now.Add(-10 * 24 * time.Hour)
	test_cases.Initialize(liaisonAddr, tenDaysBeforeNow)
	deferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		closeDataNode1()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}
	return []byte(dataAddr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	caseslifecycle.SharedContext = helpers.LifecycleSharedContext{
		LiaisonAddr: liaisonAddr,
		DataAddr:    dataAddr,
		Connection:  connection,
		SrcDir:      srcDir,
		DestDir:     destDir,
		EtcdAddr:    ep,
		BaseTime:    tenDaysBeforeNow,
	}
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
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
