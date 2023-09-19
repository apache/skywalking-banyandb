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

// Package integration_setup_test is a integration test suite.
package integration_setup_test

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
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

func TestQuery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Query Suite")
}

var (
	deferFunc  func()
	goods      []gleak.Goroutine
	now        time.Time
	connection *grpc.ClientConn
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
	dir, spaceDef, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(dir))
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
	test_stream.PreloadSchema(ctx, schemaRegistry)
	test_measure.PreloadSchema(ctx, schemaRegistry)
	By("Starting data node 0")
	closeDataNode0 := setup.DataNode(ep)
	By("Starting data node 1")
	closeDataNode1 := setup.DataNode(ep)
	By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)
	By("Initializing test cases")
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(liaisonAddr, now)
	deferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		closeDataNode1()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}
	return []byte(liaisonAddr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesstream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casestopn.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {
	deferFunc()
	Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
