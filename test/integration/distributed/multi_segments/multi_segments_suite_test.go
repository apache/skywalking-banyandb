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

package integration_multi_segments_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	casesproperty "github.com/apache/skywalking-banyandb/test/cases/property"
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
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

func TestDistributedMultiSegments(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Multi Segments Suite")
}

var (
	connection *grpc.ClientConn
	now        time.Time
	baseTime   time.Time
	deferFunc  func()
	goods      []gleak.Goroutine
)

var _ = SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())

	By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	dir, spaceDef, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(dir),
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
	test_stream.PreloadSchema(ctx, schemaRegistry)
	test_measure.PreloadSchema(ctx, schemaRegistry)
	test_trace.PreloadSchema(ctx, schemaRegistry)
	test_property.PreloadSchema(ctx, schemaRegistry)

	By("Starting data node 0")
	closeDataNode0 := setup.DataNode(ep)
	By("Starting data node 1")
	closeDataNode1 := setup.DataNode(ep)
	By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)

	By("Initializing test cases")
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	baseTime = time.Date(now.Year(), now.Month(), now.Day(), 0o0, 0o2, 0, 0, now.Location())
	test_cases.Initialize(liaisonAddr, baseTime)

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
		BaseTime:   baseTime,
	}
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   baseTime,
	}
	casestopn.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   baseTime,
	}
	casesproperty.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   baseTime,
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Distributed Multi Segments Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
