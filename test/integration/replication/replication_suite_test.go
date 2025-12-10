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
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
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
	etcdEndpoint    string
	dataNodeClosers []func()
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()

	By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	dir, spaceDef, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	etcdEndpoint = ep

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
	// Preload all schemas since test_cases.Initialize needs them
	test_stream.PreloadSchema(ctx, schemaRegistry)
	test_measure.PreloadSchema(ctx, schemaRegistry)
	test_trace.PreloadSchema(ctx, schemaRegistry)
	test_property.PreloadSchema(ctx, schemaRegistry)

	By("Starting 3 data nodes for replication test")
	dataNodeClosers = make([]func(), 0, 3)

	for i := 0; i < 3; i++ {
		closeDataNode := setup.DataNode(ep)
		dataNodeClosers = append(dataNodeClosers, closeDataNode)
	}

	By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)

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
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}

	suiteConfig := map[string]interface{}{
		"liaison_addr":  liaisonAddr,
		"etcd_endpoint": etcdEndpoint,
		"now":           now.UnixNano(),
	}

	configBytes, err := json.Marshal(suiteConfig)
	Expect(err).NotTo(HaveOccurred())
	return configBytes
}, func(configBytes []byte) {
	var config map[string]interface{}
	err := json.Unmarshal(configBytes, &config)
	Expect(err).NotTo(HaveOccurred())

	liaisonAddr = config["liaison_addr"].(string)
	etcdEndpoint = config["etcd_endpoint"].(string)
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
