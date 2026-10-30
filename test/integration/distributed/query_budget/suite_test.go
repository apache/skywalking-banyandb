// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package query_budget_test verifies, on an in-process cluster, that a stream query over the memory allowance of a
// data node is reported to the client as RESOURCE_EXHAUSTED.
package query_budget_test

import (
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/test/cases/stream/budget"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestDistributedQueryBudget(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Distributed Stream Query Budget Suite", g.Label(integration_distributed.Labels...))
}

var _ = g.Describe("stream query budget (distributed)", func() {
	var (
		goods []gleak.Goroutine
		conn  *grpc.ClientConn
		stop  func()
	)
	g.BeforeEach(func() {
		goods = gleak.Goroutines()
		gm.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gm.Succeed())
		dir, releaseSpace, err := test.NewSpace()
		gm.Expect(err).NotTo(gm.HaveOccurred())
		config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(dir))
		// Only the data node runs short of budget; the liaison keeps its default pool, so the failure
		// is the data node's and has to cross the wire to reach the client.
		// The data node starts an embedded schema server first, which creates a protector of its own; skip it.
		protector.SetQueryPoolAfter(1, budget.QueryPoolBytes)
		closeData := setup.DataNode(config)
		setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema)
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		liaisonAddr, closeLiaison := setup.LiaisonNode(config)
		stop = func() {
			closeLiaison()
			closeData()
			releaseSpace()
		}
		conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})
	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		stop()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	g.It("reports a query over the data node's memory allowance as RESOURCE_EXHAUSTED", func() {
		ns := timestamp.NowMilli().UnixNano()
		base := time.Unix(0, ns-ns%int64(time.Minute))
		budget.Write(conn, base)
		budget.ExpectFitsAndExceeds(conn, base)
	})
})
