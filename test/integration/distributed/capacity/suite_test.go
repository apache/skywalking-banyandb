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

// Package capacity_test finds, on an in-process cluster with a pinned query memory, how many results each data model can
// return from a data set far larger than the memory. The ladder runs twice per model: once with a data node that is short
// of memory and a liaison that is not, and once the other way round, so the capacity of each tier shows on its own.
package capacity_test

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
	"github.com/apache/skywalking-banyandb/test/cases/capacity"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestDistributedCapacity(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Distributed Query Capacity Suite", g.Label(integration_distributed.Labels...))
}

const (
	streamPool    = 64 << 20
	tracePool     = 64 << 20
	measurePool   = 256 << 20
	indexModePool = 64 << 20
	// roomyFactor is how much more query pool the tier that is not under test gets.
	roomyFactor = 8
	// liaisonServiceProtectors is how many protectors a liaison creates before the one its query service uses.
	liaisonServiceProtectors = 2
)

// The floors are the fewest results each scenario must serve with the memory short on one tier, a regression guard set below
// what a node serves today. They are raised when a change makes a model serve more.
func init() {
	register(capacity.StreamHotSeries(streamPool), expects{dataShort: 35, liaisonShort: 45})
	register(capacity.TraceNewest(tracePool), expects{dataShort: 40, liaisonShort: 20})
	register(capacity.MeasurePoints(measurePool), expects{dataShort: 2000, liaisonShort: 380})
	register(capacity.MeasureIndexMode(indexModePool), expects{dataShort: 900, liaisonShort: 1800})
}

// expects are the floors of one scenario, on a short data node and on a short liaison.
type expects struct {
	dataShort, liaisonShort int
}

// register runs one scenario twice: with the data node short of query memory, and with the liaison short of it.
func register(sc *capacity.Scenario, floors expects) {
	for _, tier := range []struct {
		name   string
		expect capacity.Expect
		data   uint64
		li     uint64
	}{
		{name: "data node short of memory", data: sc.PoolBytes, li: sc.PoolBytes * roomyFactor, expect: capacity.Expect{Floor: floors.dataShort}},
		{name: "liaison short of memory", data: sc.PoolBytes * roomyFactor, li: sc.PoolBytes, expect: capacity.Expect{Floor: floors.liaisonShort}},
	} {
		g.Describe("query capacity (distributed, "+tier.name+"): "+sc.Name, func() {
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
				// The data node starts an embedded schema server first, which creates a protector of its own; skip it.
				protector.SetQueryPoolAfter(1, tier.data)
				closeData := setup.DataNode(config)
				setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema)
				config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
				// A liaison creates two protectors of its own, for services other than the query service, before the one the
				// query service uses: pinning the first would leave the liaison with its default query memory.
				protector.SetQueryPoolAfter(liaisonServiceProtectors, tier.li)
				liaisonAddr, closeLiaison := setup.LiaisonNode(config)
				stop = func() {
					closeLiaison()
					closeData()
					releaseSpace()
				}
				conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()),
					// The ladder reads responses far larger than the default 4 MiB: a client that stopped there would measure itself.
					grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<30)))
				gm.Expect(err).NotTo(gm.HaveOccurred())
			})
			g.AfterEach(func() {
				gm.Expect(conn.Close()).To(gm.Succeed())
				stop()
				gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
			})
			g.It("serves a limit up to its capacity and refuses a larger one with RESOURCE_EXHAUSTED", func() {
				ns := timestamp.NowMilli().UnixNano()
				base := time.Unix(0, ns-ns%int64(time.Minute))
				capacity.Run(conn, base, sc, tier.expect, "distributed, "+tier.name)
			})
		})
	}
}
