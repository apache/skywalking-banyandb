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

// Package capacity_test finds, on a standalone node with a pinned query memory, how many results each data model can return
// from a data set far larger than the memory: a ladder of limits per model, whose top rung is the capacity of the node.
package capacity_test

import (
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/test/cases/capacity"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestStandaloneCapacity(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Standalone Query Capacity Suite", g.Label(integration_standalone.Labels...))
}

// The floors are the fewest results each scenario must serve on the memory it is given, a regression guard set below what a
// node serves today. They are raised when a change makes a model serve more.
func init() {
	register(capacity.StreamHotSeries(streamPool), capacity.Expect{Floor: 20})
	register(capacity.TraceNewest(tracePool), capacity.Expect{Floor: 7})
	register(capacity.MeasurePoints(measurePool), capacity.Expect{Floor: 300})
	register(capacity.MeasureIndexMode(indexModePool), capacity.Expect{Floor: 700})
}

const (
	streamPool    = 64 << 20
	tracePool     = 64 << 20
	measurePool   = 256 << 20
	indexModePool = 64 << 20
)

// startStandalone starts a standalone node whose query pool is poolBytes and returns a client of it and the function that stops
// both.
func startStandalone(poolBytes uint64) (*grpc.ClientConn, func()) {
	gm.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gm.Succeed())
	// The pool is pinned for the protector that the standalone node creates next.
	protector.SetNextQueryPool(poolBytes)
	addr, _, closeFn := setup.Standalone(nil)
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()),
		// The ladder reads responses far larger than the default 4 MiB: a client that stopped there would measure itself.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<30)))
	gm.Expect(err).NotTo(gm.HaveOccurred())
	return conn, func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		closeFn()
	}
}

func queryBase() time.Time {
	ns := timestamp.NowMilli().UnixNano()
	return time.Unix(0, ns-ns%int64(time.Minute))
}

// register runs one scenario on a standalone node whose query pool is the scenario's.
func register(sc *capacity.Scenario, expect capacity.Expect) {
	g.Describe("query capacity (standalone): "+sc.Name, func() {
		var (
			goods []gleak.Goroutine
			conn  *grpc.ClientConn
			stop  func()
		)
		g.BeforeEach(func() {
			goods = gleak.Goroutines()
			conn, stop = startStandalone(sc.PoolBytes)
		})
		g.AfterEach(func() {
			stop()
			gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})
		g.It("serves a limit up to its capacity and refuses a larger one with RESOURCE_EXHAUSTED", func() {
			capacity.Run(conn, queryBase(), sc, expect, "standalone")
		})
	})
	g.Describe("query memory follows the budget (standalone): "+sc.Name, func() {
		var goods []gleak.Goroutine
		g.BeforeEach(func() { goods = gleak.Goroutines() })
		g.AfterEach(func() {
			gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})
		g.It("raises the capacity with the memory and holds no more memory per result than the budget charges", func() {
			small := *sc
			conn, stop := startStandalone(small.PoolBytes)
			smallResult, _ := capacity.Climb(conn, queryBase(), &small, "standalone, the pool")
			stop()

			large := *sc
			large.PoolBytes = 2 * sc.PoolBytes
			conn, stop = startStandalone(large.PoolBytes)
			defer stop()
			largeResult, probe := capacity.Climb(conn, queryBase(), &large, "standalone, twice the pool")
			capacity.CheckScaling(smallResult, largeResult, sc.Name)
			capacity.CheckMarginalMemory(smallResult, largeResult, small.PoolBytes, large.PoolBytes, probe, sc.Name)
		})
	})
}
