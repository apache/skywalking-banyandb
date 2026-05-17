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

package query_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

// Vec independent verification on a standalone process.
//
// Boots a *separate* standalone with --measure-vectorized-enabled=true and
// replays the same Measure / TopN test entries the row-path integration
// suite already covers in suite_test.go. Each case asserts the row-path's
// expected output, so every greenness here is an INDEPENDENT verification
// that vec produces the same reference InternalDataPoints on its own
// merits — not a row-vec same-process diff, but vec running against the
// reference yaml the row path agreed with first. The cluster is fresh
// and isolated so neither side observes the other's state.
//
// This is the standalone twin of test/integration/distributed/query/
// vectorized_test.go. Together they satisfy the directive that integration
// standalone and distributed verify row and vec independently.
//
// With G8d (top-level vec dispatch) wired, plain measure queries take the
// new pkg/query/vectorized/measure/plan.Dispatch path instead of the legacy
// leaf-substitution at localIndexScan.maybeVectorized. Queries with hidden
// criteria tags (G9d) also route through Dispatch: the hidden tags are
// projected for storage filtering and stripped at egress so the wire bytes
// match the row path. GroupBy/Agg/Top continue through the row plan with
// leaf substitution. The AfterAll assertion below uses vecplan.HandledCount
// to confirm dispatch actually fires for at least one query — protecting
// against a silent regression where the eligibility gate excludes
// everything.
//
// This block runs after the on-disk-data Describe in round2.go, which closes
// the original cluster. The integration suite remains a release-candidate
// gate; the unit-level differential tests in pkg/query/vectorized/measure
// gate every PR.
var _ = ginkgo.Describe("vec independent verification (standalone)", ginkgo.Ordered, func() {
	var (
		vectorizedConn *grpc.ClientConn
		stopFn         func()
		// Snapshot dispatch counters so the AfterAll can compute the
		// delta this Describe's specs produced.
		startHandledCount     int64
		startFellThroughCount int64
		// Save the package-global SharedContexts so AfterAll can restore
		// them. Sibling Describes may run *between* this AfterAll and the
		// next BeforeAll (e.g. the top-level "TopN Tests" / "Scanning
		// Measures" tables); leaving SharedContext pointing at our
		// closed-down cluster makes those siblings time out.
		savedMeasureCtx  helpers.SharedContext
		savedTopNCtx     helpers.SharedContext
		savedWireModeRaw bool
	)
	ginkgo.BeforeAll(func() {
		savedMeasureCtx = casesmeasure.SharedContext
		savedTopNCtx = casestopn.SharedContext
		// data.MeasureWireModeRaw is a per-process atomic — the vec
		// cluster's PreRun flips it to true and the original row-baseline
		// cluster (still alive in the same test binary) would then start
		// hitting the flag-on raw-frame guard for its own responses.
		savedWireModeRaw = data.MeasureWireModeRaw()
		startHandledCount = vecplan.HandledCount()
		startFellThroughCount = vecplan.FellThroughCount()
		path, diskCleanupFn, pathErr := test.NewSpace()
		gomega.Expect(pathErr).NotTo(gomega.HaveOccurred())
		ports, portsErr := test.AllocateFreePorts(5)
		gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		addr, _, closeFn := setup.ClosableStandalone(config, path, ports,
			"--measure-vectorized-enabled=true",
		)
		stopFn = func() {
			closeFn()
			diskCleanupFn()
			tmpDirCleanup()
		}
		var connErr error
		vectorizedConn, connErr = grpchelper.Conn(addr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		test_cases.Initialize(addr, now)
		sharedCtx := helpers.SharedContext{
			Connection: vectorizedConn,
			BaseTime:   now,
		}
		casesmeasure.SharedContext = sharedCtx
		casestopn.SharedContext = sharedCtx
	})
	ginkgo.AfterAll(func() {
		// Restore the saved SharedContexts BEFORE tearing the cluster down
		// so any sibling Describe that runs between this AfterAll and its
		// own BeforeAll observes a live connection (the original cluster
		// #1 from SynchronizedBeforeSuite is still up at this point).
		casesmeasure.SharedContext = savedMeasureCtx
		casestopn.SharedContext = savedTopNCtx
		// Restore the per-process wire-mode flag BEFORE tearing down so
		// any later test that targets the row baseline cluster (same
		// process) sees the original proto codec contract.
		data.SetMeasureWireModeRaw(savedWireModeRaw)
		// G8e observability: dispatch MUST fire for at least one of the
		// replayed cases. If this assertion ever drops to zero, the
		// vec subsystem is silently 0%-covered — either the dispatch
		// eligibility gate is too tight or the wire-up regressed.
		handledDelta := vecplan.HandledCount() - startHandledCount
		fellThroughDelta := vecplan.FellThroughCount() - startFellThroughCount
		ginkgo.GinkgoWriter.Printf(
			"vec dispatch (standalone): handled=%d fell_through=%d (deltas across vec-standalone table)\n",
			handledDelta, fellThroughDelta,
		)
		gomega.Expect(handledDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec dispatch did not fire for any case in the vec-standalone table; "+
				"either the eligibility gate is too tight or processor.go's tryVecDispatch regressed")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casesmeasure.RegisterTable("Vec (standalone): scanning measures")
	casestopn.RegisterTable("Vec (standalone): TopN")
})
