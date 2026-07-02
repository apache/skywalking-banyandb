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

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

// Vec trace independent verification on a standalone process.
//
// Boots a *separate* standalone with --trace-vectorized-enabled=true and
// replays the same Trace test entries the row-based integration suite
// already covers. Each case asserts the row-path's expected output, so
// every greenness here is an INDEPENDENT verification that the vectorized
// trace path produces the same results on its own merits.
//
// The cluster is fresh and isolated so neither side observes the other's
// state. This is the standalone twin of test/integration/distributed/query/
// vectorized_trace_test.go.
var _ = ginkgo.Describe("vec trace independent verification (standalone)", ginkgo.Ordered, func() {
	var (
		vectorizedConn  *grpc.ClientConn
		stopFn          func()
		startQueryCount int64
		savedTraceCtx   helpers.SharedContext
	)
	ginkgo.BeforeAll(func() {
		savedTraceCtx = casestrace.SharedContext
		startQueryCount = vtrace.QueryCount()

		path, diskCleanupFn, pathErr := test.NewSpace()
		gomega.Expect(pathErr).NotTo(gomega.HaveOccurred())
		ports, portsErr := test.AllocateFreePorts(5)
		gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		addr, _, closeFn := setup.ClosableStandalone(config, path, ports,
			"--trace-vectorized-enabled=true",
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
		casestrace.SharedContext = helpers.SharedContext{
			Connection: vectorizedConn,
			BaseTime:   now,
		}
	})
	ginkgo.AfterAll(func() {
		// Restore the saved SharedContext before tearing down so any sibling
		// Describe that runs after this one sees the original live connection.
		casestrace.SharedContext = savedTraceCtx
		queryCountDelta := vtrace.QueryCount() - startQueryCount
		ginkgo.GinkgoWriter.Printf(
			"vec trace dispatch (standalone): query_count=%d (delta across vec-trace-standalone table)\n",
			queryCountDelta,
		)
		gomega.Expect(queryCountDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec trace dispatch did not fire for any case in the vec-trace-standalone table; "+
				"--trace-vectorized-enabled=true may not have taken effect")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casestrace.RegisterTable("Vec (standalone): Scanning Traces")
})
