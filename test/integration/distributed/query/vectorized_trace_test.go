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

package query

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

// Vec trace independent verification on a distributed cluster.
//
// Boots a *separate* distributed cluster — 2 data nodes + 1 liaison, all
// launched with --trace-vectorized-enabled=true — and replays the same
// Trace test entries the row-based distributed suite already covers in
// common.go. Each case asserts the row-path's expected output; every
// greenness here is an INDEPENDENT verification that the vectorized trace
// path produces the same results on the cluster wire.
//
// This is the distributed twin of test/integration/standalone/query/
// vectorized_trace_test.go. Together they satisfy the directive that
// integration standalone and distributed verify row and vec independently.
var _ = ginkgo.Describe("vec trace independent verification (distributed)", ginkgo.Ordered, func() {
	var (
		vectorizedConn  *grpc.ClientConn
		stopFn          func()
		startQueryCount int64
		savedTraceCtx   helpers.SharedContext
	)
	ginkgo.BeforeAll(func() {
		savedTraceCtx = casestrace.SharedContext
		startQueryCount = vtrace.QueryCount()

		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		closeDataNode0 := setup.DataNode(config, "--trace-vectorized-enabled=true")
		closeDataNode1 := setup.DataNode(config, "--trace-vectorized-enabled=true")
		setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema)
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config, "--trace-vectorized-enabled=true")
		stopFn = func() {
			closerLiaisonNode()
			closeDataNode0()
			closeDataNode1()
			tmpDirCleanup()
		}
		var connErr error
		vectorizedConn, connErr = grpchelper.Conn(liaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		test_cases.Initialize(liaisonAddr, now)
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
			"vec trace dispatch (distributed): query_count=%d (delta across vec-trace-distributed table)\n",
			queryCountDelta,
		)
		gomega.Expect(queryCountDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec trace dispatch did not fire for any case on the distributed cluster; "+
				"--trace-vectorized-enabled=true may not have taken effect")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casestrace.RegisterTable("Vec (distributed): Scanning Traces")
})
