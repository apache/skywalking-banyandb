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
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

// Vec independent verification on a distributed cluster.
//
// Boots a *separate* distributed cluster — 2 data nodes + 1 liaison, all
// launched with --measure-vectorized-enabled=true — and replays the same
// Measure / TopN test entries the row-path distributed suite already
// covers in common.go. Each case asserts the row-path's expected output;
// every greenness here is an INDEPENDENT verification that vec produces
// the same reference InternalDataPoints on the cluster wire.
//
// This is the distributed twin of test/integration/standalone/query/
// vectorized_test.go. Together they satisfy the "integration standalone
// and distributed verify row and vec independently" directive.
//
// G9f.5 no-fall-through directive: under flag-on the data-node Rev now
// emits raw vec columnar frame bodies (not proto), and the liaison's
// distributedPlan decodes them via ReduceFramesToInternalDataPoints /
// DecodeFramesToInternalDataPoints. Any vec gap (e.g. multi-measure /
// hidden-criteria-tag / trace queries that don't implement RawFrameSource
// today) surfaces here as a hard error rather than a silent retry on
// row — that is the loud-failure signal the runbook depends on.
var _ = ginkgo.Describe("vec independent verification (distributed)", ginkgo.Ordered, func() {
	var (
		vectorizedConn        *grpc.ClientConn
		stopFn                func()
		startHandledCount     int64
		startFellThroughCount int64
		savedMeasureCtx       helpers.SharedContext
		savedTopNCtx          helpers.SharedContext
	)
	ginkgo.BeforeAll(func() {
		savedMeasureCtx = casesmeasure.SharedContext
		savedTopNCtx = casestopn.SharedContext
		startHandledCount = vecplan.HandledCount()
		startFellThroughCount = vecplan.FellThroughCount()

		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		closeDataNode0 := setup.DataNode(config, "--measure-vectorized-enabled=true")
		closeDataNode1 := setup.DataNode(config, "--measure-vectorized-enabled=true")
		setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema)
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config, "--measure-vectorized-enabled=true")
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
		sharedCtx := helpers.SharedContext{
			Connection: vectorizedConn,
			BaseTime:   now,
		}
		casesmeasure.SharedContext = sharedCtx
		casestopn.SharedContext = sharedCtx
	})
	ginkgo.AfterAll(func() {
		casesmeasure.SharedContext = savedMeasureCtx
		casestopn.SharedContext = savedTopNCtx
		// Observability gate: vec dispatch must fire for at least one
		// case in the table. If this drops to zero the vec subsystem is
		// silently 0%-covered on the distributed cluster — either the
		// eligibility gate or processor.go's tryVecDispatch regressed.
		handledDelta := vecplan.HandledCount() - startHandledCount
		fellThroughDelta := vecplan.FellThroughCount() - startFellThroughCount
		ginkgo.GinkgoWriter.Printf(
			"vec dispatch (distributed): handled=%d fell_through=%d (deltas across vec-distributed table)\n",
			handledDelta, fellThroughDelta,
		)
		gomega.Expect(handledDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec dispatch did not fire for any case on the distributed cluster")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casesmeasure.RegisterTable("Vec (distributed): scanning measures")
	casestopn.RegisterTable("Vec (distributed): TopN")
})
