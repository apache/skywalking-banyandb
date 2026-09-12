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

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
)

// Vec stream independent verification on a distributed cluster.
//
// Boots a *separate* distributed cluster — 2 data nodes + 1 liaison — and
// replays the Stream test entries against it. The shared cluster in common.go
// registers no tables, so this Describe is the distributed suite's only Stream
// coverage. Each case asserts the committed reference yaml; every greenness here
// is an INDEPENDENT verification that the vectorized stream path produces the
// same results on the cluster wire (data node emits the native columnar frame;
// the liaison decodes it, merges, dedups, and limits).
//
// The AfterAll QueryCount delta assertion proves at least one case actually
// reached the vectorized scan rather than the table passing on a path that never
// fired.
//
// This is the distributed twin of test/integration/standalone/query/
// vectorized_stream_test.go and mirrors the trace vec distributed verification.
var _ = ginkgo.Describe("vec stream independent verification (distributed)", ginkgo.Ordered, func() {
	var (
		vectorizedConn  *grpc.ClientConn
		stopFn          func()
		startQueryCount int64
		startFrameEnc   int64
		startFrameDec   int64
		savedStreamCtx  helpers.SharedContext
	)
	ginkgo.BeforeAll(func() {
		savedStreamCtx = casesstream.SharedContext
		startQueryCount = vstream.QueryCount()
		startFrameEnc = data.StreamFrameEncodedCount()
		startFrameDec = data.StreamFrameDecodedCount()

		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		closeDataNode0 := setup.DataNode(config)
		closeDataNode1 := setup.DataNode(config)
		setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema)
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config)
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
		casesstream.SharedContext = helpers.SharedContext{
			Connection: vectorizedConn,
			BaseTime:   now,
		}
	})
	ginkgo.AfterAll(func() {
		// Restore the saved SharedContext before tearing down so any sibling
		// Describe that runs after this one sees the original live connection.
		casesstream.SharedContext = savedStreamCtx
		queryCountDelta := vstream.QueryCount() - startQueryCount
		ginkgo.GinkgoWriter.Printf(
			"vec stream dispatch (distributed): query_count=%d (delta across vec-stream-distributed table)\n",
			queryCountDelta,
		)
		gomega.Expect(queryCountDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec stream dispatch did not fire for any case on the distributed cluster; "+
				"vstream.QueryCount() never moved, so no query reached the vectorized stream scan")
		// QueryCount above only proves the vec COMPUTE path dispatched — a vec query
		// still emits protobuf unless the data node is distributed and in raw wire
		// mode. Assert the columnar frame itself carried traffic, so this suite fails
		// loudly if the wire format silently regresses to proto (a 48h standalone soak
		// passed while never encoding a single frame, because nothing checked).
		frameEncDelta := data.StreamFrameEncodedCount() - startFrameEnc
		frameDecDelta := data.StreamFrameDecodedCount() - startFrameDec
		ginkgo.GinkgoWriter.Printf(
			"vec stream wire (distributed): frames_encoded=%d frames_decoded=%d\n",
			frameEncDelta, frameDecDelta,
		)
		gomega.Expect(frameEncDelta).To(gomega.BeNumerically(">", int64(0)),
			"no native columnar frame was encoded on the distributed cluster; "+
				"the data node fell back to protobuf")
		gomega.Expect(frameDecDelta).To(gomega.BeNumerically(">", int64(0)),
			"no native columnar frame was decoded on the distributed cluster; "+
				"the liaison never saw a frame body")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casesstream.RegisterTable("Vec (distributed): Scanning Streams")
})
