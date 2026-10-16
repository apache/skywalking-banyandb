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
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

// Vec independent verification on a distributed cluster.
//
// Boots a *separate* distributed cluster — 2 data nodes + 1 liaison — and
// replays the Measure / TopN test entries against it. The shared cluster in
// common.go registers no tables, so this Describe is the distributed suite's
// only Measure and TopN coverage. Each case asserts the committed reference
// yaml; every greenness here is an INDEPENDENT verification that vec produces
// the same InternalDataPoints on the cluster wire.
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
		vectorizedConn    *grpc.ClientConn
		dataNodeAddrs     []string
		stopFn            func()
		startHandledCount int64
		startFrameEnc     int64
		startFrameDec     int64
		savedMeasureCtx   helpers.SharedContext
		savedTopNCtx      helpers.SharedContext
		savedWireModeRaw  bool
		startPins         int64
	)
	ginkgo.BeforeAll(func() {
		startPins = vectorized.OutstandingOwnerPins()
		savedMeasureCtx = casesmeasure.SharedContext
		savedTopNCtx = casestopn.SharedContext
		// data.MeasureWireModeRaw is a per-process atomic — the vec
		// cluster's PreRun flips it to true and the row baseline cluster
		// (alive in the same test binary) would then start hitting the
		// flag-on raw-frame guard for its own responses. Save the
		// current value here and restore it in AfterAll so the row
		// baseline keeps its proto-codec contract end-to-end.
		savedWireModeRaw = data.MeasureWireModeRaw()
		startHandledCount = vecplan.HandledCount()
		startFrameEnc = data.MeasureFrameEncodedCount()
		startFrameDec = data.MeasureFrameDecodedCount()

		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		dataAddr0, _, _, closeDataNode0 := setup.DataNodeWithAddrAndDir(config)
		dataAddr1, _, _, closeDataNode1 := setup.DataNodeWithAddrAndDir(config)
		dataNodeAddrs = []string{dataAddr0, dataAddr1}
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
		// Restore the per-process wire-mode flag BEFORE tearing down so
		// any subsequent test that targets the row baseline cluster (same
		// process) sees the original proto codec contract.
		data.SetMeasureWireModeRaw(savedWireModeRaw)
		// Observability gate: vec dispatch must fire for at least one
		// case in the table. If this drops to zero the vec subsystem is
		// silently 0%-covered on the distributed cluster — either the
		// eligibility gate or processor.go's dispatchMeasure regressed.
		handledDelta := vecplan.HandledCount() - startHandledCount
		ginkgo.GinkgoWriter.Printf(
			"vec dispatch (distributed): handled=%d (delta across the vec-distributed table)\n", handledDelta)
		gomega.Expect(handledDelta).To(gomega.BeNumerically(">", int64(0)),
			"vec dispatch did not fire for any case on the distributed cluster")
		// HandledCount above only proves the vec COMPUTE path dispatched. A vec query
		// still emits protobuf unless the data node is distributed and in raw wire
		// mode, so assert the columnar frame itself carried traffic — otherwise a
		// silent fallback to proto reads as full coverage.
		frameEncDelta := data.MeasureFrameEncodedCount() - startFrameEnc
		frameDecDelta := data.MeasureFrameDecodedCount() - startFrameDec
		ginkgo.GinkgoWriter.Printf(
			"vec measure wire (distributed): frames_encoded=%d frames_decoded=%d\n",
			frameEncDelta, frameDecDelta,
		)
		gomega.Expect(frameEncDelta).To(gomega.BeNumerically(">", int64(0)),
			"no native columnar frame was encoded on the distributed cluster; "+
				"the data node fell back to protobuf")
		gomega.Expect(frameDecDelta).To(gomega.BeNumerically(">", int64(0)),
			"no native columnar frame was decoded on the distributed cluster; "+
				"the liaison never saw a frame body")
		// Every query is done: the liaison and the data nodes must have released every pin they took.
		gomega.Eventually(vectorized.OutstandingOwnerPins, 30*time.Second, 100*time.Millisecond).Should(gomega.Equal(startPins),
			"owner pins outlived the queries")
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casesmeasure.RegisterTable("Vec (distributed): scanning measures")
	observeQLFixture := func(request *measurev1.QueryRequest, want *measurev1.QueryResponse) {
		body, marshalErr := proto.Marshal(request)
		gomega.Expect(marshalErr).NotTo(gomega.HaveOccurred())
		for nodeIndex, addr := range dataNodeAddrs {
			conn, connErr := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
			func() {
				defer func() { gomega.Expect(conn.Close()).To(gomega.Succeed()) }()
				gomega.Eventually(func(inner gomega.Gomega) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					stream, streamErr := clusterv1.NewServiceClient(conn).Send(ctx)
					inner.Expect(streamErr).NotTo(gomega.HaveOccurred())
					// This RPC targets exactly one data node; no liaison or replica selection participates.
					inner.Expect(stream.Send(&clusterv1.SendRequest{
						Topic: data.TopicMeasureQuery.String(), MessageId: 1, Body: body, Group: "replicated_group",
					})).To(gomega.Succeed())
					inner.Expect(stream.CloseSend()).To(gomega.Succeed())
					response, recvErr := stream.Recv()
					inner.Expect(recvErr).NotTo(gomega.HaveOccurred())
					inner.Expect(response.GetError()).To(gomega.BeEmpty())
					got := &measurev1.QueryResponse{}
					inner.Expect(proto.Unmarshal(response.GetBody(), got)).To(gomega.Succeed())
					inner.Expect(measuredata.CompareQLResult(got, want, false)).To(gomega.Succeed())
					for _, point := range got.GetDataPoints() {
						inner.Expect(point.GetVersion()).To(gomega.Equal(int64(2)), "obsolete fixture version on data node %d", nodeIndex)
					}
				}, flags.EventuallyTimeout).Should(gomega.Succeed(), "fixture F on data node %d (%s)", nodeIndex, addr)
			}()
			ginkgo.GinkgoWriter.Printf("QL fixture F: data_node=%d address=%s rows=6 version=2 (direct node RPC)\n", nodeIndex, addr)
		}
	}
	casesmeasure.RegisterQLTable("QL measure RPC verification", observeQLFixture)
	casesmeasure.RegisterQLBatchTable("QL shared batch execution", observeQLFixture)
	casestopn.RegisterTable("Vec (distributed): TopN")
})
