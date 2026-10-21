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

package rbacdata_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// This table uses real protected endpoints, metadata, storage and execution in both
// topologies. The large projection deliberately exceeds the 16 MiB execution budget.
var _ = g.DescribeTable("T02 gate-error-cancel-budget real RPC", func(distributed bool, memoryMiB int) {
	gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T02 requires -tags=querymetrics")
	root, release, err := test.NewSpace()
	gm.Expect(err).NotTo(gm.HaveOccurred())
	g.DeferCleanup(release)
	policy := filepath.Join(root, "security.yaml")
	gm.Expect(os.WriteFile(policy, []byte(dataPolicy), 0o600)).To(gm.Succeed())
	memoryFlag := fmt.Sprintf("--measure-vectorized-query-memory-mib=%d", memoryMiB)
	var addr string
	if distributed {
		config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(root))
		_, _, _, closeData := setup.DataNodeWithAddrAndDir(config, memoryFlag, "--allowed-bytes=8GiB")
		g.DeferCleanup(closeData)
		var closeLiaison func()
		addr, _, closeLiaison = setup.LiaisonNodeWithHTTPAuth(config, adminActor.name, adminActor.password, "--auth-config-file="+policy, memoryFlag, "--allowed-bytes=8GiB")
		g.DeferCleanup(closeLiaison)
	} else {
		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())
		var closeServer func()
		addr, _, closeServer = setup.EmptyClosableStandalone(nil, root, ports, "--auth-config-file="+policy, memoryFlag, "--allowed-bytes=8GiB")
		g.DeferCleanup(closeServer)
	}
	conn, dialErr := grpclib.NewClient(addr, grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithDefaultCallOptions(grpclib.MaxCallRecvMsgSize(64*1024*1024)))
	gm.Expect(dialErr).NotTo(gm.HaveOccurred())
	g.DeferCleanup(func() { gm.Expect(conn.Close()).To(gm.Succeed()) })
	groups := databasev1.NewGroupRegistryServiceClient(conn)
	registry := databasev1.NewMeasureRegistryServiceClient(conn)
	native := measurev1.NewMeasureServiceClient(conn)
	sql := bydbqlv1.NewBydbQLServiceClient(conn)
	barrier := schemav1.NewSchemaBarrierServiceClient(conn)
	for _, group := range []string{groupAlpha, groupBeta} {
		_, err = groups.Create(adminActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{Group: measureGroup(group)})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		_, err = registry.Create(adminActor.ctx(), &databasev1.MeasureRegistryServiceCreateRequest{Measure: measureSchema(group)})
		gm.Expect(err).NotTo(gm.HaveOccurred())
	}
	_, err = barrier.AwaitSchemaApplied(adminActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:         []*schemav1.SchemaKey{{Kind: "measure", Group: groupAlpha, Name: fixtureMeasure}, {Kind: "measure", Group: groupBeta, Name: fixtureMeasure}},
		MinRevisions: []int64{0, 0},
	})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	at := time.Now().Truncate(time.Millisecond)
	seedMeasureFrames(adminActor, native, markerFrame(groupAlpha, markerAlpha, at, 1))
	baseline := vectorized.QueryMemoryStats().LiveReservedBytes
	queryText := func(group string) string {
		return fmt.Sprintf("SELECT * FROM MEASURE %s IN %s TIME > '-15m' LIMIT 1000", fixtureMeasure, group)
	}

	gm.Eventually(func(inner gm.Gomega) {
		response, queryErr := native.Query(readerAlphaActor.ctx(), markerQuery(at, groupAlpha))
		inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		inner.Expect(markersIn(response)).To(gm.Equal([]string{markerAlpha}))
	}, flags.EventuallyTimeout).Should(gm.Succeed())

	tracedNative := markerQuery(at, groupAlpha)
	tracedNative.Trace = true
	nativeTraceResponse, traceErr := native.Query(readerAlphaActor.ctx(), tracedNative)
	gm.Expect(traceErr).NotTo(gm.HaveOccurred())
	gm.Expect(completedOutputOperator(nativeTraceResponse.GetTrace())).To(gm.BeTrue(),
		"native trace must contain completed output execution with emitted rows: %v", nativeTraceResponse.GetTrace())
	tracedQL := strings.Replace(queryText(groupAlpha), " LIMIT", " WITH QUERY_TRACE LIMIT", 1)
	qlTraceResponse, traceErr := sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: tracedQL})
	gm.Expect(traceErr).NotTo(gm.HaveOccurred())
	gm.Expect(completedOutputOperator(qlTraceResponse.GetMeasureResult().GetTrace())).To(gm.BeTrue(),
		"QL trace must contain completed output execution with emitted rows: %v", qlTraceResponse.GetMeasureResult().GetTrace())
	if distributed {
		gm.Expect(completedDistributedIterator(qlTraceResponse.GetMeasureResult().GetTrace(), 1)).To(gm.BeTrue(),
			"distributed QL trace must include final serialized consumption: %v", qlTraceResponse.GetMeasureResult().GetTrace())
	}
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout).Should(gm.Equal(baseline))
	// Authorization must terminate both public calls before either batch caller executes.
	before := vmeasure.BatchExecutionStats()
	_, err = native.Query(readerAlphaActor.ctx(), markerQuery(at, groupBeta))
	gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied))
	_, err = sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: queryText(groupBeta)})
	gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied))
	gm.Expect(vmeasure.BatchExecutionStats()).To(gm.Equal(before))
	// An already canceled public client must not start storage execution.
	ctx, cancel := context.WithCancel(readerAlphaActor.ctx())
	cancel()
	_, err = native.Query(ctx, markerQuery(at, groupAlpha))
	gm.Expect(status.Code(err)).To(gm.Equal(codes.Canceled))
	_, err = sql.Query(ctx, &bydbqlv1.QueryRequest{Query: queryText(groupAlpha)})
	gm.Expect(status.Code(err)).To(gm.Equal(codes.Canceled))
	gm.Expect(vmeasure.BatchExecutionStats()).To(gm.Equal(before))
	// Revision opt-in remains a native-only request feature in the legacy QL grammar.
	gated := markerQuery(at, groupAlpha)
	gated.GroupModRevisions = map[string]int64{groupAlpha: 1}
	gatedResponse, gateErr := native.Query(readerAlphaActor.ctx(), gated)
	gm.Expect(gateErr).NotTo(gm.HaveOccurred())
	gm.Expect(gatedResponse.GroupStatuses[groupAlpha]).To(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA))
	gm.Expect(gatedResponse.DataPoints).To(gm.BeEmpty())
	gm.Expect(vmeasure.BatchExecutionStats()).To(gm.Equal(before))
	// Exercise a real planning failure, then prove the same endpoint remains usable.
	invalid := markerQuery(at, groupAlpha)
	invalid.FieldProjection.Names = []string{"missing_field"}
	_, err = native.Query(readerAlphaActor.ctx(), invalid)
	gm.Expect(err).To(gm.HaveOccurred())
	_, err = sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{
		Query: fmt.Sprintf("SELECT missing_field FROM MEASURE %s IN %s TIME > '-15m'", fixtureMeasure, groupAlpha),
	})
	gm.Expect(err).To(gm.HaveOccurred())
	small, smallErr := sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: queryText(groupAlpha)})

	gm.Expect(smallErr).NotTo(gm.HaveOccurred())
	gm.Expect(markersIn(small.GetMeasureResult())).To(gm.Equal([]string{markerAlpha}))
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout).Should(gm.Equal(baseline))
	// Each row is unique so storage cannot collapse the large projected string.
	const rows = 768
	payloadSize := 4 * 1024
	payload := strings.Repeat("x", payloadSize)
	frames := make([]*measurev1.WriteRequest, rows)
	for i := range frames {
		frames[i] = markerFrame(groupAlpha, fmt.Sprintf("%04d-%s", i, payload), at, uint64(i+2))
	}
	seedMeasureFrames(adminActor, native, frames...)
	wide := markerQuery(at, groupAlpha)
	wide.Limit = 1000

	if memoryMiB == 16 {
		// The selected index/SeriesData itself can exceed this allowance, so a
		// visibility COUNT must obey the same limit. Poll the budget refusal while
		// bulk writes become queryable; the 256 MiB cases verify every seeded row.
		for _, ql := range []bool{false, true} {
			gm.Eventually(func() codes.Code {
				if ql {
					_, queryErr := sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: queryText(groupAlpha)})
					return status.Code(queryErr)
				}
				_, queryErr := native.Query(readerAlphaActor.ctx(), wide)
				return status.Code(queryErr)
			}, flags.EventuallyTimeout).Should(gm.Equal(codes.ResourceExhausted))
			gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout).Should(gm.Equal(baseline))
		}
		return
	}
	// Wait for visibility through a tiny projection outside the resource assertion.
	countQuery := fmt.Sprintf("SELECT COUNT(value) FROM MEASURE %s IN %s TIME > '-15m'", fixtureMeasure, groupAlpha)
	gm.Eventually(func(inner gm.Gomega) {
		response, queryErr := sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: countQuery})
		inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		inner.Expect(response.GetMeasureResult().GetDataPoints()).To(gm.HaveLen(1))
		inner.Expect(response.GetMeasureResult().GetDataPoints()[0].GetFields()[0].GetValue().GetInt().GetValue()).To(gm.Equal(int64(rows + 1)))
	}, flags.EventuallyTimeout).Should(gm.Succeed())

	// Count and raw projection can follow different scan paths. Verify raw
	// visibility independently through a narrow projection before asserting
	// large-output resource behavior.
	visible := markerQuery(at, groupAlpha)
	visible.TagProjection = nil
	visible.Limit = 1000
	gm.Eventually(func(inner gm.Gomega) {
		response, queryErr := native.Query(readerAlphaActor.ctx(), visible)
		inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		inner.Expect(response.GetDataPoints()).To(gm.HaveLen(rows + 1))
	}, flags.EventuallyTimeout).Should(gm.Succeed())
	for _, ql := range []bool{false, true} {
		var response *measurev1.QueryResponse
		if ql {
			qlResponse, queryErr := sql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: queryText(groupAlpha)})
			err = queryErr
			response = qlResponse.GetMeasureResult()
		} else {
			response, err = native.Query(readerAlphaActor.ctx(), wide)
		}
		if memoryMiB == 16 {
			gm.Expect(status.Code(err)).To(gm.Equal(codes.ResourceExhausted), "large projected output must fail before crossing 16MiB, ql=%v", ql)
		} else {
			gm.Expect(err).NotTo(gm.HaveOccurred(), "256MiB output must succeed, ql=%v", ql)
			gm.Expect(response.DataPoints).To(gm.HaveLen(rows + 1))
		}
		gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout).Should(gm.Equal(baseline))
	}
}, g.Entry("standalone16MiB", false, 16), g.Entry("standalone256MiB", false, 256),
	g.Entry("distributed16MiB", true, 16), g.Entry("distributed256MiB", true, 256))

// completedOutputOperator requires actual consumed rows and a finalized output span.
func completedOutputOperator(trace *commonv1.Trace) bool {
	var visit func([]*commonv1.Span) bool
	visit = func(spans []*commonv1.Span) bool {
		for _, span := range spans {
			if span.GetMessage() == "limit" && span.GetEndTime() != nil {
				for _, tag := range span.GetTags() {
					if tag.GetKey() == "rows_out" && tag.GetValue() == "1" {
						return true
					}
				}
			}
			if visit(span.GetChildren()) {
				return true
			}
		}
		return false
	}
	return visit(trace.GetSpans())
}

func completedDistributedIterator(trace *commonv1.Trace, rows int) bool {
	var visit func([]*commonv1.Span) bool
	visit = func(spans []*commonv1.Span) bool {
		for _, parent := range spans {
			if strings.HasPrefix(parent.GetMessage(), "distributed-") && parent.GetEndTime() != nil {
				for _, child := range parent.GetChildren() {
					if child.GetMessage() != "iterator" || child.GetEndTime() == nil || child.GetEndTime().AsTime().After(parent.GetEndTime().AsTime()) {
						continue
					}
					tags := make(map[string]string)
					for _, tag := range child.GetTags() {
						tags[tag.GetKey()] = tag.GetValue()
					}
					if tags["rounds"] == fmt.Sprint(rows) && tags["size"] == fmt.Sprint(rows) {
						return true
					}
				}
			}
			if visit(parent.GetChildren()) {
				return true
			}
		}
		return false
	}
	return visit(trace.GetSpans())
}
