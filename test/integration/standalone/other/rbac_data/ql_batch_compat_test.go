// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rbacdata_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = g.DescribeTable("T02 small-budget compatibility real RPC", func(distributed bool) {
	gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T02 requires -tags=querymetrics")
	root, release, err := test.NewSpace()
	gm.Expect(err).NotTo(gm.HaveOccurred())
	g.DeferCleanup(release)
	// Eight MiB also exercises the default fallback lease on hosts without a
	// cgroup memory protector. Do not enlarge admission to make these pass.
	const memoryFlag = "--measure-vectorized-query-memory-mib=8"
	var addr string
	if distributed {
		config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(root))
		_, _, _, closeData := setup.DataNodeWithAddrAndDir(config, memoryFlag)
		g.DeferCleanup(closeData)
		var closeLiaison func()
		addr, _, closeLiaison = setup.LiaisonNodeWithHTTP(config, memoryFlag)
		g.DeferCleanup(closeLiaison)
	} else {
		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())
		var closeServer func()
		addr, _, closeServer = setup.EmptyClosableStandalone(nil, root, ports, memoryFlag)
		g.DeferCleanup(closeServer)
	}
	conn, dialErr := grpclib.NewClient(addr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	gm.Expect(dialErr).NotTo(gm.HaveOccurred())
	g.DeferCleanup(func() { gm.Expect(conn.Close()).To(gm.Succeed()) })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err = databasev1.NewGroupRegistryServiceClient(conn).Create(ctx,
		&databasev1.GroupRegistryServiceCreateRequest{Group: measureGroup(groupAlpha)})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	schema := measureSchema(groupAlpha)
	schema.Fields = append(schema.Fields, &databasev1.FieldSpec{
		Name: "payload", FieldType: databasev1.FieldType_FIELD_TYPE_STRING,
		EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	})
	_, err = databasev1.NewMeasureRegistryServiceClient(conn).Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	_, err = schemav1.NewSchemaBarrierServiceClient(conn).AwaitSchemaApplied(ctx, &schemav1.AwaitSchemaAppliedRequest{
		Keys: []*schemav1.SchemaKey{{Kind: "measure", Group: groupAlpha, Name: fixtureMeasure}}, MinRevisions: []int64{0},
	})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	native := measurev1.NewMeasureServiceClient(conn)
	sql := bydbqlv1.NewBydbQLServiceClient(conn)
	at := time.Now().Truncate(time.Millisecond)
	payload := strings.Repeat("0123456789abcdef", 4096)
	frame := markerFrame(groupAlpha, markerAlpha, at, 1)
	frame.DataPoint.Fields = append(frame.DataPoint.Fields, &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: payload}}})
	seedMeasureFrames(adminActor, native, frame)
	request := markerQuery(at, groupAlpha)
	request.FieldProjection.Names = []string{"payload"}
	text := fmt.Sprintf("SELECT payload::field FROM MEASURE %s IN %s TIME > '-15m' LIMIT 1", fixtureMeasure, groupAlpha)
	gm.Eventually(func(inner gm.Gomega) {
		response, queryErr := native.Query(ctx, request)
		inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		inner.Expect(response.GetDataPoints()).To(gm.HaveLen(1))
		inner.Expect(response.DataPoints[0].GetFields()[0].GetValue().GetStr().GetValue()).To(gm.Equal(payload))
	}, flags.EventuallyTimeout).Should(gm.Succeed())
	response, err := sql.Query(ctx, &bydbqlv1.QueryRequest{Query: text})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	gm.Expect(response.GetMeasureResult().GetDataPoints()).To(gm.HaveLen(1))
	gm.Expect(response.GetMeasureResult().DataPoints[0].GetFields()[0].GetValue().GetStr().GetValue()).To(gm.Equal(payload))
	gm.Expect(proto.Size(response)).To(gm.BeNumerically(">", 32<<10), "compatibility must include a real large result")
	// Concurrent narrow reads must not inherit the wide string projection.
	request.FieldProjection.Names = []string{"value"}
	parallel := 32
	if distributed {
		// Each distributed query additionally holds receive workspace. The finite
		// fallback pool may reject a larger burst for actual byte pressure.
		parallel = 8
	}
	baseline := vectorized.QueryMemoryStats().LiveReservedBytes
	start := make(chan struct{})
	results := make(chan error, parallel)
	for i := 0; i < parallel; i++ {
		go func(ql bool) {
			<-start
			var result *measurev1.QueryResponse
			var queryErr error
			if ql {
				qlResult, qlErr := sql.Query(ctx, &bydbqlv1.QueryRequest{Query: strings.Replace(text, "payload::field", "value::field", 1)})
				queryErr, result = qlErr, qlResult.GetMeasureResult()
			} else {
				result, queryErr = native.Query(ctx, request)
			}
			if queryErr == nil && (len(result.GetDataPoints()) != 1 || result.DataPoints[0].GetFields()[0].GetValue().GetInt().GetValue() != 1) {
				queryErr = fmt.Errorf("incorrect tiny query response: %v", result)
			}
			results <- queryErr
		}(i%2 != 0)
	}
	close(start)
	for i := 0; i < parallel; i++ {
		gm.Expect(<-results).NotTo(gm.HaveOccurred())
	}
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout).Should(gm.Equal(baseline))
}, g.Entry("standalone8MiB", false), g.Entry("distributed8MiB", true))
