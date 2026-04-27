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

package schema

import (
	"context"
	"fmt"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// sendStreamQueryWithRange issues a unary stream Query with an explicit time range.
// It returns the QueryResponse or an error; callers inspect Elements / GroupStatuses.
// The query carries GroupModRevisions so the schema-aware clamp activates — without
// it the liaison treats the request as a legacy query and skips clamping.
func sendStreamQueryWithRange(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName, streamName string,
	begin, end time.Time,
	modRevision int64,
) (*streamv1.QueryResponse, error) {
	return client.Query(ctx, &streamv1.QueryRequest{
		Groups: []string{groupName},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
			End:   timestamppb.New(end.Truncate(time.Millisecond)),
		},
		Limit:             10,
		GroupModRevisions: map[string]int64{groupName: modRevision},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"svc"}},
			},
		},
	})
}

// Schema time-range clamp smoke tests — Rule 7 / §4.6.
// §4.6.4 writes real data and verifies no pre-creation leakage.
// Each spec exercises a distinct outcome of clamping the query TimeRange.Begin to
// the maximum schema.CreatedAt across all queried groups.
var _ = g.Describe("Schema time-range clamp", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §4.6.1: when both Begin and End fall before the schema's CreatedAt, the server
	// clamps Begin forward to CreatedAt; since clamped Begin > End the response is
	// immediately empty (no data can exist in the resulting range).
	g.It("returns empty result when query End is before schema CreatedAt (§4.6.1)", func() {
		groupName := fmt.Sprintf("clamp-past-%d", time.Now().UnixNano())
		streamName := "clamp_stream"

		g.By("Creating stream group and schema")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createStreamResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		streamRev := createStreamResp.GetModRevision()

		g.By("Waiting for schema to be applied in cache (so CreatedAt is populated)")
		awaitErr := clients.AwaitApplied(ctx, []string{fmt.Sprintf("stream:%s/%s", groupName, streamName)}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Querying with a range that ends far in the past (before schema creation)")
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		resp, queryErr := sendStreamQueryWithRange(ctx, clients.StreamWriteClient, groupName, streamName,
			epoch, epoch.Add(time.Millisecond), streamRev)
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(resp.GetElements()).Should(gm.BeEmpty(),
			"clamped Begin (≈ CreatedAt) > End (epoch+1ms) must produce an empty response")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.6.2: when Begin is before schema.CreatedAt but End is in the future, the
	// server clamps Begin forward to CreatedAt and the query executes successfully.
	// Since no data was written the response has zero elements but no error.
	g.It("succeeds and returns zero elements when query spans schema CreatedAt (§4.6.2)", func() {
		// TODO(phase-2): Phase 1 AwaitRevisionApplied / AwaitSchemaApplied are liaison-only
		// by design. Unlike §4.6.1 and §4.6.3 (both ends far in the past, where the clamp
		// short-circuits at the liaison and never dispatches to data nodes), this spec uses
		// End=now+1h so the clamped range is non-empty and the query is dispatched. In
		// distributed mode the data node can lag the liaison's schema view at that moment,
		// causing the dispatched query to fail with "group not found". Cluster-wide barrier
		// semantics ship in Phase 2 via NodeSchemaStatusService + liaison fan-out (plan
		// Steps 2.1–2.2); re-enable this spec in distributed mode once those land.
		if SharedContext.Mode == helpers.ModeDistributed {
			g.Skip("§4.6.2 requires cluster-wide propagation barrier (Phase 2)")
		}
		groupName := fmt.Sprintf("clamp-span-%d", time.Now().UnixNano())
		streamName := "clamp_stream"

		g.By("Creating stream group and schema")
		groupResp, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createStreamResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		streamRev := createStreamResp.GetModRevision()

		g.By("Waiting for group and stream schema to be applied in cache")
		gm.Expect(clients.AwaitRevision(ctx, groupResp.GetModRevision(), 10*time.Second)).ShouldNot(gm.HaveOccurred())
		awaitErr := clients.AwaitApplied(ctx, []string{fmt.Sprintf("stream:%s/%s", groupName, streamName)}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Querying with Begin in the far past and End in the near future")
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		now := time.Now()
		resp, queryErr := sendStreamQueryWithRange(ctx, clients.StreamWriteClient, groupName, streamName,
			epoch, now.Add(time.Hour), streamRev)
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred(),
			"query spanning schema CreatedAt must succeed after Begin is clamped")
		// No data was written; expect zero elements (not an error).
		gm.Expect(resp.GetElements()).Should(gm.BeEmpty(),
			"no data was written so zero elements are expected")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.6.3: when multiple groups are queried and each has a different CreatedAt,
	// Begin is clamped to the maximum CreatedAt across all groups.
	// An End in the far past produces an empty response.
	g.It("clamps to the maximum CreatedAt across multiple groups (§4.6.3)", func() {
		group1 := fmt.Sprintf("clamp-multi1-%d", time.Now().UnixNano())
		group2 := fmt.Sprintf("clamp-multi2-%d", time.Now().UnixNano())
		streamName := "clamp_stream"

		g.By("Creating two stream groups and schemas back-to-back")
		streamRevs := make(map[string]int64, 2)
		for _, groupName := range []string{group1, group2} {
			_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: wgStreamGroup(groupName),
			})
			gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
			createStreamResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: wgStreamSpec(groupName, streamName),
			})
			gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
			streamRevs[groupName] = createStreamResp.GetModRevision()
		}

		g.By("Waiting for both schemas to be applied")
		awaitErr := clients.AwaitApplied(ctx, []string{
			fmt.Sprintf("stream:%s/%s", group1, streamName),
			fmt.Sprintf("stream:%s/%s", group2, streamName),
		}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Querying both groups with a range that ends far in the past")
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		resp, queryErr := clients.StreamWriteClient.Query(ctx, &streamv1.QueryRequest{
			Groups: []string{group1, group2},
			Name:   streamName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(epoch),
				End:   timestamppb.New(epoch.Add(time.Millisecond)),
			},
			Limit:             10,
			GroupModRevisions: streamRevs,
			Projection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: "default", Tags: []string{"svc"}},
				},
			},
		})
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(resp.GetElements()).Should(gm.BeEmpty(),
			"max CreatedAt across both groups > epoch+1ms so the clamped range must be empty")

		for _, groupName := range []string{group1, group2} {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{
				Group: groupName,
			})
		}
	})

	// §4.6.4: PRIMARY Rule 7 correctness test — falsifies the unclamped case.
	// Writes one datum at T_data1 in group1 (older CreatedAt1), then creates group2
	// with CreatedAt2 > T_data1. Querying both groups with a wide Begin must return
	// ZERO data points: the clamp forwards Begin to max(CreatedAt1, CreatedAt2) =
	// CreatedAt2 > T_data1, so the datum is excluded. Without clamp, T_data1 falls
	// inside [Begin, End] and the datum would leak — proving the clamp is actually
	// applied rather than merely consistent with an already-in-range write.
	g.It("clips TimeRange.Begin to max(CreatedAt) and excludes pre-creation data (§4.6.4)", func() {
		// TODO(phase-2): Phase 1 AwaitRevisionApplied is liaison-only by design. This spec's
		// baseline sanity check (Create → AwaitRevision → Write → Query expecting HaveLen(1))
		// races the data-node tsTable readiness in distributed mode. The actual clamp
		// falsification is sound; only the prerequisite Write→Query round-trip flakes.
		// Cluster-wide barrier semantics ship in Phase 2 via NodeSchemaStatusService +
		// liaison fan-out (plan Steps 2.1–2.2); re-enable this spec in distributed mode
		// once those land.
		if SharedContext.Mode == helpers.ModeDistributed {
			g.Skip("§4.6.4 requires cluster-wide propagation barrier (Phase 2)")
		}
		group1 := fmt.Sprintf("clamp-leak1-%d", time.Now().UnixNano())
		group2 := fmt.Sprintf("clamp-leak2-%d", time.Now().UnixNano())
		measureName := "clamp_measure"

		g.By("Creating older group1 and measure (CreatedAt1)")
		_, createGroup1Err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(group1),
		})
		gm.Expect(createGroup1Err).ShouldNot(gm.HaveOccurred())

		createResp1, createMeasure1Err := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV1(group1, measureName),
		})
		gm.Expect(createMeasure1Err).ShouldNot(gm.HaveOccurred())
		r1 := createResp1.GetModRevision()
		gm.Expect(clients.AwaitRevision(ctx, r1, 10*time.Second)).ShouldNot(gm.HaveOccurred())

		g.By("Writing one data point at T_data1 in measure1 (strictly after CreatedAt1, strictly before CreatedAt2)")
		tData1 := timestamp.NowMilli()
		hostTags := []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}},
			}},
		}}
		writeStatus, writeErr := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, group1, measureName, tData1, r1, hostTags)
		gm.Expect(writeErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"write at T_data1 must return STATUS_SUCCEED")

		g.By("Sanity-checking that querying group1 alone returns the datum (legacy unclamped baseline — pass modRevision=0)")
		baselineResp, baselineErr := queryMeasureRange(ctx, clients.MeasureWriteClient, group1, measureName,
			tData1.Add(-time.Hour), time.Now().Add(time.Hour), 0)
		gm.Expect(baselineErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(baselineResp.GetDataPoints()).Should(gm.HaveLen(1),
			"baseline single-group query must return the written datum — otherwise §4.6.4 is not falsifying anything")

		g.By("Creating newer group2 and measure (CreatedAt2 > T_data1)")
		_, createGroup2Err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(group2),
		})
		gm.Expect(createGroup2Err).ShouldNot(gm.HaveOccurred())

		createResp2, createMeasure2Err := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV1(group2, measureName),
		})
		gm.Expect(createMeasure2Err).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()
		gm.Expect(clients.AwaitRevision(ctx, r2, 10*time.Second)).ShouldNot(gm.HaveOccurred())

		g.By("Verifying CreatedAt2 > T_data1 so the clamp falsification is meaningful")
		getResp2, getMeasure2Err := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: group2},
		})
		gm.Expect(getMeasure2Err).ShouldNot(gm.HaveOccurred())
		createdAt2 := getResp2.GetMeasure().GetCreatedAt()
		gm.Expect(createdAt2).ShouldNot(gm.BeNil())
		gm.Expect(createdAt2.AsTime().After(tData1)).Should(gm.BeTrue(),
			"CreatedAt2 must be strictly after T_data1 for the falsification to be meaningful")

		g.By("Querying group1+group2 with Begin far before T_data1 — clamp forwards Begin to CreatedAt2 > T_data1, excluding the datum")
		queryResp, queryErr := clients.MeasureWriteClient.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{group1, group2},
			Name:   measureName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(tData1.Add(-time.Hour).Truncate(time.Millisecond)),
				End:   timestamppb.New(time.Now().Add(time.Hour).Truncate(time.Millisecond)),
			},
			GroupModRevisions: map[string]int64{group1: r1, group2: r2},
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: "default", Tags: []string{"host"}},
				},
			},
			FieldProjection: &measurev1.QueryRequest_FieldProjection{
				Names: []string{"value"},
			},
			Limit: 100,
		})
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp.GetDataPoints()).Should(gm.BeEmpty(),
			"Rule 7 clamp invariant: T_data1 < CreatedAt2 must be excluded by the clamp; without clamp the datum would leak")

		for _, groupName := range []string{group1, group2} {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
		}
	})
})
