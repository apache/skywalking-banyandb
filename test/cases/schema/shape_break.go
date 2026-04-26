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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// shapeBreakMeasureGroup creates a CATALOG_MEASURE group with the given name.
func shapeBreakMeasureGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

// shapeBreakMeasureSpec returns a Measure with entity=[svc] and both svc/host tags defined,
// so that an update changing entity to [svc, host] passes validate.Measure but fails
// validateMeasureUpdate (entity changed).
func shapeBreakMeasureSpec(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: name, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Fields: []*databasev1.FieldSpec{{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
}

// throughputMeasureSpecV1 returns a Measure for the throughput tests with entity=[host].
func throughputMeasureSpecV1(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: name, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"host"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Fields: []*databasev1.FieldSpec{{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
}

// throughputMeasureSpecV2 returns a Measure with entity=[host, instance] (shape-break spec).
func throughputMeasureSpecV2(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: name, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"host", "instance"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "instance", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Fields: []*databasev1.FieldSpec{{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
}

// queryMeasureRange issues a unary measure Query over the given time range.
// modRevision opts the request into the schema-aware path: the liaison clamps
// TimeRange.Begin to schema.CreatedAt only when GroupModRevisions is non-empty.
// Pass 0 to keep the query in the legacy unclamped mode.
func queryMeasureRange(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName, measureName string,
	begin, end time.Time,
	modRevision int64,
) (*measurev1.QueryResponse, error) {
	req := &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
			End:   timestamppb.New(end.Truncate(time.Millisecond)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"host"}},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"value"},
		},
		Limit: 100,
	}
	if modRevision > 0 {
		req.GroupModRevisions = map[string]int64{groupName: modRevision}
	}
	return client.Query(ctx, req)
}

// §6.8 / §6.9 / §6.10 / §6.11 — Shape-break and delete-then-recreate scenarios.
var _ = g.Describe("Schema shape-break rejection", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §6.8: shape-break — delete+apply new shape creates the new measure (Rule 7 clamp end-to-end).
	g.It("shape-break: delete+apply new shape creates the new measure (§6.8)", func() {
		// TODO(phase-2): Phase 1 AwaitRevisionApplied is liaison-only by design. Both this spec
		// and §6.11 perform an end-to-end data Write+Query round-trip through the liaison after
		// a schema mutation; in distributed mode the data node can lag the liaison briefly on
		// tsTable readiness or query-side index refresh, racing the immediate Write→Query.
		// Cluster-wide barrier semantics ship in Phase 2 via NodeSchemaStatusService + liaison
		// fan-out (plan Steps 2.1–2.2); re-enable this spec in distributed mode once those land.
		if SharedContext.Mode == "distributed" {
			g.Skip("§6.8 requires cluster-wide propagation barrier (Phase 2)")
		}
		groupName := fmt.Sprintf("sb-new-%d", time.Now().UnixNano())
		measureName := "throughput"

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure throughput with entity=[host] → R1")
		createResp1, createErr1 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV1(groupName, measureName),
		})
		gm.Expect(createErr1).ShouldNot(gm.HaveOccurred())
		r1 := createResp1.GetModRevision()

		awaitErr1 := clients.AwaitRevision(ctx, r1, 10*time.Second)
		gm.Expect(awaitErr1).ShouldNot(gm.HaveOccurred())

		g.By("Capturing CreatedAt1 from GetMeasure")
		getResp1, getErr1 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr1).ShouldNot(gm.HaveOccurred())
		createdAt1 := getResp1.GetMeasure().GetCreatedAt()
		gm.Expect(createdAt1).ShouldNot(gm.BeNil())

		g.By("Writing one data point at T_data1 = CreatedAt1 + 50ms")
		// T_data1 uses wall-clock now rather than a future offset from CreatedAt1 so
		// a subsequent recreate stamps CreatedAt2 strictly after this timestamp.
		tData1 := timestamp.NowMilli()
		hostTags1 := []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}},
			}},
		}}
		writeStatus1, writeErr1 := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, groupName, measureName, tData1, r1, hostTags1)
		gm.Expect(writeErr1).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus1).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"initial write must return STATUS_SUCCEED")

		g.By("Querying [CreatedAt1, now+1h] — must return exactly 1 data point")
		queryResp1, queryErr1 := queryMeasureRange(ctx, clients.MeasureWriteClient, groupName, measureName,
			createdAt1.AsTime(), time.Now().Add(time.Hour), r1)
		gm.Expect(queryErr1).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp1.GetDataPoints()).Should(gm.HaveLen(1), "sanity baseline: pre-delete query must return 1 data point")

		g.By("Deleting measure → T_del; awaiting deletion")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		tDel := deleteResp.GetDeleteTime()

		awaitDelErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDelErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure throughput with entity=[host, instance] → R2")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV2(groupName, measureName),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()

		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		g.By("Assertions: new shape at R2, CreatedAt2 after T_del, old data clamped, stale write rejected, new write succeeds")
		getResp2, getErr2 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr2).ShouldNot(gm.HaveOccurred())
		got2 := getResp2.GetMeasure()

		gm.Expect(got2.GetEntity().GetTagNames()).Should(gm.Equal([]string{"host", "instance"}),
			"new shape must have entity=[host, instance]")
		gm.Expect(got2.GetMetadata().GetModRevision()).Should(gm.Equal(r2), "ModRevision must equal R2")
		gm.Expect(got2.GetCreatedAt()).ShouldNot(gm.BeNil())
		createdAt2 := got2.GetCreatedAt()
		gm.Expect(createdAt2.AsTime().After(createdAt1.AsTime())).Should(gm.BeTrue(),
			"CreatedAt2 must be after CreatedAt1")
		gm.Expect(createdAt2.AsTime().UnixNano()).Should(gm.BeNumerically(">", tDel),
			"CreatedAt2 must be after T_del")

		// Rule 7 clamp: query [T_data1, now+1h] must return empty because T_data1 < CreatedAt2.
		queryResp2, queryErr2 := queryMeasureRange(ctx, clients.MeasureWriteClient, groupName, measureName,
			tData1, time.Now().Add(time.Hour), r2)
		gm.Expect(queryErr2).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp2.GetDataPoints()).Should(gm.BeEmpty(),
			"Rule 7 clamp must hide pre-CreatedAt2 data points")

		// Stale write with R1 must be rejected.
		staleHostTags := []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}},
			}},
		}}
		writeStatusStale, writeErrStale := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, groupName, measureName,
			time.Now(), r1, staleHostTags)
		gm.Expect(writeErrStale).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatusStale).Should(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA.String()),
			"write with stale R1 must return STATUS_EXPIRED_SCHEMA")

		// Write with R2 and new tag layout must succeed.
		newTags := []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "inst1"}}},
			},
		}}
		writeStatusNew, writeErrNew := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, groupName, measureName,
			time.Now(), r2, newTags)
		gm.Expect(writeErrNew).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatusNew).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"write with R2 and new tag layout must return STATUS_SUCCEED")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.9: entity-change update is rejected; the error mentions "entity"; ModRevision is unchanged.
	g.It("rejects a measure entity-change update and leaves ModRevision unchanged (§6.9)", func() {
		groupName := fmt.Sprintf("sb1-measure-%d", time.Now().UnixNano())
		measureName := "sb1_measure"

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure with entity=[svc]")
		_, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: shapeBreakMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())

		g.By("Getting measure to capture baseline ModRevision")
		baseResp, getBaseErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getBaseErr).ShouldNot(gm.HaveOccurred())
		baseModRev := baseResp.GetMeasure().GetMetadata().GetModRevision()
		gm.Expect(baseModRev).Should(gm.BeNumerically(">", int64(0)))

		g.By("Attempting UpdateMeasure with entity=[svc, host] (entity change)")
		changedMeasure := baseResp.GetMeasure()
		changedMeasure.Entity = &databasev1.Entity{TagNames: []string{"svc", "host"}}
		_, updateErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: changedMeasure,
		})
		gm.Expect(updateErr).Should(gm.HaveOccurred(), "UpdateMeasure with entity change must return an error")
		gm.Expect(updateErr.Error()).Should(gm.ContainSubstring("entity"),
			"the error message must reference 'entity'")

		g.By("Verifying ModRevision is unchanged after rejected update")
		afterResp, getAfterErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(afterResp.GetMeasure().GetMetadata().GetModRevision()).Should(
			gm.Equal(baseModRev),
			"ModRevision must be unchanged after a rejected entity-change update",
		)

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.10: full measure content (entity, tag families, fields, created_at, updated_at, mod_revision)
	// is unchanged after a rejected entity-change update.
	g.It("leaves the full measure schema unchanged after a rejected entity-change update (§6.10)", func() {
		groupName := fmt.Sprintf("sb2-measure-%d", time.Now().UnixNano())
		measureName := "sb2_measure"

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure with entity=[svc]")
		_, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: shapeBreakMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())

		g.By("Getting measure to capture baseline state")
		beforeResp, getBeforeErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getBeforeErr).ShouldNot(gm.HaveOccurred())
		before := beforeResp.GetMeasure()

		g.By("Attempting UpdateMeasure with entity=[svc, host] (entity change)")
		changedMeasure := proto.Clone(before).(*databasev1.Measure)
		changedMeasure.Entity = &databasev1.Entity{TagNames: []string{"svc", "host"}}
		_, updateErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: changedMeasure,
		})
		gm.Expect(updateErr).Should(gm.HaveOccurred(), "UpdateMeasure with entity change must return an error")

		g.By("Verifying the full measure schema is unchanged after rejected update")
		afterResp, getAfterErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		after := afterResp.GetMeasure()

		gm.Expect(after.GetMetadata().GetModRevision()).Should(
			gm.Equal(before.GetMetadata().GetModRevision()),
			"ModRevision must be unchanged",
		)
		gm.Expect(after.GetEntity().GetTagNames()).Should(
			gm.Equal(before.GetEntity().GetTagNames()),
			"Entity must be unchanged",
		)
		gm.Expect(after.GetTagFamilies()).Should(
			gm.HaveLen(len(before.GetTagFamilies())),
			"TagFamilies count must be unchanged",
		)
		gm.Expect(after.GetFields()).Should(
			gm.HaveLen(len(before.GetFields())),
			"Fields count must be unchanged",
		)
		gm.Expect(after.GetCreatedAt()).ShouldNot(gm.BeNil())
		if before.GetCreatedAt() != nil {
			gm.Expect(after.GetCreatedAt().AsTime().UnixNano()).Should(
				gm.Equal(before.GetCreatedAt().AsTime().UnixNano()),
				"created_at must be unchanged after rejected update",
			)
		}
		gm.Expect(after.GetUpdatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(after.GetUpdatedAt().AsTime().UnixNano()).Should(
			gm.Equal(before.GetUpdatedAt().AsTime().UnixNano()),
			"updated_at must be unchanged after rejected update",
		)

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.11: delete-then-recreate original shape drops old data (Rule 7 clamp).
	g.It("delete-then-recreate original shape drops old data (§6.11)", func() {
		// TODO(phase-2): Phase 1 AwaitRevisionApplied is liaison-only by design — it confirms
		// the liaison cache observes R2 but not that every data node has rebuilt the tsTable
		// after the delete-then-recreate. The post-recreate Write+Query round-trip exercised
		// by this spec races the data-node tsTable rebuild on slow CI runners. Cluster-wide
		// barrier semantics ship in Phase 2 via NodeSchemaStatusService + liaison fan-out
		// (plan Steps 2.1–2.2); re-enable this spec in distributed mode once those land.
		if SharedContext.Mode == "distributed" {
			g.Skip("§6.11 requires cluster-wide propagation barrier (Phase 2)")
		}
		groupName := fmt.Sprintf("sb-same-%d", time.Now().UnixNano())
		measureName := "throughput"

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: shapeBreakMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure throughput entity=[host] → R1")
		createResp1, createErr1 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV1(groupName, measureName),
		})
		gm.Expect(createErr1).ShouldNot(gm.HaveOccurred())
		r1 := createResp1.GetModRevision()

		awaitErr1 := clients.AwaitRevision(ctx, r1, 10*time.Second)
		gm.Expect(awaitErr1).ShouldNot(gm.HaveOccurred())

		getResp1, getErr1 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr1).ShouldNot(gm.HaveOccurred())
		createdAt1 := getResp1.GetMeasure().GetCreatedAt()
		gm.Expect(createdAt1).ShouldNot(gm.BeNil())

		g.By("Writing data at T_data1 = CreatedAt1 + 50ms; verifying query returns that point")
		// T_data1 uses wall-clock now rather than a future offset from CreatedAt1 so
		// a subsequent recreate stamps CreatedAt2 strictly after this timestamp.
		tData1 := timestamp.NowMilli()
		hostTags := []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}},
			}},
		}}
		writeStatus1, writeErr1 := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, groupName, measureName, tData1, r1, hostTags)
		gm.Expect(writeErr1).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus1).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		queryResp1, queryErr1 := queryMeasureRange(ctx, clients.MeasureWriteClient, groupName, measureName,
			createdAt1.AsTime(), time.Now().Add(time.Hour), r1)
		gm.Expect(queryErr1).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp1.GetDataPoints()).Should(gm.HaveLen(1), "baseline: pre-delete query returns 1 data point")

		g.By("Deleting measure → T_del; awaiting deletion")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		tDel := deleteResp.GetDeleteTime()

		awaitDelErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDelErr).ShouldNot(gm.HaveOccurred())

		g.By("Recreating same spec entity=[host] → R2; CreatedAt2 > T_del")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: throughputMeasureSpecV1(groupName, measureName),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()

		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		getResp2, getErr2 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr2).ShouldNot(gm.HaveOccurred())
		got2 := getResp2.GetMeasure()
		createdAt2 := got2.GetCreatedAt()
		gm.Expect(createdAt2).ShouldNot(gm.BeNil())
		gm.Expect(createdAt2.AsTime().UnixNano()).Should(gm.BeNumerically(">", tDel),
			"CreatedAt2 must be after T_del")

		g.By("Assertions: shape matches original, pre-delete data clamped, R2 write succeeds")
		gm.Expect(got2.GetEntity().GetTagNames()).Should(gm.Equal([]string{"host"}),
			"recreated measure must preserve original entity")

		// Rule 7 clamp: query [T_data1, now+1h] returns empty because T_data1 < CreatedAt2.
		queryResp2, queryErr2 := queryMeasureRange(ctx, clients.MeasureWriteClient, groupName, measureName,
			tData1, time.Now().Add(time.Hour), r2)
		gm.Expect(queryErr2).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp2.GetDataPoints()).Should(gm.BeEmpty(),
			"Rule 7 clamp must hide pre-delete data because T_data1 < CreatedAt2")

		// Write with R2 must succeed.
		tNewWrite := time.Now()
		writeStatus2, writeErr2 := sendMeasureWriteAtTime(ctx, clients.MeasureWriteClient, groupName, measureName,
			tNewWrite, r2, hostTags)
		gm.Expect(writeErr2).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus2).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"write with R2 must succeed")

		// Post-write query [CreatedAt2, now+1h] must return the newly-written point.
		queryResp3, queryErr3 := queryMeasureRange(ctx, clients.MeasureWriteClient, groupName, measureName,
			createdAt2.AsTime(), time.Now().Add(time.Hour), r2)
		gm.Expect(queryErr3).ShouldNot(gm.HaveOccurred())
		gm.Expect(queryResp3.GetDataPoints()).Should(gm.HaveLen(1),
			"post-creation write must be queryable after AwaitRevision(R2)")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
