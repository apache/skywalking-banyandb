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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// arMeasureGroup creates a CATALOG_MEASURE group suitable for apply-rollback tests.
func arMeasureGroup(name string) *commonv1.Group {
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

// arMeasureSpec returns a minimal Measure with entity=[host] for apply-rollback tests.
func arMeasureSpec(group, name string) *databasev1.Measure {
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

// applyRollbackMeasureName is the fixture measure name reused across the §6.1–§6.7 specs.
const applyRollbackMeasureName = "cpu_total"

// Schema apply-rollback tests — §6.1 through §6.7.
var _ = g.Describe("Schema apply rollback", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §6.1 — creates a measure visible end-to-end.
	g.It("creates a measure visible end-to-end (§6.1)", func() {
		groupName := fmt.Sprintf("ar-vm-%d", time.Now().UnixNano())
		measureName := applyRollbackMeasureName

		g.By("Creating measure group and awaiting revision")
		createGroupResp, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		groupRev := createGroupResp.GetModRevision()
		gm.Expect(groupRev).Should(gm.BeNumerically(">", int64(0)))

		awaitGroupErr := clients.AwaitRevision(ctx, groupRev, 10*time.Second)
		gm.Expect(awaitGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure and awaiting revision")
		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		r := createMeasureResp.GetModRevision()
		gm.Expect(r).Should(gm.BeNumerically(">", int64(0)))

		awaitErr := clients.AwaitRevision(ctx, r, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Verifying GetMeasure returns correct metadata")
		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		got := getResp.GetMeasure()
		gm.Expect(got.GetMetadata().GetModRevision()).Should(gm.Equal(r),
			"GetMeasure.ModRevision must equal the create response ModRevision")
		gm.Expect(got.GetCreatedAt()).ShouldNot(gm.BeNil(), "CreatedAt must be set")
		gm.Expect(got.GetUpdatedAt()).ShouldNot(gm.BeNil(), "UpdatedAt must be set")
		gm.Expect(got.GetCreatedAt().AsTime().Equal(got.GetUpdatedAt().AsTime())).Should(gm.BeTrue(),
			"CreatedAt must equal UpdatedAt on first create")

		g.By("Verifying ListMeasure returns exactly one user-created entry")
		listResp, listErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listErr).ShouldNot(gm.HaveOccurred())
		userOnly := userMeasures(listResp.GetMeasure())
		gm.Expect(userOnly).Should(gm.HaveLen(1), "ListMeasure must return exactly one user-created entry")
		gm.Expect(userOnly[0].GetMetadata().GetName()).Should(gm.Equal(measureName))

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.2 — Apply-rollback: a rejected schema update must leave the full schema state intact,
	// including mod_revision, created_at, updated_at, and the entity definition.
	g.It("preserves mod_revision, created_at, updated_at, and entity after a rejected entity-change update (§6.2)", func() {
		groupName := fmt.Sprintf("ar-measure-%d", time.Now().UnixNano())
		measureName := "ar_measure"

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        2,
					SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
			},
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure with entity=[svc]")
		_, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
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
			},
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())

		g.By("Getting measure to capture initial state")
		initialResp, getInitialErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getInitialErr).ShouldNot(gm.HaveOccurred())
		initial := initialResp.GetMeasure()
		gm.Expect(initial.GetCreatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(initial.GetUpdatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(initial.GetMetadata().GetModRevision()).Should(gm.BeNumerically(">", int64(0)))
		initialModRev := initial.GetMetadata().GetModRevision()
		initialCreatedAt := initial.GetCreatedAt().AsTime().UnixNano()
		initialUpdatedAt := initial.GetUpdatedAt().AsTime().UnixNano()

		g.By("Attempting UpdateMeasure with entity=[svc, host] (entity change that must be rejected)")
		changedMeasure := proto.Clone(initial).(*databasev1.Measure)
		changedMeasure.Entity = &databasev1.Entity{TagNames: []string{"svc", "host"}}
		_, updateErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: changedMeasure,
		})
		gm.Expect(updateErr).Should(gm.HaveOccurred(), "UpdateMeasure with entity change must return an error")

		g.By("Verifying mod_revision, created_at, updated_at, and entity are all unchanged")
		afterResp, getAfterErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		after := afterResp.GetMeasure()

		gm.Expect(after.GetMetadata().GetModRevision()).Should(
			gm.Equal(initialModRev),
			"mod_revision must be unchanged after a rejected update",
		)
		gm.Expect(after.GetCreatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(after.GetCreatedAt().AsTime().UnixNano()).Should(
			gm.Equal(initialCreatedAt),
			"created_at must be unchanged after a rejected update",
		)
		gm.Expect(after.GetUpdatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(after.GetUpdatedAt().AsTime().UnixNano()).Should(
			gm.Equal(initialUpdatedAt),
			"updated_at must be unchanged after a rejected update",
		)
		gm.Expect(after.GetEntity().GetTagNames()).Should(
			gm.Equal([]string{"svc"}),
			"entity must be unchanged after a rejected update",
		)

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.3 — lists groups and measures after create.
	g.It("lists groups and measures after create (§6.3)", func() {
		groupName := fmt.Sprintf("ar-list-%d", time.Now().UnixNano())
		measureName := applyRollbackMeasureName

		g.By("Creating measure group and measure")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		r := createMeasureResp.GetModRevision()

		g.By("Awaiting revision")
		awaitErr := clients.AwaitRevision(ctx, r, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("ListGroup contains the group")
		listGroupResp, listGroupErr := clients.GroupClient.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
		gm.Expect(listGroupErr).ShouldNot(gm.HaveOccurred())
		foundGroup := false
		for _, grp := range listGroupResp.GetGroup() {
			if grp.GetMetadata().GetName() == groupName {
				foundGroup = true
				break
			}
		}
		gm.Expect(foundGroup).Should(gm.BeTrue(), "ListGroup must contain the created group")

		g.By("ListMeasure returns exactly one user-created entry")
		listMeasureResp, listMeasureErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listMeasureErr).ShouldNot(gm.HaveOccurred())
		userMeasureOnly := userMeasures(listMeasureResp.GetMeasure())
		gm.Expect(userMeasureOnly).Should(gm.HaveLen(1))
		gm.Expect(userMeasureOnly[0].GetMetadata().GetName()).Should(gm.Equal(measureName))

		g.By("ListStream, ListTrace, ListIndexRule, ListIndexRuleBinding, ListTopNAggregation all empty")
		listStreamResp, listStreamErr := clients.StreamRegClient.List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: groupName})
		gm.Expect(listStreamErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listStreamResp.GetStream()).Should(gm.BeEmpty(), "ListStream must be empty")

		listTraceResp, listTraceErr := clients.TraceRegClient.List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: groupName})
		gm.Expect(listTraceErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listTraceResp.GetTrace()).Should(gm.BeEmpty(), "ListTrace must be empty")

		listIdxRuleResp, listIdxRuleErr := clients.IndexRuleClient.List(ctx, &databasev1.IndexRuleRegistryServiceListRequest{Group: groupName})
		gm.Expect(listIdxRuleErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listIdxRuleResp.GetIndexRule()).Should(gm.BeEmpty(), "ListIndexRule must be empty")

		listIdxBindResp, listIdxBindErr := clients.IndexRuleBindingClient.List(ctx, &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: groupName})
		gm.Expect(listIdxBindErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listIdxBindResp.GetIndexRuleBinding()).Should(gm.BeEmpty(), "ListIndexRuleBinding must be empty")

		listTopNResp, listTopNErr := clients.TopNAggregationRegClient.List(ctx, &databasev1.TopNAggregationRegistryServiceListRequest{Group: groupName})
		gm.Expect(listTopNErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listTopNResp.GetTopNAggregation()).Should(gm.BeEmpty(), "ListTopNAggregation must be empty")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.4 — adds a second measure and keeps the first.
	g.It("adds a second measure and keeps the first (§6.4)", func() {
		groupName := fmt.Sprintf("ar-two-%d", time.Now().UnixNano())

		g.By("Creating measure group")
		createGroupResp, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		groupRev := createGroupResp.GetModRevision()
		awaitGroupErr := clients.AwaitRevision(ctx, groupRev, 10*time.Second)
		gm.Expect(awaitGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating first measure cpu_total → R1")
		createResp1, createErr1 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, applyRollbackMeasureName),
		})
		gm.Expect(createErr1).ShouldNot(gm.HaveOccurred())
		r1 := createResp1.GetModRevision()
		gm.Expect(r1).Should(gm.BeNumerically(">", int64(0)))

		awaitErr1 := clients.AwaitRevision(ctx, r1, 10*time.Second)
		gm.Expect(awaitErr1).ShouldNot(gm.HaveOccurred())

		g.By("Creating second measure mem_total → R2 > R1")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, "mem_total"),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()
		gm.Expect(r2).Should(gm.BeNumerically(">", r1), "R2 must be > R1")

		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		g.By("Verifying GetMeasure for cpu_total returns R1")
		getCPUResp, getCPUErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: applyRollbackMeasureName, Group: groupName},
		})
		gm.Expect(getCPUErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(getCPUResp.GetMeasure().GetMetadata().GetModRevision()).Should(gm.Equal(r1),
			"cpu_total ModRevision must equal R1")

		g.By("Verifying GetMeasure for mem_total returns R2")
		getMemResp, getMemErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: "mem_total", Group: groupName},
		})
		gm.Expect(getMemErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(getMemResp.GetMeasure().GetMetadata().GetModRevision()).Should(gm.Equal(r2),
			"mem_total ModRevision must equal R2")

		g.By("ListMeasure returns both user-created measures")
		listResp, listErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listErr).ShouldNot(gm.HaveOccurred())
		userOnly := userMeasures(listResp.GetMeasure())
		gm.Expect(userOnly).Should(gm.HaveLen(2), "ListMeasure must return both user-created measures")
		names := make([]string, 0, 2)
		for _, m := range userOnly {
			names = append(names, m.GetMetadata().GetName())
		}
		gm.Expect(names).Should(gm.ConsistOf(applyRollbackMeasureName, "mem_total"))

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.5 — drops measure from BanyanDB on delete.
	g.It("drops measure from BanyanDB on delete (§6.5)", func() {
		groupName := fmt.Sprintf("ar-del-%d", time.Now().UnixNano())
		measureName := applyRollbackMeasureName

		g.By("Creating measure group and measure")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		r := createMeasureResp.GetModRevision()

		awaitErr := clients.AwaitRevision(ctx, r, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Deleting measure and verifying delete response")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(deleteResp.GetDeleted()).Should(gm.BeTrue(), "DeleteMeasure must return deleted=true")
		gm.Expect(deleteResp.GetDeleteTime()).Should(gm.BeNumerically(">", int64(0)), "delete_time must be > 0")

		g.By("Awaiting deletion propagation")
		awaitDeletedErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDeletedErr).ShouldNot(gm.HaveOccurred())

		g.By("Verifying GetMeasure returns NotFound")
		_, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr).Should(gm.HaveOccurred())
		st, _ := status.FromError(getErr)
		gm.Expect(st.Code()).Should(gm.Equal(codes.NotFound), "GetMeasure must return NotFound after deletion")

		g.By("Verifying ListMeasure has no user-created entries")
		listResp, listErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(userMeasures(listResp.GetMeasure())).Should(gm.BeEmpty(),
			"ListMeasure must contain no user-created entries after deletion")

		g.By("Verifying write to deleted measure fails or returns non-SUCCEED status")
		writeStatus, writeErr := sendSingleMeasureWrite(ctx, clients.MeasureWriteClient, groupName, measureName, 0)
		// The measure entity is no longer in the routing cache; write must fail.
		if writeErr == nil {
			gm.Expect(writeStatus).ShouldNot(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
				"write to deleted measure must not return STATUS_SUCCEED")
		}

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.6 — delete-then-recreate is idempotent for identical shape.
	g.It("delete-then-recreate is idempotent for identical shape (§6.6)", func() {
		groupName := fmt.Sprintf("ar-recreate-%d", time.Now().UnixNano())
		measureName := applyRollbackMeasureName

		g.By("Creating measure group and measure → R1")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createResp1, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		r1 := createResp1.GetModRevision()

		awaitErr1 := clients.AwaitRevision(ctx, r1, 10*time.Second)
		gm.Expect(awaitErr1).ShouldNot(gm.HaveOccurred())

		g.By("Deleting measure → T_del; awaiting deletion")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		tDel := deleteResp.GetDeleteTime()
		gm.Expect(tDel).Should(gm.BeNumerically(">", int64(0)))

		awaitDelErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDelErr).ShouldNot(gm.HaveOccurred())

		g.By("Recreating the same measure spec → R2")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()

		g.By("Awaiting R2")
		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		g.By("Assertions: ModRevision, R2 > R1, CreatedAt2 > T_del, shape matches, ListMeasure length 1")
		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		got := getResp.GetMeasure()

		gm.Expect(got.GetMetadata().GetModRevision()).Should(gm.Equal(r2), "ModRevision must equal R2")
		gm.Expect(r2).Should(gm.BeNumerically(">", r1), "R2 must be > R1")
		gm.Expect(got.GetCreatedAt()).ShouldNot(gm.BeNil())
		gm.Expect(got.GetCreatedAt().AsTime().UnixNano()).Should(
			gm.BeNumerically(">", tDel),
			"CreatedAt2 must be after T_del (tombstone invariant Step 1.3)",
		)
		gm.Expect(got.GetEntity().GetTagNames()).Should(gm.Equal([]string{"host"}), "entity must match original shape")

		listResp, listErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(userMeasures(listResp.GetMeasure())).Should(gm.HaveLen(1),
			"ListMeasure must return exactly one user-created entry after recreate")

		g.By("Write with stale R1 must return STATUS_EXPIRED_SCHEMA")
		writeStatus, writeErr := sendSingleMeasureWrite(ctx, clients.MeasureWriteClient, groupName, measureName, r1)
		gm.Expect(writeErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA.String()),
			"write with stale ModRevision R1 must return STATUS_EXPIRED_SCHEMA after recreate")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.7 — produces no measures when nothing is created.
	g.It("produces no measures when nothing is created (§6.7)", func() {
		groupName := fmt.Sprintf("ar-empty-%d", time.Now().UnixNano())

		g.By("Creating group only — no measures")
		createGroupResp, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: arMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		groupRev := createGroupResp.GetModRevision()
		awaitGroupErr := clients.AwaitRevision(ctx, groupRev, 10*time.Second)
		gm.Expect(awaitGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("All list RPCs must return no user-created entries")
		listMeasureResp, listMeasureErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
		gm.Expect(listMeasureErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(userMeasures(listMeasureResp.GetMeasure())).Should(gm.BeEmpty(),
			"ListMeasure must contain no user-created entries")

		listStreamResp, listStreamErr := clients.StreamRegClient.List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: groupName})
		gm.Expect(listStreamErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listStreamResp.GetStream()).Should(gm.BeEmpty(), "ListStream must be empty")

		listTraceResp, listTraceErr := clients.TraceRegClient.List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: groupName})
		gm.Expect(listTraceErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listTraceResp.GetTrace()).Should(gm.BeEmpty(), "ListTrace must be empty")

		listIdxRuleResp, listIdxRuleErr := clients.IndexRuleClient.List(ctx, &databasev1.IndexRuleRegistryServiceListRequest{Group: groupName})
		gm.Expect(listIdxRuleErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listIdxRuleResp.GetIndexRule()).Should(gm.BeEmpty(), "ListIndexRule must be empty")

		listIdxBindResp, listIdxBindErr := clients.IndexRuleBindingClient.List(ctx, &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: groupName})
		gm.Expect(listIdxBindErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listIdxBindResp.GetIndexRuleBinding()).Should(gm.BeEmpty(), "ListIndexRuleBinding must be empty")

		listTopNResp, listTopNErr := clients.TopNAggregationRegClient.List(ctx, &databasev1.TopNAggregationRegistryServiceListRequest{Group: groupName})
		gm.Expect(listTopNErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(listTopNResp.GetTopNAggregation()).Should(gm.BeEmpty(), "ListTopNAggregation must be empty")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
