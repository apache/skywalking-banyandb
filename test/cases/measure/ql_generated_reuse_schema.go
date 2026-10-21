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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package measure_test

import (
	"context"
	"time"

	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func prepareGeneratedReuseFixture(base time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: qlgen.T05FixtureSchema()})
	gm.Expect(createErr).NotTo(gm.HaveOccurred())
	writeGeneratedReuseFixture(base)
}

func writeGeneratedReuseFixture(base time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: qlgen.T05FixtureSchema().GetMetadata()})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	writes := qlgen.T05FixtureWrites(base)
	for index, request := range writes {
		request.Metadata = nil
		if index == 0 {
			request.Metadata = registered.GetMeasure().GetMetadata()
		}
	}
	suite, generateErr := qlgen.Generate()
	gm.Expect(generateErr).NotTo(gm.HaveOccurred())
	readiness := suite.ReuseReadiness
	gm.Eventually(func(inner gm.Gomega) {
		writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer writeCancel()
		inner.Expect(writeGeneratedQLFixture(writeCtx, SharedContext.Connection, writes)).To(gm.Succeed())
		inner.Expect(executeGeneratedReuseCall(writeCtx, base, readiness)).To(gm.Succeed())
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
}

func awaitGeneratedReuseRevision(revision int64) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	response, awaitErr := schemav1.NewSchemaBarrierServiceClient(SharedContext.Connection).AwaitRevisionApplied(ctx,
		&schemav1.AwaitRevisionAppliedRequest{MinRevision: revision, Timeout: durationpb.New(10 * time.Second)})
	gm.Expect(awaitErr).NotTo(gm.HaveOccurred())
	gm.Expect(response.GetApplied()).To(gm.BeTrue())
}

func replaceGeneratedReuseSchema(group string) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{Group: group, Name: qlgen.T05FixtureName},
	})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	schema := registered.GetMeasure()
	schema.TagFamilies[0].Tags = append(schema.TagFamilies[0].Tags,
		&databasev1.TagSpec{Name: "cache_dependency_probe", Type: databasev1.TagType_TAG_TYPE_STRING})
	updated, updateErr := registry.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: schema})
	gm.Expect(updateErr).NotTo(gm.HaveOccurred())
	awaitGeneratedReuseRevision(updated.GetModRevision())
}

func verifyReuseDependencies(base time.Time, scenario qlgen.ReuseScenario) {
	switch scenario.Kind {
	case "schema-replace-delete-recreate":
		verifyReuseSchemaLifecycle(base, scenario)
	case "multi-group-dependencies":
		verifyReuseMultiGroup(base, scenario)
	case "same-revision-watch", "lower-revision-watch":
		verifyReuseRevision(base, scenario)
	case "permissions-and-fallback":
		verifyReusePermissionsAndFallback(base, scenario)
	}
}

func verifyReuseSchemaLifecycle(base time.Time, scenario qlgen.ReuseScenario) {
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[0])).To(gm.Succeed())
	before := liaisongrpc.BydbQLPlanCacheStats()
	replaceGeneratedReuseSchema(qlgen.FixtureGroup)
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[1])).To(gm.Succeed())
	afterReplace := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(afterReplace.Stale).To(gm.BeNumerically(">", before.Stale))
	gm.Expect(afterReplace.Compiles).To(gm.BeNumerically(">", before.Compiles))
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	deleted, deleteErr := registry.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{Metadata: qlgen.T05FixtureSchema().GetMetadata()})
	gm.Expect(deleteErr).NotTo(gm.HaveOccurred())
	gm.Expect(deleted.GetDeleted()).To(gm.BeTrue())
	needsRecreate := true
	defer func() {
		if needsRecreate {
			restoreGeneratedReuseFixture(base)
		}
	}()
	// The cache contract is the liaison's local schema view. Observe deletion
	// through the actual QL endpoint; waiting for physical absence on every
	// data node would test a separate cluster-wide deletion barrier.
	gm.Eventually(func() error {
		return executeGeneratedReuseCall(context.Background(), base, scenario.Cases[2])
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
	created, recreateErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: qlgen.T05FixtureSchema()})
	gm.Expect(recreateErr).NotTo(gm.HaveOccurred())
	awaitGeneratedReuseRevision(created.GetModRevision())
	writeGeneratedReuseFixture(base)
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[3])).To(gm.Succeed())
	gm.Expect(liaisongrpc.BydbQLPlanCacheStats().Compiles).To(gm.BeNumerically(">", afterReplace.Compiles))
	needsRecreate = false
}

func verifyReuseMultiGroup(base time.Time, scenario qlgen.ReuseScenario) {
	const other = qlgen.T05OtherFixtureGroup
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	groupRegistry := databasev1.NewGroupRegistryServiceClient(SharedContext.Connection)
	_, groupErr := groupRegistry.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: other}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1, SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}})
	gm.Expect(groupErr).NotTo(gm.HaveOccurred())
	schema := qlgen.T05FixtureSchema()
	schema.Metadata.Group = other
	created, schemaErr := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection).
		Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
	gm.Expect(schemaErr).NotTo(gm.HaveOccurred())
	awaitGeneratedReuseRevision(created.GetModRevision())
	cases := scenario.Cases
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, cases[0])).To(gm.Succeed())
	before := liaisongrpc.BydbQLPlanCacheStats()
	// Updating an unreferenced object must leave this multi-group plan reusable.
	unrelated := qlgen.T05FixtureSchema()
	unrelated.Metadata.Name = "ql_t05_unrelated"
	unrelatedCreated, unrelatedErr := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection).
		Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: unrelated})
	gm.Expect(unrelatedErr).NotTo(gm.HaveOccurred())
	awaitGeneratedReuseRevision(unrelatedCreated.GetModRevision())
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, cases[1])).To(gm.Succeed())
	gm.Expect(liaisongrpc.BydbQLPlanCacheStats().Compiles).To(gm.Equal(before.Compiles))
	replaceGeneratedReuseSchema(other)
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, cases[2])).To(gm.Succeed())
	after := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(after.Compiles).To(gm.BeNumerically(">", before.Compiles), "every queried group contributes a dependency")
	gm.Expect(after.Stale).To(gm.BeNumerically(">", before.Stale))
}

func verifyReuseRevision(base time.Time, scenario qlgen.ReuseScenario) {
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[0])).To(gm.Succeed())
	before := liaisongrpc.BydbQLPlanCacheStats()
	var revision *int64
	if scenario.Kind == "lower-revision-watch" {
		lower := int64(1)
		revision = &lower
	}
	restore := liaisongrpc.BumpBydbQLMeasureGenerationForTest(qlgen.FixtureGroup, qlgen.T05FixtureName, revision)
	defer restore()
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[1])).To(gm.Succeed())
	after := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(after.Stale).To(gm.BeNumerically(">", before.Stale), "revision inequality and watch generation invalidate a cached dependency")
	if revision != nil {
		gm.Expect(after.Bypass).To(gm.BeNumerically(">", before.Bypass), "a local revision inconsistent with registry resolution cannot publish a reusable plan")
	}
	restore()
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[2])).To(gm.Succeed())
}

func restoreGeneratedReuseFixture(base time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	created, createErr := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection).
		Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: qlgen.T05FixtureSchema()})
	if status.Code(createErr) != codes.AlreadyExists {
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		awaitGeneratedReuseRevision(created.GetModRevision())
	}
	writeGeneratedReuseFixture(base)
}
