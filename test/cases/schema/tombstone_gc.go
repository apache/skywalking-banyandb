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

// Package schema contains shared test cases for schema-related functionality.
//
// §4.7.1 and §4.7.2 exercise tombstone retention behavior. The test suites wire
// the standalone server with --schema-server-tombstone-retention=2s in the
// BeforeSuite, so the specs run without requiring any environment variable.
package schema

import (
	"context"
	"fmt"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// tombstoneRetentionForTest mirrors the --schema-server-tombstone-retention flag
// configured in the integration suite setup. Keep in sync with common.go.
const tombstoneRetentionForTest = 2 * time.Second

// Schema tombstone GC specs — §4.7.1 / §4.7.2.
// The integration suite wires the standalone server with
// --schema-server-tombstone-retention=tombstoneRetentionForTest in BeforeSuite,
// so these specs run without any environment-variable setup.
var _ = g.Describe("Schema tombstone GC", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §4.7.1 — retains tombstone within retention window.
	// The tombstone invariant (Step 1.3): server stamps updated_at = now() which is always
	// after delete_time, so a normal recreate within the retention window always succeeds.
	// This spec verifies that the recreate succeeds and CreatedAt2 is after T_del.
	g.It("retains tombstone within retention window — recreate succeeds with fresh CreatedAt (§4.7.1)", func() {
		retention := tombstoneRetentionForTest

		groupName := fmt.Sprintf("tgc-retain-%d", time.Now().UnixNano())
		measureName := "tgc_measure"

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

		g.By("Capturing T_create from GetMeasure.CreatedAt")
		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(getResp.GetMeasure().GetCreatedAt()).ShouldNot(gm.BeNil())

		g.By("Deleting measure → T_del; awaiting deletion")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(deleteResp.GetDeleted()).Should(gm.BeTrue())
		tDel := deleteResp.GetDeleteTime()
		gm.Expect(tDel).Should(gm.BeNumerically(">", int64(0)))

		awaitDelErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDelErr).ShouldNot(gm.HaveOccurred())

		g.By("Waiting half the retention duration (within the window)")
		time.Sleep(retention / 2)

		// The server stamps updated_at = now() on create, which is always > T_del.
		// Therefore the tombstone invariant is satisfied and the recreate succeeds.
		g.By("Recreating same measure spec within retention window — must succeed")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred(),
			"recreate within retention window must succeed: server stamps updated_at = now() > T_del")

		r2 := createResp2.GetModRevision()
		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		g.By("Verifying CreatedAt2 is after T_del (Step 1.3 tombstone invariant)")
		getResp2, getErr2 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr2).ShouldNot(gm.HaveOccurred())
		createdAt2 := getResp2.GetMeasure().GetCreatedAt()
		gm.Expect(createdAt2).ShouldNot(gm.BeNil())
		gm.Expect(createdAt2.AsTime().UnixNano()).Should(gm.BeNumerically(">", tDel),
			"CreatedAt2 must be after T_del — Step 1.3 tombstone invariant")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.7.2 — physically removes tombstone after retention.
	// After retention + 1s, the GC loop purges the tombstone. A recreate succeeds
	// without any constraint from the (now-expired) tombstone.
	g.It("physically removes tombstone after retention — recreate succeeds cleanly (§4.7.2)", func() {
		retention := tombstoneRetentionForTest

		groupName := fmt.Sprintf("tgc-gc-%d", time.Now().UnixNano())
		measureName := "tgc_measure"

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

		g.By("Deleting measure → T_del; awaiting deletion")
		deleteResp, deleteErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(deleteErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(deleteResp.GetDeleted()).Should(gm.BeTrue())

		awaitDelErr := clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)
		gm.Expect(awaitDelErr).ShouldNot(gm.HaveOccurred())

		g.By("Waiting retention + 1s for GC to purge tombstone")
		time.Sleep(retention + time.Second)

		g.By("Recreating same measure spec after retention — must succeed (tombstone purged by GC)")
		createResp2, createErr2 := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: arMeasureSpec(groupName, measureName),
		})
		gm.Expect(createErr2).ShouldNot(gm.HaveOccurred(),
			"recreate after tombstone GC must succeed without InvalidArgument from tombstone invariant")

		r2 := createResp2.GetModRevision()
		awaitErr2 := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr2).ShouldNot(gm.HaveOccurred())

		g.By("Verifying measure exists after recreate")
		getResp2, getErr2 := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getErr2).ShouldNot(gm.HaveOccurred())
		gm.Expect(getResp2.GetMeasure()).ShouldNot(gm.BeNil())
		gm.Expect(getResp2.GetMeasure().GetMetadata().GetModRevision()).Should(gm.Equal(r2))

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
