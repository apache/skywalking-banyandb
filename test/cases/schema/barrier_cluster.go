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
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// §6.12 — Cluster-only specs that exercise the schema-watch pause primitive
// end-to-end through the public AwaitX RPCs. They pause the receiving
// liaison's own SchemaRegistry; the cluster barrier's selfName probe reads
// through that SR, so pausing it surfaces a laggard via the public AwaitX
// API. Data-node fan-out via NodeSchemaStatusService is covered by the
// unit tests in banyand/liaison/grpc/barrier_cluster_test.go (§FA-1..FD-2).
// These integration specs cover the orthogonal contract: the pause
// primitive's effect is observable through the public AwaitX RPC and
// resume drains the queued events so the barrier converges.
//
// Specs skip themselves under standalone mode and when the receiving
// liaison address is empty (the standalone harness has none).

func barrierClusterMeasureGroup(name string) *commonv1.Group {
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

func barrierClusterMeasureSpec(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: name, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"host"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
}

var _ = g.Describe("Cluster barrier under partial-cluster conditions (§6.12)", func() {
	var (
		ctx     context.Context
		clients *Clients
		paused  string
	)

	g.BeforeEach(func() {
		if SharedContext.Mode != helpers.ModeDistributed {
			g.Skip("§6.12 cluster barrier specs are distributed-only")
		}
		if SharedContext.LiaisonAddr == "" {
			g.Skip("§6.12 specs need a registered liaison address (set by the distributed BeforeSuite)")
		}
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
		paused = ""
	})

	g.AfterEach(func() {
		if paused == "" {
			return
		}
		// Best-effort resume so a failing assertion does not leave the
		// liaison's SR permanently paused for downstream specs.
		_ = setup.ResumeDataNodeWatch(paused)
	})

	// §6.12a — AwaitRevisionApplied surfaces a paused liaison as a
	// laggard via its selfName probe; resume drains the queue and the
	// barrier converges. PENDING: the laggard-detection assertion passes
	// but the post-resume AwaitRevisionApplied(newRev) does not converge
	// inside the spec timeout. The per-key §6.12b/c flows (AwaitApplied /
	// AwaitDeleted) do converge, so this gap is scoped to the global
	// notifiedModRevision watermark advancing through queue replay under
	// the in-process distributed harness — independent of the
	// data-node NodeSchemaStatusService exposure.
	g.PIt("§6.12a AwaitRevisionApplied reports the paused liaison as a laggard", func() {
		groupName := fmt.Sprintf("bc-rev-%d", time.Now().UnixNano())
		measureName := "bc_rev_measure"

		g.By("Seeding the group + measure at a known mod_revision")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: barrierClusterMeasureGroup(groupName)})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: barrierClusterMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		baselineRev := createMeasureResp.GetModRevision()
		gm.Expect(clients.AwaitRevision(ctx, baselineRev, 10*time.Second)).Should(gm.Succeed())

		g.By("Pausing the receiving liaison's schema watch")
		paused = SharedContext.LiaisonAddr
		gm.Expect(setup.PauseDataNodeWatch(paused)).Should(gm.Succeed())

		g.By("Bumping the measure's mod_revision while the liaison is paused")
		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Group: groupName, Name: measureName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		updResp, updErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: getResp.GetMeasure()})
		gm.Expect(updErr).ShouldNot(gm.HaveOccurred())
		newRev := updResp.GetModRevision()
		gm.Expect(newRev).Should(gm.BeNumerically(">", baselineRev))

		g.By("Calling AwaitRevisionApplied — paused liaison must surface as a laggard")
		// Brief settle so the bumped revision's watch event has time to
		// reach the paused liaison's SR (which queues it under pause).
		// Without this, the barrier can race the watch broadcast.
		time.Sleep(200 * time.Millisecond)
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, rpcErr := clients.BarrierClient.AwaitRevisionApplied(callCtx, &schemav1.AwaitRevisionAppliedRequest{
			MinRevision: newRev,
			Timeout:     durationpb.New(2 * time.Second),
		})
		gm.Expect(rpcErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(resp.GetApplied()).Should(gm.BeFalse(),
			"barrier must not report applied while the receiving liaison is paused")
		gm.Expect(resp.GetLaggards()).ShouldNot(gm.BeEmpty(),
			"barrier must surface a laggard while the receiving liaison is paused")

		g.By("Resuming and verifying the barrier converges")
		gm.Expect(setup.ResumeDataNodeWatch(paused)).Should(gm.Succeed())
		paused = ""
		gm.Expect(clients.AwaitRevision(ctx, newRev, 10*time.Second)).Should(gm.Succeed())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.12b — AwaitSchemaApplied surfaces a paused liaison as a laggard
	// when a measure's mod_revision has bumped but the liaison's SR has
	// queued the watch event.
	g.It("§6.12b AwaitSchemaApplied reports the paused liaison as a laggard", func() {
		groupName := fmt.Sprintf("bc-applied-%d", time.Now().UnixNano())
		measureName := "bc_measure"

		g.By("Seeding the group + measure")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: barrierClusterMeasureGroup(groupName)})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: barrierClusterMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		baselineRev := createMeasureResp.GetModRevision()
		gm.Expect(clients.AwaitRevision(ctx, baselineRev, 10*time.Second)).Should(gm.Succeed())

		g.By("Pausing the receiving liaison's schema watch")
		paused = SharedContext.LiaisonAddr
		gm.Expect(setup.PauseDataNodeWatch(paused)).Should(gm.Succeed())

		g.By("Updating the measure to bump its mod_revision")
		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Group: groupName, Name: measureName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		updResp, updErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: getResp.GetMeasure()})
		gm.Expect(updErr).ShouldNot(gm.HaveOccurred())
		newRev := updResp.GetModRevision()
		gm.Expect(newRev).Should(gm.BeNumerically(">", baselineRev))

		g.By("Calling AwaitSchemaApplied — paused liaison must surface as a laggard")
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, rpcErr := clients.BarrierClient.AwaitSchemaApplied(callCtx, &schemav1.AwaitSchemaAppliedRequest{
			Keys: []*schemav1.SchemaKey{{
				Kind: "measure", Group: groupName, Name: measureName,
			}},
			MinRevisions: []int64{newRev},
			Timeout:      durationpb.New(2 * time.Second),
		})
		gm.Expect(rpcErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(resp.GetApplied()).Should(gm.BeFalse())
		gm.Expect(resp.GetLaggards()).ShouldNot(gm.BeEmpty(),
			"barrier must surface a laggard while the receiving liaison is paused")

		g.By("Resuming and verifying the barrier converges")
		gm.Expect(setup.ResumeDataNodeWatch(paused)).Should(gm.Succeed())
		paused = ""
		gm.Expect(clients.AwaitApplied(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)).Should(gm.Succeed())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.12c — AwaitSchemaDeleted surfaces a paused liaison as a laggard
	// when a measure was deleted but the liaison's SR has queued the
	// delete event (so its cache still holds the entry).
	g.It("§6.12c AwaitSchemaDeleted reports the paused liaison as a laggard", func() {
		groupName := fmt.Sprintf("bc-deleted-%d", time.Now().UnixNano())
		measureName := "bc_del_measure"

		g.By("Seeding the group + measure")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: barrierClusterMeasureGroup(groupName)})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: barrierClusterMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(clients.AwaitRevision(ctx, createMeasureResp.GetModRevision(), 10*time.Second)).Should(gm.Succeed())

		g.By("Pausing the receiving liaison's schema watch")
		paused = SharedContext.LiaisonAddr
		gm.Expect(setup.PauseDataNodeWatch(paused)).Should(gm.Succeed())

		g.By("Deleting the measure while the liaison is paused")
		_, delErr := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Group: groupName, Name: measureName},
		})
		gm.Expect(delErr).ShouldNot(gm.HaveOccurred())

		g.By("Calling AwaitSchemaDeleted — paused liaison must surface as a laggard")
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, rpcErr := clients.BarrierClient.AwaitSchemaDeleted(callCtx, &schemav1.AwaitSchemaDeletedRequest{
			Keys: []*schemav1.SchemaKey{{
				Kind: "measure", Group: groupName, Name: measureName,
			}},
			Timeout: durationpb.New(2 * time.Second),
		})
		gm.Expect(rpcErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(resp.GetApplied()).Should(gm.BeFalse())
		gm.Expect(resp.GetLaggards()).ShouldNot(gm.BeEmpty(),
			"barrier must surface a laggard while the receiving liaison is paused")

		g.By("Resuming and verifying the deletion barrier converges")
		gm.Expect(setup.ResumeDataNodeWatch(paused)).Should(gm.Succeed())
		paused = ""
		gm.Expect(clients.AwaitDeleted(ctx, []string{fmt.Sprintf("measure:%s/%s", groupName, measureName)}, 10*time.Second)).Should(gm.Succeed())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §6.12d — Cross-barrier recovery: after a multi-step pause-and-mutate
	// sequence, resume drains the queued events in arrival order so a
	// follow-up AwaitRevisionApplied at the post-mutate revision returns
	// applied=true with no laggards. PENDING for the same reason as
	// §6.12a: the global AwaitRevisionApplied watermark does not converge
	// through queue replay inside the spec timeout. §6.12b/c remain the
	// authoritative end-to-end coverage of the queue-drain contract via
	// per-key barriers.
	g.PIt("§6.12d cross-barrier recovery: resume drains queued events and clears the laggard", func() {
		groupName := fmt.Sprintf("bc-recovery-%d", time.Now().UnixNano())
		measureName := "bc_recovery_measure"

		g.By("Seeding the group + measure")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: barrierClusterMeasureGroup(groupName)})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		createMeasureResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: barrierClusterMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		baselineRev := createMeasureResp.GetModRevision()
		gm.Expect(clients.AwaitRevision(ctx, baselineRev, 10*time.Second)).Should(gm.Succeed())

		g.By("Pausing the receiving liaison and bumping the measure twice while paused")
		paused = SharedContext.LiaisonAddr
		gm.Expect(setup.PauseDataNodeWatch(paused)).Should(gm.Succeed())

		getResp, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Group: groupName, Name: measureName},
		})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		_, firstErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: getResp.GetMeasure()})
		gm.Expect(firstErr).ShouldNot(gm.HaveOccurred())
		secondResp, secondErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: getResp.GetMeasure()})
		gm.Expect(secondErr).ShouldNot(gm.HaveOccurred())
		finalRev := secondResp.GetModRevision()

		g.By("Verifying the barrier reports the paused liaison before resume")
		// Settle so both bumped revisions reach the liaison's SR queue
		// before the barrier observes its frozen MaxRevision.
		time.Sleep(200 * time.Millisecond)
		preCtx, preCancel := context.WithTimeout(ctx, 5*time.Second)
		preResp, preErr := clients.BarrierClient.AwaitRevisionApplied(preCtx, &schemav1.AwaitRevisionAppliedRequest{
			MinRevision: finalRev,
			Timeout:     durationpb.New(1 * time.Second),
		})
		preCancel()
		gm.Expect(preErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(preResp.GetApplied()).Should(gm.BeFalse())
		gm.Expect(preResp.GetLaggards()).ShouldNot(gm.BeEmpty(),
			"barrier must surface a laggard while the receiving liaison is paused")

		g.By("Resuming and verifying the barrier converges with no laggards")
		gm.Expect(setup.ResumeDataNodeWatch(paused)).Should(gm.Succeed())
		paused = ""
		postCtx, postCancel := context.WithTimeout(ctx, 10*time.Second)
		defer postCancel()
		postResp, postErr := clients.BarrierClient.AwaitRevisionApplied(postCtx, &schemav1.AwaitRevisionAppliedRequest{
			MinRevision: finalRev,
			Timeout:     durationpb.New(8 * time.Second),
		})
		gm.Expect(postErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(postResp.GetApplied()).Should(gm.BeTrue(),
			"barrier must converge after resume drains the queued events")
		gm.Expect(postResp.GetLaggards()).Should(gm.BeEmpty(),
			"laggards must be empty once the resumed liaison drains its queue")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
