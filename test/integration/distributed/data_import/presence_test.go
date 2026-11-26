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

package data_import_test

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// A CSV import into a target whose groups have the source's shard numbers checks, when run again,
// that every shard it wrote rows into still holds rows, summing the replicas of the shard: a shard
// held by both replicas, or by one of them, exists.
var _ = ginkgo.Describe("Data check of a CSV import per shard across replicas", ginkgo.Ordered, func() {
	var target *cluster

	ginkgo.BeforeAll(func() {
		// Empty: the import creates every group with the source's shard number and replicas.
		target = startCluster([]string{hotLabel, hotLabel, warmLabel}, true)
		ginkgo.DeferCleanup(target.stop)
	})

	ginkgo.It("finds every shard on its replicas and refuses once the shards of a group hold nothing", func() {
		opts := importOptionsWith(csvArtifact, nil)
		first := transfertest.RunImport(context.Background(), target.liaisonAddr, opts, nil)
		gomega.Expect(first.Err).NotTo(gomega.HaveOccurred(), first.Log)
		exportRoundTrip(target, csvArtifact)

		again := transfertest.RunImport(context.Background(), target.liaisonAddr, opts, nil)
		gomega.Expect(again.Err).NotTo(gomega.HaveOccurred(), again.Log)
		gomega.Expect(again.Log).To(gomega.ContainSubstring("already imported into this cluster at "))
		gomega.Expect(again.Log).NotTo(gomega.ContainSubstring("not per shard"), "every group has the source's shard number")

		ginkgo.By("Deleting the data of the stream group, its schema kept")
		client := databasev1.NewGroupRegistryServiceClient(target.conn)
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		_, err := client.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: transfertest.StreamGroup, Force: true, DataOnly: true})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func(g gomega.Gomega) {
			resp, queryErr := client.Query(ctx, &databasev1.GroupRegistryServiceQueryRequest{Group: transfertest.StreamGroup})
			g.Expect(queryErr).NotTo(gomega.HaveOccurred())
			g.Expect(resp.GetTask().GetCurrentPhase()).To(gomega.Equal(databasev1.GroupDeletionTask_PHASE_COMPLETED), resp.GetTask().GetMessage())
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

		refused := transfertest.RunImport(context.Background(), target.liaisonAddr, opts, nil)
		gomega.Expect(refused.ExitCode()).To(gomega.Equal(exitcode.ExitResume), refused.Log)
		perShard := "stream " + transfertest.StreamGroup + ` stage hot shard \d+: \d+ row\(s\) imported \(`
		gomega.Expect(refused.Err.Error()).To(gomega.MatchRegexp(perShard))
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring("no node of the target holds any"))
	})
})
