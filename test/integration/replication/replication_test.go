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

package replication_test

import (
	"context"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	metadataclient "github.com/apache/skywalking-banyandb/banyand/metadata/client"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = g.Describe("Replication", func() {
	var conn *grpc.ClientConn

	g.BeforeEach(func() {
		var err error
		conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	g.AfterEach(func() {
		if conn != nil {
			gm.Expect(conn.Close()).To(gm.Succeed())
		}
	})

	g.Context("with replicated_group", func() {
		g.It("should survive node failure", func() {
			g.By("Verifying the measure exists in replicated_group")
			ctx := context.Background()
			measureMetadata := &commonv1.Metadata{
				Name:  "service_traffic",
				Group: "replicated_group",
			}

			schemaClient := databasev1.NewMeasureRegistryServiceClient(conn)
			resp, err := schemaClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: measureMetadata})
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(resp.GetMeasure()).NotTo(gm.BeNil())
			gm.Expect(resp.GetMeasure().GetMetadata().GetGroup()).To(gm.Equal("replicated_group"))

			g.By("Getting list of all nodes from etcd (includes data nodes + liaison)")
			nodePath := "/" + metadataclient.DefaultNamespace + "/nodes"
			allNodes, err2 := helpers.ListKeys(etcdEndpoint, nodePath)
			gm.Expect(err2).NotTo(gm.HaveOccurred())

			// We have: 3 data nodes + 1 liaison node = 4 nodes total
			gm.Expect(len(allNodes)).To(gm.Equal(4),
				"Should have 4 nodes total (3 data nodes + 1 liaison node), found %d", len(allNodes))

			g.By("Stopping one data node")
			// We should have 3 data node closers in dataNodeClosers
			// Stop the first one
			// Create a local copy to avoid mutating the package-level slice
			closersToStop := make([]func(), len(dataNodeClosers))
			copy(closersToStop, dataNodeClosers)
			closersToStop[0]()

			// Wait for the cluster to stabilize
			gm.Eventually(func() int {
				nodes, err3 := helpers.ListKeys(etcdEndpoint, nodePath)
				if err3 != nil {
					return 0
				}
				return len(nodes)
			}, flags.EventuallyTimeout).Should(gm.Equal(3),
				"Should have 3 nodes total after stopping one data node (2 data nodes + 1 liaison)")

			g.By("Verifying data is still accessible after node failure")
			verifyDataContentAfterNodeFailure(conn, now)

			g.By("Verifying replication factor")
			groupClient := databasev1.NewGroupRegistryServiceClient(conn)
			groupResp, err := groupClient.Get(ctx, &databasev1.GroupRegistryServiceGetRequest{
				Group: "replicated_group",
			})
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(groupResp.GetGroup()).NotTo(gm.BeNil())
			gm.Expect(groupResp.GetGroup().GetResourceOpts().GetReplicas()).To(gm.Equal(uint32(2)),
				"replicated_group should have replicas=2")
		})
	})
})

func verifyDataContentAfterNodeFailure(conn *grpc.ClientConn, baseTime time.Time) {
	// This verifies that data is still accessible AFTER a node has failed

	// Create a SharedContext like in measure.go
	sharedContext := helpers.SharedContext{
		Connection: conn,
		BaseTime:   baseTime,
	}

	// Create args for the entity_replicated test case
	args := helpers.Args{
		Input:    "entity_replicated",
		Duration: 25 * time.Minute,
		Offset:   -20 * time.Minute,
		DisOrder: true,
	}

	// This will:
	// 1. Read entity_replicated.ql and entity_replicated.yaml
	// 2. Execute the query
	// 3. Verify results match expected data
	gm.Eventually(func(innerGm gm.Gomega) {
		casesmeasuredata.VerifyFn(innerGm, sharedContext, args)
	}, flags.EventuallyTimeout).Should(gm.Succeed(),
		"Should be able to query and verify data content after node failure")
}
