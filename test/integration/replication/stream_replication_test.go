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
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

var _ = g.Describe("Stream Normal Mode Replication", func() {
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

	g.It("should return consistent results from replicas", func() {
		g.By("Verifying the stream exists in default group")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		streamMetadata := &commonv1.Metadata{
			Name:  "sw",
			Group: "default",
		}

		schemaClient := databasev1.NewStreamRegistryServiceClient(conn)
		resp, err := schemaClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: streamMetadata})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.GetStream()).NotTo(gm.BeNil())
		gm.Expect(resp.GetStream().GetMetadata().GetGroup()).To(gm.Equal("default"))

		g.By("Verifying replication factor for default group")
		groupClient := databasev1.NewGroupRegistryServiceClient(conn)
		groupResp, groupErr := groupClient.Get(ctx, &databasev1.GroupRegistryServiceGetRequest{
			Group: "default",
		})
		gm.Expect(groupErr).NotTo(gm.HaveOccurred())
		gm.Expect(groupResp.GetGroup()).NotTo(gm.BeNil())
		gm.Expect(groupResp.GetGroup().GetResourceOpts().GetReplicas()).To(gm.Equal(uint32(2)),
			"default group should have replicas=2")

		g.By("Querying data multiple times to verify consistency (deduplication)")
		verifyStreamDataContentWithArgs(conn, now, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})
	})

	g.It("should survive single node failure", func() {
		g.By("Verifying the stream exists in default group")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		streamMetadata := &commonv1.Metadata{
			Name:  "sw",
			Group: "default",
		}

		schemaClient := databasev1.NewStreamRegistryServiceClient(conn)
		resp, err := schemaClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: streamMetadata})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.GetStream()).NotTo(gm.BeNil())

		g.By("Stopping one data node")
		closersToStop := make([]func(), len(dataNodeClosers))
		copy(closersToStop, dataNodeClosers)
		closersToStop[0]()

		g.By("Verifying data is still accessible after node failure")
		verifyStreamDataContentWithArgs(conn, now, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})
	})

	g.It("should recover data after node restart", func() {
		g.By("Verifying the stream exists in default group")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		streamMetadata := &commonv1.Metadata{
			Name:  "sw",
			Group: "default",
		}

		schemaClient := databasev1.NewStreamRegistryServiceClient(conn)
		resp, err := schemaClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: streamMetadata})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.GetStream()).NotTo(gm.BeNil())

		g.By("Stopping one data node")
		closersToStop := make([]func(), len(dataNodeClosers))
		copy(closersToStop, dataNodeClosers)
		closersToStop[0]()

		g.By("Verifying data is still accessible during node downtime")
		verifyStreamDataContentWithArgs(conn, now, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})

		g.By("Restarting the data node")
		_, _, closeDataNode := setup.DataNodeFromDataDir(clusterConfig, dataNodeDirs[0], "--node-labels", "role=data")
		dataNodeClosers[0] = closeDataNode

		g.By("Waiting for cluster to stabilize and handoff queue to drain")
		gm.Eventually(func() bool {
			return isClusterStable(conn)
		}, flags.EventuallyTimeout).Should(gm.BeTrue(), "Cluster should stabilize after node restart")

		g.By("Verifying data is still accessible after node restart")
		verifyStreamDataContentWithArgs(conn, now, helpers.Args{
			Input:    "all",
			Duration: 25 * time.Minute,
			Offset:   -20 * time.Minute,
		})
	})
})

func verifyStreamDataContentWithArgs(conn *grpc.ClientConn, baseTime time.Time, args helpers.Args) {
	sharedContext := helpers.SharedContext{
		Connection: conn,
		BaseTime:   baseTime,
	}
	gm.Eventually(func(innerGm gm.Gomega) {
		casesstreamdata.VerifyFn(innerGm, sharedContext, args)
	}, flags.EventuallyTimeout).Should(gm.Succeed(),
		"Should be able to query and verify stream data content")
}
