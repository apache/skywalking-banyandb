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

package property_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/lifecycle"
)

func init() {
	lifecycle.SetupFunc = func() lifecycle.SetupResult {
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		Expect(tmpErr).NotTo(HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		By("Starting hot data node")
		dataAddr, srcDir, closeDataNode0 := setup.DataNodeWithAddrAndDir(config, "--node-labels", "type=hot",
			"--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s", "--trace-flush-timeout", "0s")
		By("Starting warm data node")
		_, destDir, closeDataNode1 := setup.DataNodeWithAddrAndDir(config, "--node-labels", "type=warm",
			"--has-schema-role=false",
			"--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s", "--trace-flush-timeout", "0s")
		setup.PreloadSchemaViaProperty(config, test_stream.LoadSchemaWithStages, test_measure.LoadSchemaWithStages,
			test_trace.PreloadSchemaWithStages, test_property.PreloadSchema)
		By("Capturing per-node group snapshots after property schema preload")
		nodeGroups := setup.QueryNodeGroups(config)
		schemaAddrs := config.SchemaServerAddrs()
		sort.Strings(schemaAddrs)
		for _, schemaAddr := range schemaAddrs {
			groups := nodeGroups[schemaAddr]
			By(fmt.Sprintf("Schema server %s groups(%d): %v", schemaAddr, len(groups), groups))
		}
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config, "--data-node-selector", "type=hot")
		By("Verifying cluster state: hot node has ROLE_META, warm node does not")
		verifyClusterNodeRoles(liaisonAddr)
		By("Initializing test cases with 10 days before")
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		tenDaysBeforeNow := now.Add(-10 * 24 * time.Hour)
		test_cases.Initialize(liaisonAddr, tenDaysBeforeNow)
		time.Sleep(flags.ConsistentlyTimeout)
		return lifecycle.SetupResult{
			DataAddr:                  dataAddr,
			LiaisonAddr:               liaisonAddr,
			SrcDir:                    srcDir,
			DestDir:                   destDir,
			TenDaysBeforeNow:          tenDaysBeforeNow,
			PropertyDiscoveryFilePath: dfWriter.Path(),
			MetadataFlags: []string{
				"--schema-registry-mode=property",
				"--node-discovery-mode=file",
				fmt.Sprintf("--node-discovery-file-path=%s", dfWriter.Path()),
			},
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				closeDataNode1()
				tmpDirCleanup()
			},
		}
	}
}

func verifyClusterNodeRoles(liaisonAddr string) {
	conn, connErr := grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpclib.WithTransportCredentials(insecure.NewCredentials()))
	Expect(connErr).NotTo(HaveOccurred())
	defer func() { _ = conn.Close() }()
	clusterClient := databasev1.NewClusterStateServiceClient(conn)
	Eventually(func(g Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		state, stateErr := clusterClient.GetClusterState(ctx, &databasev1.GetClusterStateRequest{})
		g.Expect(stateErr).NotTo(HaveOccurred())
		tire2 := state.GetRouteTables()["tire2"]
		g.Expect(tire2).NotTo(BeNil(), "tire2 route table not found")
		foundHot := false
		foundWarm := false
		for _, node := range tire2.GetRegistered() {
			labels := node.GetLabels()
			hasMetaRole := false
			for _, role := range node.GetRoles() {
				if role == databasev1.Role_ROLE_META {
					hasMetaRole = true
					break
				}
			}
			if labels["type"] == "hot" {
				foundHot = true
				g.Expect(hasMetaRole).To(BeTrue(),
					fmt.Sprintf("hot node %s should have ROLE_META", node.GetMetadata().GetName()))
			} else if labels["type"] == "warm" {
				foundWarm = true
				g.Expect(hasMetaRole).To(BeFalse(),
					fmt.Sprintf("warm node %s should NOT have ROLE_META", node.GetMetadata().GetName()))
			}
		}
		g.Expect(foundHot).To(BeTrue(), "no hot node found in cluster state")
		g.Expect(foundWarm).To(BeTrue(), "no warm node found in cluster state")
	}).WithTimeout(flags.EventuallyTimeout).WithPolling(time.Second).Should(Succeed())
}

func TestPropertyLifecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Property Lifecycle Suite")
}
