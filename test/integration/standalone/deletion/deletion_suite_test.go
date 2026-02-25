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

// Package integration_deletion_test provides integration tests for group deletion in standalone mode.
package integration_deletion_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestDeletion(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Integration Deletion Suite", g.Label(integration_standalone.Labels...))
}

var _ = g.BeforeSuite(func() {
	gm.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gm.Succeed())
})

var _ = g.Describe("GroupDeletion", func() {
	var goods []gleak.Goroutine
	var conn *grpclib.ClientConn
	var deferFn func()
	var dataPath string
	var groupClient databasev1.GroupRegistryServiceClient

	g.BeforeEach(func() {
		goods = gleak.Goroutines()
		var pathDeferFn func()
		var spaceErr error
		dataPath, pathDeferFn, spaceErr = test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())
		ports, portsErr := test.AllocateFreePorts(4)
		gm.Expect(portsErr).NotTo(gm.HaveOccurred())
		var addr string
		var serverCloseFn func()
		addr, _, serverCloseFn = setup.ClosableStandalone(dataPath, ports)
		var connErr error
		conn, connErr = grpchelper.Conn(addr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(connErr).NotTo(gm.HaveOccurred())
		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
		deferFn = func() {
			serverCloseFn()
			pathDeferFn()
		}
	})

	g.AfterEach(func() {
		_ = conn.Close()
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	g.It("returns NotFound when deleting a nonexistent group", func() {
		_, err := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: "nonexistent-group-xyz",
		})
		gm.Expect(err).Should(gm.HaveOccurred())
		errStatus, ok := status.FromError(err)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(errStatus.Code()).To(gm.Equal(codes.NotFound))
	})

	g.It("can delete an empty group without force flag", func() {
		const newGroup = "empty-test-group"
		streamDir := filepath.Join(dataPath, "stream", "data", newGroup)
		g.By("Creating a new empty group")
		_, err := groupClient.Create(context.TODO(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: newGroup},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 1,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			},
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying the group exists")
		getResp, err := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: newGroup})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())
		gm.Expect(getResp.GetGroup().GetMetadata().GetName()).To(gm.Equal(newGroup))

		g.By("Verifying group data directory exists on disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(streamDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Deleting the empty group without force")
		gm.Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: newGroup,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		g.By("Verifying the group is eventually removed")
		gm.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: newGroup})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gm.Equal(codes.NotFound))

		g.By("Verifying group data directory is removed from disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(streamDir)
			return os.IsNotExist(statErr)
		}, flags.EventuallyTimeout).Should(gm.BeTrue())
	})

	g.It("can delete an existing group with force=true", func() {
		const groupName = "sw_metric"
		measureDir := filepath.Join(dataPath, "measure", "data", groupName)
		g.By("Verifying the group exists with resources")
		_, err := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying group data directory exists on disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Deleting group with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying the group is eventually removed")
		gm.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gm.Equal(codes.NotFound))

		g.By("Verifying group data directory is removed from disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return os.IsNotExist(statErr)
		}, flags.EventuallyTimeout).Should(gm.BeTrue())
	})

	g.It("can query deletion task status until completed", func() {
		const groupName = "sw_metric"
		measureDir := filepath.Join(dataPath, "measure", "data", groupName)
		g.By("Verifying group data directory exists on disk before deletion")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Initiating group deletion with force=true")
		_, err := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Waiting for deletion task to pass through IN_PROGRESS and reach COMPLETED")
		var seenInProgress bool
		gm.Eventually(func() databasev1.GroupDeletionTask_Phase {
			queryResp, queryErr := groupClient.Query(context.TODO(), &databasev1.GroupRegistryServiceQueryRequest{
				Group: groupName,
			})
			if queryErr != nil {
				return databasev1.GroupDeletionTask_PHASE_UNSPECIFIED
			}
			phase := queryResp.GetTask().GetCurrentPhase()
			if phase == databasev1.GroupDeletionTask_PHASE_IN_PROGRESS {
				seenInProgress = true
			}
			return phase
		}, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(databasev1.GroupDeletionTask_PHASE_COMPLETED))
		gm.Expect(seenInProgress).To(gm.BeTrue(), "deletion task should have passed through IN_PROGRESS phase")
	})
})
