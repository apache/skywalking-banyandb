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

// Package integration_deletion_test provides integration tests for group deletion in distributed mode.
package integration_deletion_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func TestDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Deletion Suite")
}

var (
	deferFunc       func()
	goods           []gleak.Goroutine
	connection      *grpc.ClientConn
	groupClient     databasev1.GroupRegistryServiceClient
	dataNode0Path   string
	dataNode1Path   string
	liaisonNodePath string
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()

	By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	dir, spaceDef, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(dir),
		embeddedetcd.AutoCompactionMode("periodic"),
		embeddedetcd.AutoCompactionRetention("1h"),
		embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
	)
	Expect(err).ShouldNot(HaveOccurred())
	<-server.ReadyNotify()

	By("Starting data node 0")
	_, dn0Path, closeDataNode0 := setup.DataNodeWithAddrAndDir(ep)
	By("Starting data node 1")
	_, dn1Path, closeDataNode1 := setup.DataNodeWithAddrAndDir(ep)
	By("Starting liaison node")
	liaisonAddr, liaisonPath, closerLiaisonNode := setup.LiaisonNodeWithAddrAndDir(ep)

	deferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		closeDataNode1()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}

	return []byte(liaisonAddr + "," + dn0Path + "," + dn1Path + "," + liaisonPath)
}, func(address []byte) {
	parts := strings.SplitN(string(address), ",", 4)
	liaisonGrpcAddr := parts[0]
	dataNode0Path = parts[1]
	dataNode1Path = parts[2]
	liaisonNodePath = parts[3]

	var connErr error
	connection, connErr = grpchelper.Conn(liaisonGrpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(connErr).NotTo(HaveOccurred())

	groupClient = databasev1.NewGroupRegistryServiceClient(connection)
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Distributed Deletion Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

var _ = Describe("GroupDeletion", func() {
	It("returns NotFound when deleting a nonexistent group", func() {
		_, err := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: "nonexistent-group-dist-xyz",
		})
		Expect(err).Should(HaveOccurred())
		errStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(errStatus.Code()).To(Equal(codes.NotFound))
	})

	It("can delete an empty group without force flag", func() {
		const newGroup = "dist-empty-deletion-group"
		dn0StreamDir := filepath.Join(dataNode0Path, "stream", "data", newGroup)
		dn1StreamDir := filepath.Join(dataNode1Path, "stream", "data", newGroup)
		liaisonStreamDir := filepath.Join(liaisonNodePath, "stream", "data", newGroup)
		By("Creating a new empty group")
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
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying the group exists")
		getResp, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: newGroup})
		Expect(getErr).ShouldNot(HaveOccurred())
		Expect(getResp.GetGroup().GetMetadata().GetName()).To(Equal(newGroup))

		By("Verifying group data directories exist on all nodes")
		Eventually(func() bool {
			_, statErr0 := os.Stat(dn0StreamDir)
			_, statErr1 := os.Stat(dn1StreamDir)
			_, statErrL := os.Stat(liaisonStreamDir)
			return statErr0 == nil && statErr1 == nil && statErrL == nil
		}, flags.EventuallyTimeout).Should(BeTrue())

		By("Deleting the empty group without force")
		Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: newGroup,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(Succeed())

		By("Verifying the group is eventually removed")
		Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: newGroup})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(Equal(codes.NotFound))

		By("Verifying group data directories are removed from all nodes")
		Eventually(func() bool {
			_, statErr0 := os.Stat(dn0StreamDir)
			_, statErr1 := os.Stat(dn1StreamDir)
			_, statErrL := os.Stat(liaisonStreamDir)
			return os.IsNotExist(statErr0) && os.IsNotExist(statErr1) && os.IsNotExist(statErrL)
		}, flags.EventuallyTimeout).Should(BeTrue())
	})

	It("can delete an existing group with force=true", func() {
		const groupName = "dist-force-deletion-group"
		dn0MeasureDir := filepath.Join(dataNode0Path, "measure", "data", groupName)
		dn1MeasureDir := filepath.Join(dataNode1Path, "measure", "data", groupName)
		liaisonMeasureDir := filepath.Join(liaisonNodePath, "measure", "data", groupName)
		By("Creating a group with resources")
		_, err := groupClient.Create(context.TODO(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
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
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying group data directories exist on all nodes")
		Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return statErr0 == nil && statErr1 == nil && statErrL == nil
		}, flags.EventuallyTimeout).Should(BeTrue())

		By("Deleting group with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying the group is eventually removed")
		Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(Equal(codes.NotFound))

		By("Verifying group data directories are removed from all nodes")
		Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return os.IsNotExist(statErr0) && os.IsNotExist(statErr1) && os.IsNotExist(statErrL)
		}, flags.EventuallyTimeout).Should(BeTrue())
	})

	It("can query deletion task status until completed", func() {
		const groupName = "dist-deletion-task-group"
		By("Creating a group to delete")
		_, err := groupClient.Create(context.TODO(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
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
		Expect(err).ShouldNot(HaveOccurred())

		By("Initiating group deletion with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		Expect(err).ShouldNot(HaveOccurred())

		By("Waiting for deletion task to reach COMPLETED")
		Eventually(func() databasev1.GroupDeletionTask_Phase {
			queryResp, queryErr := groupClient.Query(context.TODO(), &databasev1.GroupRegistryServiceQueryRequest{
				Group: groupName,
			})
			if queryErr != nil {
				return databasev1.GroupDeletionTask_PHASE_UNSPECIFIED
			}
			return queryResp.GetTask().GetCurrentPhase()
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(databasev1.GroupDeletionTask_PHASE_COMPLETED))
	})
})
