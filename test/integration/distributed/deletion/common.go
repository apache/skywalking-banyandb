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

// Package deletion provides shared test setup for distributed group deletion integration tests.
package deletion

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
)

// SetupResult contains all info returned by SetupFunc.
type SetupResult struct {
	StopFunc        func()
	LiaisonAddr     string
	DataNode0Path   string
	DataNode1Path   string
	LiaisonNodePath string
}

// SetupFunc is provided by sub-packages to start the environment.
var SetupFunc func() SetupResult

var (
	result             SetupResult
	connection         *grpc.ClientConn
	groupClient        databasev1.GroupRegistryServiceClient
	measureRegClient   databasev1.MeasureRegistryServiceClient
	measureWriteClient measurev1.MeasureServiceClient
	dataNode0Path      string
	dataNode1Path      string
	liaisonNodePath    string
	goods              []gleak.Goroutine
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()
	result = SetupFunc()
	return []byte(result.LiaisonAddr + "," + result.DataNode0Path + "," + result.DataNode1Path + "," + result.LiaisonNodePath)
}, func(address []byte) {
	parts := strings.SplitN(string(address), ",", 4)
	liaisonGrpcAddr := parts[0]
	dataNode0Path = parts[1]
	dataNode1Path = parts[2]
	liaisonNodePath = parts[3]

	var connErr error
	connection, connErr = grpchelper.Conn(liaisonGrpcAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())

	groupClient = databasev1.NewGroupRegistryServiceClient(connection)
	measureRegClient = databasev1.NewMeasureRegistryServiceClient(connection)
	measureWriteClient = measurev1.NewMeasureServiceClient(connection)
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Distributed Deletion Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if result.StopFunc != nil {
			result.StopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

const (
	nonexistentGroupName = "nonexistent_test_group"
	emptyGroupName       = "empty_test_group"
	groupName            = "test_group"
	measureName          = "test_measure"
)

var _ = ginkgo.Describe("GroupDeletion", func() {
	ginkgo.It("returns NotFound when deleting a nonexistent group", func() {
		_, err := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: nonexistentGroupName,
		})
		gomega.Expect(err).Should(gomega.HaveOccurred())
		errStatus, ok := status.FromError(err)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(errStatus.Code()).To(gomega.Equal(codes.NotFound))
	})

	ginkgo.It("can delete an empty group without force flag", func() {
		dn0StreamDir := filepath.Join(dataNode0Path, "stream", "data", emptyGroupName)
		dn1StreamDir := filepath.Join(dataNode1Path, "stream", "data", emptyGroupName)
		liaisonStreamDir := filepath.Join(liaisonNodePath, "stream", "data", emptyGroupName)

		ginkgo.By("Creating a new empty group")
		_, err := groupClient.Create(context.TODO(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: emptyGroupName},
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
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Verifying the group exists")
		getResp, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: emptyGroupName})
		gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(getResp.GetGroup().GetMetadata().GetName()).To(gomega.Equal(emptyGroupName))

		ginkgo.By("Verifying group data directories exist on all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0StreamDir)
			_, statErr1 := os.Stat(dn1StreamDir)
			_, statErrL := os.Stat(liaisonStreamDir)
			return statErr0 == nil && statErr1 == nil && statErrL == nil
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		ginkgo.By("Deleting the empty group without force")
		gomega.Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: emptyGroupName,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(gomega.Succeed())

		ginkgo.By("Verifying the group is eventually removed")
		gomega.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: emptyGroupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gomega.Equal(codes.NotFound))

		ginkgo.By("Verifying group data directories are removed from all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0StreamDir)
			_, statErr1 := os.Stat(dn1StreamDir)
			_, statErrL := os.Stat(liaisonStreamDir)
			return os.IsNotExist(statErr0) && os.IsNotExist(statErr1) && os.IsNotExist(statErrL)
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("can delete an existing group with force=true", func() {
		dn0MeasureDir := filepath.Join(dataNode0Path, "measure", "data", groupName)
		dn1MeasureDir := filepath.Join(dataNode1Path, "measure", "data", groupName)
		liaisonMeasureDir := filepath.Join(liaisonNodePath, "measure", "data", groupName)

		ginkgo.By("Creating a group with measure schema and data")
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
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		ginkgo.By("Verifying group data directories exist on all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return statErr0 == nil && statErr1 == nil && statErrL == nil
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		ginkgo.By("Attempting to delete group without force flag should fail")
		gomega.Eventually(func() codes.Code {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: groupName,
			})
			if deleteErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(deleteErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gomega.Equal(codes.FailedPrecondition))

		ginkgo.By("Deleting group with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Verifying the group is eventually removed")
		gomega.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gomega.Equal(codes.NotFound))

		ginkgo.By("Verifying group data directories are removed from all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return os.IsNotExist(statErr0) && os.IsNotExist(statErr1) && os.IsNotExist(statErrL)
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("can recreate and delete a group with the same name after deletion", func() {
		dn0MeasureDir := filepath.Join(dataNode0Path, "measure", "data", groupName)
		dn1MeasureDir := filepath.Join(dataNode1Path, "measure", "data", groupName)
		liaisonMeasureDir := filepath.Join(liaisonNodePath, "measure", "data", groupName)

		ginkgo.By("Creating a group with measure schema and data")
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
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		ginkgo.By("Deleting the group with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Waiting for deletion task to reach COMPLETED")
		gomega.Eventually(func() databasev1.GroupDeletionTask_Phase {
			queryResp, queryErr := groupClient.Query(context.TODO(), &databasev1.GroupRegistryServiceQueryRequest{
				Group: groupName,
			})
			if queryErr != nil {
				return databasev1.GroupDeletionTask_PHASE_UNSPECIFIED
			}
			return queryResp.GetTask().GetCurrentPhase()
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gomega.Equal(databasev1.GroupDeletionTask_PHASE_COMPLETED))

		ginkgo.By("Recreating the same group after deletion")
		gomega.Eventually(func() error {
			_, createErr := groupClient.Create(context.TODO(), &databasev1.GroupRegistryServiceCreateRequest{
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
			return createErr
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		ginkgo.By("Verifying the recreated group data directories exist on all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return statErr0 == nil && statErr1 == nil && statErrL == nil
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		ginkgo.By("Deleting the recreated group with force=true")
		gomega.Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: groupName,
				Force: true,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(gomega.Succeed())

		ginkgo.By("Verifying the recreated group is eventually removed")
		gomega.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gomega.Equal(codes.NotFound))

		ginkgo.By("Verifying the recreated group data directories are removed from all nodes")
		gomega.Eventually(func() bool {
			_, statErr0 := os.Stat(dn0MeasureDir)
			_, statErr1 := os.Stat(dn1MeasureDir)
			_, statErrL := os.Stat(liaisonMeasureDir)
			return os.IsNotExist(statErr0) && os.IsNotExist(statErr1) && os.IsNotExist(statErrL)
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})
})

func createMeasureInGroup(regClient databasev1.MeasureRegistryServiceClient, group, measure string) {
	_, createErr := regClient.Create(context.TODO(), &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: measure, Group: group},
			Entity:   &databasev1.Entity{TagNames: []string{"id"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}},
			Fields: []*databasev1.FieldSpec{{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			}},
		},
	})
	gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
	time.Sleep(2 * time.Second)
}

func writeMeasureDataToGroup(writeClient measurev1.MeasureServiceClient, group, measure string, count int) {
	wc, wcErr := writeClient.Write(context.TODO())
	gomega.Expect(wcErr).ShouldNot(gomega.HaveOccurred())
	metadata := &commonv1.Metadata{Name: measure, Group: group}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < count; idx++ {
		sendErr := wc.Send(&measurev1.WriteRequest{
			Metadata: metadata,
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id_" + strconv.Itoa(idx)}},
					}},
				}},
				Fields: []*modelv1.FieldValue{{
					Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(idx * 100)}},
				}},
			},
			MessageId: uint64(time.Now().UnixNano() + int64(idx)),
		})
		gomega.Expect(sendErr).ShouldNot(gomega.HaveOccurred())
	}
	closeSendErr := wc.CloseSend()
	gomega.Expect(closeSendErr).ShouldNot(gomega.HaveOccurred())
	for {
		_, recvErr := wc.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gomega.Expect(recvErr).ShouldNot(gomega.HaveOccurred())
	}
}
