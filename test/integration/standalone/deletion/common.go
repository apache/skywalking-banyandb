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

// Package deletion contains integration tests for standalone group deletion.
package deletion

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	grpclib "google.golang.org/grpc"
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
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// SetupFunc is provided by sub-packages to create a standalone server for each test.
// It returns the server address, data path, and a cleanup function.
var SetupFunc func() (addr string, dataPath string, closeFn func())

// PropertySetup creates a standalone server with property-based configuration.
func PropertySetup() (string, string, func()) {
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gm.Expect(tmpErr).NotTo(gm.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	path, pathDeferFn, spaceErr := test.NewSpace()
	gm.Expect(spaceErr).NotTo(gm.HaveOccurred())
	ports, portsErr := test.AllocateFreePorts(5)
	gm.Expect(portsErr).NotTo(gm.HaveOccurred())
	addr, _, serverCloseFn := setup.ClosableStandalone(config, path, ports)
	return addr, path, func() {
		serverCloseFn()
		pathDeferFn()
		tmpDirCleanup()
	}
}

var _ = g.BeforeSuite(func() {
	gm.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gm.Succeed())
})

const (
	groupName            = "test_group"
	nonexistentGroupName = "nonexistent_test_group"
	emptyGroupName       = "empty_test_group"
	measureName          = "test_measure"
)

var _ = g.Describe("GroupDeletion", func() {
	var goods []gleak.Goroutine
	var conn *grpclib.ClientConn
	var deferFn func()
	var dataPath string
	var groupClient databasev1.GroupRegistryServiceClient
	var measureRegClient databasev1.MeasureRegistryServiceClient
	var measureWriteClient measurev1.MeasureServiceClient

	g.BeforeEach(func() {
		goods = gleak.Goroutines()
		var addr string
		addr, dataPath, deferFn = SetupFunc()
		var connErr error
		conn, connErr = grpchelper.Conn(addr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(connErr).NotTo(gm.HaveOccurred())
		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
		measureRegClient = databasev1.NewMeasureRegistryServiceClient(conn)
		measureWriteClient = measurev1.NewMeasureServiceClient(conn)
	})

	g.AfterEach(func() {
		_ = conn.Close()
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	g.It("returns NotFound when deleting a nonexistent group", func() {
		_, err := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: nonexistentGroupName,
		})
		gm.Expect(err).Should(gm.HaveOccurred())
		errStatus, ok := status.FromError(err)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(errStatus.Code()).To(gm.Equal(codes.NotFound))
	})

	g.It("can delete an empty group without force flag", func() {
		streamDir := filepath.Join(dataPath, "stream", "data", emptyGroupName)
		g.By("Creating a new empty group")
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
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying the group exists")
		getResp, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: emptyGroupName})
		gm.Expect(getErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(getResp.GetGroup().GetMetadata().GetName()).To(gm.Equal(emptyGroupName))

		g.By("Verifying group data directory exists on disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(streamDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Deleting the empty group without force")
		gm.Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: emptyGroupName,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		g.By("Verifying the group is eventually removed")
		gm.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: emptyGroupName})
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
		measureDir := filepath.Join(dataPath, "measure", "data", groupName)

		g.By("Creating a group with measure schema and data")
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
		gm.Expect(err).ShouldNot(gm.HaveOccurred())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		g.By("Verifying group data directory exists on disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Attempting to delete group without force flag should fail")
		gm.Eventually(func() codes.Code {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: groupName,
			})
			if deleteErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(deleteErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gm.Equal(codes.FailedPrecondition))

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

	g.It("can recreate and delete a group with the same name after deletion", func() {
		measureDir := filepath.Join(dataPath, "measure", "data", groupName)

		g.By("Creating a group with measure schema and data")
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
		gm.Expect(err).ShouldNot(gm.HaveOccurred())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		g.By("Deleting the group with force=true")
		_, err = groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
			Force: true,
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Waiting for deletion task to reach COMPLETED")
		gm.Eventually(func() databasev1.GroupDeletionTask_Phase {
			queryResp, queryErr := groupClient.Query(context.TODO(), &databasev1.GroupRegistryServiceQueryRequest{
				Group: groupName,
			})
			if queryErr != nil {
				return databasev1.GroupDeletionTask_PHASE_UNSPECIFIED
			}
			return queryResp.GetTask().GetCurrentPhase()
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Equal(databasev1.GroupDeletionTask_PHASE_COMPLETED))

		g.By("Recreating the same group after deletion")
		gm.Eventually(func() error {
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
		}, flags.EventuallyTimeout).Should(gm.Succeed())
		createMeasureInGroup(measureRegClient, groupName, measureName)
		writeMeasureDataToGroup(measureWriteClient, groupName, measureName, 5)

		g.By("Verifying the recreated group data directory exists on disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return statErr == nil
		}, flags.EventuallyTimeout).Should(gm.BeTrue())

		g.By("Deleting the recreated group with force=true")
		gm.Eventually(func() error {
			_, deleteErr := groupClient.Delete(context.TODO(), &databasev1.GroupRegistryServiceDeleteRequest{
				Group: groupName,
				Force: true,
			})
			return deleteErr
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		g.By("Verifying the recreated group is eventually removed")
		gm.Eventually(func() codes.Code {
			_, getErr := groupClient.Get(context.TODO(), &databasev1.GroupRegistryServiceGetRequest{Group: groupName})
			if getErr == nil {
				return codes.OK
			}
			st, _ := status.FromError(getErr)
			return st.Code()
		}, flags.EventuallyTimeout).Should(gm.Equal(codes.NotFound))

		g.By("Verifying the recreated group data directory is removed from disk")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(measureDir)
			return os.IsNotExist(statErr)
		}, flags.EventuallyTimeout).Should(gm.BeTrue())
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
	gm.Expect(createErr).ShouldNot(gm.HaveOccurred())
	time.Sleep(2 * time.Second)
}

func writeMeasureDataToGroup(writeClient measurev1.MeasureServiceClient, group, measure string, count int) {
	wc, wcErr := writeClient.Write(context.TODO())
	gm.Expect(wcErr).ShouldNot(gm.HaveOccurred())
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
		gm.Expect(sendErr).ShouldNot(gm.HaveOccurred())
	}
	closeSendErr := wc.CloseSend()
	gm.Expect(closeSendErr).ShouldNot(gm.HaveOccurred())
	for {
		_, recvErr := wc.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gm.Expect(recvErr).ShouldNot(gm.HaveOccurred())
	}
}
