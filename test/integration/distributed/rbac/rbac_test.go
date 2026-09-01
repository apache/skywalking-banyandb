// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package rbac_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const distributedRBACPolicy = `
users:
  - username: "rbac-admin"
    password: "admin-secret"
  - username: "rbac-reader"
    password: "reader-secret"
rbac:
  enabled: true
  bindings:
    - principal: "rbac-admin"
      role: "admin"
      groups: ["*"]
    - principal: "rbac-reader"
      role: "reader"
      groups: ["rbac-distributed-alpha"]
`

const (
	distributedAlpha   = "rbac-distributed-alpha"
	distributedBeta    = "rbac-distributed-beta"
	distributedMeasure = "service_cpm"
	alphaMarker        = "alpha-through-liaison-a"
	betaMarker         = "beta-through-liaison-a"
)

type distributedActor struct {
	username string
	password string
}

func (a distributedActor) context() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("username", a.username, "password", a.password))
}

var (
	distributedAdmin  = distributedActor{username: "rbac-admin", password: "admin-secret"}
	distributedReader = distributedActor{username: "rbac-reader", password: "reader-secret"}
)

func distributedGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

func distributedMeasureSchema(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: distributedMeasure},
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
	}
}

func distributedMarkerFrame(group, marker string, at time.Time, messageID uint64) *measurev1.WriteRequest {
	return &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: group, Name: distributedMeasure},
		MessageId: messageID,
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(at),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: marker}}}},
			}},
			Fields: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}}},
		},
	}
}

func distributedMarkerQuery(at time.Time, groups ...string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Groups: groups,
		Name:   distributedMeasure,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(at.Add(-time.Hour)),
			End:   timestamppb.New(at.Add(time.Hour)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"id"}}},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
	}
}

func distributedMarkers(response *measurev1.QueryResponse) []string {
	markers := make([]string, 0, len(response.GetDataPoints()))
	for _, dataPoint := range response.GetDataPoints() {
		for _, family := range dataPoint.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				markers = append(markers, tag.GetValue().GetStr().GetValue())
			}
		}
	}
	return markers
}

func sendDistributedFrames(
	actor distributedActor,
	client measurev1.MeasureServiceClient,
	frames ...*measurev1.WriteRequest,
) error {
	stream, openErr := client.Write(actor.context())
	if openErr != nil {
		return openErr
	}
	for _, frame := range frames {
		if sendErr := stream.Send(frame); sendErr != nil {
			if errors.Is(sendErr, io.EOF) {
				break
			}
			return sendErr
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return closeErr
	}
	for {
		_, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return nil
		}
		if receiveErr != nil {
			return receiveErr
		}
	}
}

var _ = g.Describe("rbac-data distributed public-path smoke", func() {
	var (
		closeCluster  func()
		groupsA       databasev1.GroupRegistryServiceClient
		groupsB       databasev1.GroupRegistryServiceClient
		measureSchema databasev1.MeasureRegistryServiceClient
		measuresA     measurev1.MeasureServiceClient
		measuresB     measurev1.MeasureServiceClient
		barrierB      schemav1.SchemaBarrierServiceClient
		baseTime      time.Time
	)

	g.BeforeEach(func() {
		baseTime = time.Now().Truncate(time.Millisecond)
		rootDir, releaseSpace, spaceErr := test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())

		policyPath := filepath.Join(rootDir, "security.yaml")
		gm.Expect(os.WriteFile(policyPath, []byte(distributedRBACPolicy), 0o600)).To(gm.Succeed())
		config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(rootDir))
		_, _, _, closeDataNode := setup.DataNodeWithAddrAndDir(config)
		liaisonA, _, closeLiaisonA := setup.LiaisonNodeWithHTTPAuth(
			config,
			distributedAdmin.username,
			distributedAdmin.password,
			"--auth-config-file="+policyPath,
		)
		liaisonB, _, closeLiaisonB := setup.LiaisonNodeWithHTTPAuth(
			config,
			distributedAdmin.username,
			distributedAdmin.password,
			"--auth-config-file="+policyPath,
		)
		connectionA, connectionAErr := grpclib.NewClient(liaisonA, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(connectionAErr).NotTo(gm.HaveOccurred())
		connectionB, connectionBErr := grpclib.NewClient(liaisonB, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(connectionBErr).NotTo(gm.HaveOccurred())

		groupsA = databasev1.NewGroupRegistryServiceClient(connectionA)
		groupsB = databasev1.NewGroupRegistryServiceClient(connectionB)
		measureSchema = databasev1.NewMeasureRegistryServiceClient(connectionA)
		measuresA = measurev1.NewMeasureServiceClient(connectionA)
		measuresB = measurev1.NewMeasureServiceClient(connectionB)
		barrierB = schemav1.NewSchemaBarrierServiceClient(connectionB)
		closeCluster = func() {
			_ = connectionB.Close()
			_ = connectionA.Close()
			closeLiaisonB()
			closeLiaisonA()
			closeDataNode()
			releaseSpace()
		}
	})

	g.AfterEach(func() {
		closeCluster()
	})

	g.It("keeps public group decisions consistent while cross-liaison data flow remains healthy", func() {
		for _, group := range []string{distributedAlpha, distributedBeta} {
			_, createErr := groupsA.Create(distributedAdmin.context(), &databasev1.GroupRegistryServiceCreateRequest{
				Group: distributedGroup(group),
			})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(), "the administrator must create group %s", group)
			_, schemaErr := measureSchema.Create(distributedAdmin.context(), &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: distributedMeasureSchema(group),
			})
			gm.Expect(schemaErr).NotTo(gm.HaveOccurred(), "the administrator must create %s on %s", distributedMeasure, group)
		}

		_, barrierErr := barrierB.AwaitSchemaApplied(distributedAdmin.context(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: []*schemav1.SchemaKey{
				{Kind: "group", Name: distributedAlpha},
				{Kind: "group", Name: distributedBeta},
				{Kind: "measure", Group: distributedAlpha, Name: distributedMeasure},
				{Kind: "measure", Group: distributedBeta, Name: distributedMeasure},
			},
			MinRevisions: []int64{0, 0, 0, 0},
		})
		gm.Expect(barrierErr).NotTo(gm.HaveOccurred(), "the distributed fixture schema must converge before public data calls")

		gm.Expect(sendDistributedFrames(distributedAdmin, measuresA,
			distributedMarkerFrame(distributedAlpha, alphaMarker, baseTime, 1),
			distributedMarkerFrame(distributedBeta, betaMarker, baseTime, 2),
		)).To(gm.Succeed(), "the administrator must seed both groups through liaison A")

		gm.Eventually(func() []string {
			response, queryErr := measuresB.Query(distributedAdmin.context(), distributedMarkerQuery(baseTime, distributedAlpha, distributedBeta))
			if queryErr != nil {
				return nil
			}
			return distributedMarkers(response)
		}, flags.EventuallyTimeout).Should(gm.ConsistOf(alphaMarker, betaMarker),
			"data written through liaison A must be readable through liaison B")

		for _, endpoint := range []struct {
			measures measurev1.MeasureServiceClient
			name     string
		}{
			{name: "liaison A", measures: measuresA},
			{name: "liaison B", measures: measuresB},
		} {
			alphaResponse, alphaErr := endpoint.measures.Query(distributedReader.context(), distributedMarkerQuery(baseTime, distributedAlpha))
			gm.Expect(alphaErr).NotTo(gm.HaveOccurred(), "%s must serve the reader's alpha group", endpoint.name)
			gm.Expect(distributedMarkers(alphaResponse)).To(gm.ConsistOf(alphaMarker))

			_, betaErr := endpoint.measures.Query(distributedReader.context(), distributedMarkerQuery(baseTime, distributedBeta))
			gm.Expect(status.Code(betaErr)).To(gm.Equal(codes.PermissionDenied), "%s must refuse beta", endpoint.name)

			mixedResponse, mixedErr := endpoint.measures.Query(
				distributedReader.context(),
				distributedMarkerQuery(baseTime, distributedAlpha, distributedBeta),
			)
			gm.Expect(status.Code(mixedErr)).To(gm.Equal(codes.PermissionDenied), "%s must refuse a mixed request whole", endpoint.name)
			gm.Expect(distributedMarkers(mixedResponse)).To(gm.BeEmpty(), "%s must return no partial result", endpoint.name)
		}

		listed, listErr := groupsB.List(distributedReader.context(), &databasev1.GroupRegistryServiceListRequest{})
		gm.Expect(listErr).NotTo(gm.HaveOccurred())
		groupNames := make([]string, 0, len(listed.GetGroup()))
		for _, group := range listed.GetGroup() {
			groupNames = append(groupNames, group.GetMetadata().GetName())
		}
		gm.Expect(groupNames).To(gm.ContainElement(distributedAlpha))
		gm.Expect(groupNames).NotTo(gm.ContainElement(distributedBeta))
	})
})
