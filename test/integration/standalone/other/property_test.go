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

package other

import (
	"context"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = g.Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient
	var goods []gleak.Goroutine

	g.BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Standalone(testConfig)
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gClient := databasev1.NewGroupRegistryServiceClient(conn)
		_, err = gClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "g"},
				Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		pClient := databasev1.NewPropertyRegistryServiceClient(conn)
		_, err = pClient.Create(context.Background(), &databasev1.PropertyRegistryServiceCreateRequest{
			Property: &databasev1.Property{
				Metadata: &commonv1.Metadata{
					Group: "g",
					Name:  "p",
				},
				Tags: []*databasev1.TagSpec{
					{Name: "t1", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "t2", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		client = propertyv1.NewPropertyServiceClient(conn)
		goods = gleak.Goroutines()
	})
	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gm.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	})
	g.It("applies properties", func() {
		md := &commonv1.Metadata{
			Name:  "p",
			Group: "g",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeTrue())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Ids:    []string{"1"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		property := got.Properties[0]
		gm.Expect(property.Tags).To(gm.Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
		}))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeFalse())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		got, err = client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Ids:    []string{"1"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		property = got.Properties[0]
		gm.Expect(property.Tags).To(gm.Equal([]*modelv1.Tag{
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
		}))
	})
	g.It("applies properties with new tags", func() {
		md := &commonv1.Metadata{
			Name:  "p",
			Group: "g",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeTrue())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(1)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeFalse())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Ids:    []string{"1"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		property := got.Properties[0]
		gm.Expect(property.Tags).To(gm.Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
		}))
	})
	g.It("applies null tag", func() {
		md := &commonv1.Metadata{
			Name:  "p",
			Group: "g",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeTrue())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeFalse())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Ids:    []string{"1"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		property := got.Properties[0]
		gm.Expect(property.Tags).To(gm.Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
		}))
	})
})

var _ = g.Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient
	var md *commonv1.Metadata

	g.BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Standalone(testConfig)
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gClient := databasev1.NewGroupRegistryServiceClient(conn)
		_, err = gClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "g"},
				Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		pClient := databasev1.NewPropertyRegistryServiceClient(conn)
		_, err = pClient.Create(context.Background(), &databasev1.PropertyRegistryServiceCreateRequest{
			Property: &databasev1.Property{
				Metadata: &commonv1.Metadata{
					Group: "g",
					Name:  "p",
				},
				Tags: []*databasev1.TagSpec{
					{Name: "t1", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "t2", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		client = propertyv1.NewPropertyServiceClient(conn)
		md = &commonv1.Metadata{
			Name:  "p",
			Group: "g",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Id:       "1",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeTrue())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: &commonv1.Metadata{
				Name:  "p",
				Group: "g",
			},
			Id: "2",
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v21"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.Created).To(gm.BeTrue())
		gm.Expect(resp.TagsNum).To(gm.Equal(uint32(2)))
	})
	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
	})
	g.It("lists properties in a group", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(2))
	})
	g.It("lists properties under a name", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(2))
	})
	g.It("filters properties by tag", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Criteria: &modelv1.Criteria{
				Exp: &modelv1.Criteria_Condition{
					Condition: &modelv1.Condition{
						Name: "t1",
						Op:   modelv1.Condition_BINARY_OP_EQ,
						Value: &modelv1.TagValue{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{Value: "v1"},
							},
						},
					},
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		gm.Expect(got.Properties[0].Tags).To(gm.HaveLen(2))
		got, err = client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Criteria: &modelv1.Criteria{
				Exp: &modelv1.Criteria_Condition{
					Condition: &modelv1.Condition{
						Name: "t1",
						Op:   modelv1.Condition_BINARY_OP_EQ,
						Value: &modelv1.TagValue{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{Value: "foo"},
							},
						},
					},
				},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(0))
	})
	g.It("projects a tag", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:        []string{"g"},
			Name:          "p",
			Ids:           []string{"1"},
			TagProjection: []string{"t1"},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		gm.Expect(got.Properties[0].Tags).To(gm.HaveLen(1))
	})
	g.It("limits result size", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Limit:  1,
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
	})
	g.It("traces the query", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
			Criteria: &modelv1.Criteria{
				Exp: &modelv1.Criteria_Condition{
					Condition: &modelv1.Condition{
						Name: "t1",
						Op:   modelv1.Condition_BINARY_OP_EQ,
						Value: &modelv1.TagValue{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{Value: "v1"},
							},
						},
					},
				},
			},
			TagProjection: []string{"t1"},
			Limit:         1,
			Trace:         true,
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Properties).To(gm.HaveLen(1))
		gm.Expect(got.Properties[0].Tags).To(gm.HaveLen(1))
		gm.Expect(got.Trace).NotTo(gm.BeNil())
	})
	g.It("deletes a property", func() {
		got, err := client.Delete(context.Background(), &propertyv1.DeleteRequest{
			Name:  "p",
			Group: "g",
			Id:    "1",
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Deleted).To(gm.BeTrue())
		qGot, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(qGot.Properties).To(gm.HaveLen(1))
	})
	g.It("deletes all properties", func() {
		got, err := client.Delete(context.Background(), &propertyv1.DeleteRequest{
			Name:  "p",
			Group: "g",
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(got.Deleted).To(gm.BeTrue())
		qGot, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
			Name:   "p",
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(qGot.Properties).To(gm.HaveLen(0))
	})
})
