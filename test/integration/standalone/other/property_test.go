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

package integration_other_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient
	var goods []gleak.Goroutine

	BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Standalone()
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
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
		Expect(err).NotTo(HaveOccurred())
		client = propertyv1.NewPropertyServiceClient(conn)
		goods = gleak.Goroutines()
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	})
	It("applies properties", func() {
		md := &propertyv1.Metadata{
			Container: &commonv1.Metadata{
				Name:  "p",
				Group: "g",
			},
			Id: "1",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
			Ids:       []string{"1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		property := got.Properties[0]
		Expect(property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
		}))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeFalse())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		got, err = client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
			Ids:       []string{"1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		property = got.Properties[0]
		Expect(property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
		}))
	})
	It("applies properties with new tags", func() {
		md := &propertyv1.Metadata{
			Container: &commonv1.Metadata{
				Name:  "p",
				Group: "g",
			},
			Id: "1",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(1)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeFalse())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
			Ids:       []string{"1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		property := got.Properties[0]
		Expect(property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
		}))
	})
	It("applies null tag", func() {
		md := &propertyv1.Metadata{
			Container: &commonv1.Metadata{
				Name:  "p",
				Group: "g",
			},
			Id: "1",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeFalse())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
			Ids:       []string{"1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		property := got.Properties[0]
		Expect(property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
		}))
	})
})

var _ = Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient
	var md *propertyv1.Metadata

	BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Standalone()
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
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
		Expect(err).NotTo(HaveOccurred())
		client = propertyv1.NewPropertyServiceClient(conn)
		md = &propertyv1.Metadata{
			Container: &commonv1.Metadata{
				Name:  "p",
				Group: "g",
			},
			Id: "1",
		}
		resp, err := client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: &propertyv1.Metadata{
				Container: &commonv1.Metadata{
					Name:  "p",
					Group: "g",
				},
				Id: "2",
			},
			Tags: []*modelv1.Tag{
				{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v21"}}}},
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
	})
	It("lists properties in a group", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups: []string{"g"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(2))
	})
	It("lists properties in a container", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(2))
	})
	It("filters properties by tag", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
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
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		Expect(got.Properties[0].Tags).To(HaveLen(2))
		got, err = client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
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
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(0))
	})
	It("projects a tag", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:        []string{"g"},
			Container:     "p",
			Ids:           []string{"1"},
			TagProjection: []string{"t1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		Expect(got.Properties[0].Tags).To(HaveLen(1))
	})
	It("limits result size", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
			Limit:     1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
	})
	It("traces the query", func() {
		got, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
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
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Properties).To(HaveLen(1))
		Expect(got.Properties[0].Tags).To(HaveLen(1))
		Expect(got.Trace).NotTo(BeNil())
	})
	It("deletes a property", func() {
		got, err := client.Delete(context.Background(), &propertyv1.DeleteRequest{
			Container: "p",
			Group:     "g",
			Id:        "1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Deleted).To(BeTrue())
		qGot, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(qGot.Properties).To(HaveLen(1))
	})
	It("deletes all properties", func() {
		got, err := client.Delete(context.Background(), &propertyv1.DeleteRequest{
			Container: "p",
			Group:     "g",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Deleted).To(BeTrue())
		qGot, err := client.Query(context.Background(), &propertyv1.QueryRequest{
			Groups:    []string{"g"},
			Container: "p",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(qGot.Properties).To(HaveLen(0))
	})
})
