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
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
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
		gClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: "g"}}})
		client = propertyv1.NewPropertyServiceClient(conn)
		goods = gleak.Goroutines()
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
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
		got, err := client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Property.Tags).To(Equal([]*modelv1.Tag{
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
		Expect(resp.TagsNum).To(Equal(uint32(1)))
		got, err = client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
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
		got, err := client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Property.Tags).To(Equal([]*modelv1.Tag{
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
		got, err := client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
		}))
	})
	It("applies a property with TTL", func() {
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
			Ttl: "1h",
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		Expect(resp.LeaseId).To(BeNumerically(">", 0))
		got, err := client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Property.Tags).To(Equal([]*modelv1.Tag{
			{Key: "t1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v1"}}}},
			{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v2"}}}},
		}))
		Expect(got.Property.GetTtl()).To(Equal("1h"))
		Expect(got.Property.GetLeaseId()).To(Equal(resp.LeaseId))
		resp, err = client.Apply(context.Background(), &propertyv1.ApplyRequest{Property: &propertyv1.Property{
			Metadata: md,
			Tags: []*modelv1.Tag{
				{Key: "t2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v22"}}}},
			},
			Ttl: "1s",
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeFalse())
		Expect(resp.TagsNum).To(Equal(uint32(1)))
		Eventually(func() error {
			_, err := client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
			return err
		}, flags.EventuallyTimeout).Should(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})
	It("keeps alive", func() {
		_, err := client.KeepAlive(context.Background(), &propertyv1.KeepAliveRequest{LeaseId: 0})
		Expect(err).Should(MatchError("rpc error: code = Unknown desc = etcdserver: requested lease not found"))
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
			Ttl: "30m",
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Created).To(BeTrue())
		Expect(resp.TagsNum).To(Equal(uint32(2)))
		Expect(resp.LeaseId).To(BeNumerically(">", 0))
		_, err = client.KeepAlive(context.Background(), &propertyv1.KeepAliveRequest{LeaseId: resp.LeaseId})
		Expect(err).NotTo(HaveOccurred())
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
		gClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: "g"}}})
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
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
	})
	It("lists properties in a group", func() {
		got, err := client.List(context.Background(), &propertyv1.ListRequest{Container: &commonv1.Metadata{
			Group: "g",
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(got.Property)).To(Equal(1))
	})
	It("lists properties in a container", func() {
		got, err := client.List(context.Background(), &propertyv1.ListRequest{Container: md.Container})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(got.Property)).To(Equal(1))
	})
	It("deletes properties", func() {
		got, err := client.Delete(context.Background(), &propertyv1.DeleteRequest{Metadata: md})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Deleted).To(BeTrue())
		_, err = client.Get(context.Background(), &propertyv1.GetRequest{Metadata: md})
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})
})
