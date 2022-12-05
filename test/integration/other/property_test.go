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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient

	BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Common()
		Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(Succeed())
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		client = propertyv1.NewPropertyServiceClient(conn)
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
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
})

var _ = Describe("Property application", func() {
	var deferFn func()
	var conn *grpc.ClientConn
	var client propertyv1.PropertyServiceClient
	var md *propertyv1.Metadata

	BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Common()
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
	})
	It("lists properties", func() {
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
