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

package grpc_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var _ = Describe("Registry", func() {
	var gracefulStop func()
	var conn *grpc.ClientConn
	meta := &commonv1.Metadata{
		Group: "default",
	}
	BeforeEach(func() {
		gracefulStop = setup(nil)
		var err error
		conn, err = grpc.Dial("localhost:17912", grpc.WithInsecure())
		Expect(err).NotTo(HaveOccurred())
	})
	It("manages the stream", func() {
		client := databasev1.NewStreamRegistryServiceClient(conn)
		Expect(client).NotTo(BeNil())
		meta.Name = "sw"
		getResp, err := client.Get(context.TODO(), &databasev1.StreamRegistryServiceGetRequest{Metadata: meta})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
		By("Cleanup the registry")
		deleteResp, err := client.Delete(context.TODO(), &databasev1.StreamRegistryServiceDeleteRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deleteResp).NotTo(BeNil())
		Expect(deleteResp.GetDeleted()).To(BeTrue())
		By("Verifying the registry empty")
		_, err = client.Get(context.TODO(), &databasev1.StreamRegistryServiceGetRequest{
			Metadata: meta,
		})
		errStatus, _ := status.FromError(err)
		Expect(errStatus.Message()).To(Equal(schema.ErrEntityNotFound.Error()))
		By("Creating a new stream")
		_, err = client.Create(context.TODO(), &databasev1.StreamRegistryServiceCreateRequest{Stream: getResp.GetStream()})
		Expect(err).ShouldNot(HaveOccurred())
		By("Verifying the new stream")
		getResp, err = client.Get(context.TODO(), &databasev1.StreamRegistryServiceGetRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
	})
	It("manages the index-rule-binding", func() {
		client := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
		Expect(client).NotTo(BeNil())
		meta.Name = "sw-index-rule-binding"
		getResp, err := client.Get(context.TODO(), &databasev1.IndexRuleBindingRegistryServiceGetRequest{Metadata: meta})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
		By("Cleanup the registry")
		deleteResp, err := client.Delete(context.TODO(), &databasev1.IndexRuleBindingRegistryServiceDeleteRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deleteResp).NotTo(BeNil())
		Expect(deleteResp.GetDeleted()).To(BeTrue())
		By("Verifying the registry empty")
		_, err = client.Get(context.TODO(), &databasev1.IndexRuleBindingRegistryServiceGetRequest{
			Metadata: meta,
		})
		errStatus, _ := status.FromError(err)
		Expect(errStatus.Message()).To(Equal(schema.ErrEntityNotFound.Error()))
		By("Creating a new index-rule-binding")
		_, err = client.Create(context.TODO(), &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: getResp.GetIndexRuleBinding()})
		Expect(err).ShouldNot(HaveOccurred())
		By("Verifying the new index-rule-binding")
		getResp, err = client.Get(context.TODO(), &databasev1.IndexRuleBindingRegistryServiceGetRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
	})
	It("manages the index-rule", func() {
		client := databasev1.NewIndexRuleRegistryServiceClient(conn)
		Expect(client).NotTo(BeNil())
		meta.Name = "db.instance"
		getResp, err := client.Get(context.TODO(), &databasev1.IndexRuleRegistryServiceGetRequest{Metadata: meta})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
		By("Cleanup the registry")
		deleteResp, err := client.Delete(context.TODO(), &databasev1.IndexRuleRegistryServiceDeleteRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(deleteResp).NotTo(BeNil())
		Expect(deleteResp.GetDeleted()).To(BeTrue())
		By("Verifying the registry empty")
		_, err = client.Get(context.TODO(), &databasev1.IndexRuleRegistryServiceGetRequest{
			Metadata: meta,
		})
		errStatus, _ := status.FromError(err)
		Expect(errStatus.Message()).To(Equal(schema.ErrEntityNotFound.Error()))
		By("Creating a new index-rule")
		_, err = client.Create(context.TODO(), &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: getResp.GetIndexRule()})
		Expect(err).ShouldNot(HaveOccurred())
		By("Verifying the new index-rule")
		getResp, err = client.Get(context.TODO(), &databasev1.IndexRuleRegistryServiceGetRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
	})
	AfterEach(func() {
		_ = conn.Close()
		gracefulStop()
	})
})
