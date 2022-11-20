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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

var _ = Describe("Registry", func() {
	var gracefulStop func()
	var conn *grpclib.ClientConn
	meta := &commonv1.Metadata{
		Group: "default",
	}
	BeforeEach(func() {
		gracefulStop = setupForRegistry()
		addr := "localhost:17912"
		Eventually(
			helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
			testflags.EventuallyTimeout).Should(Succeed())
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
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
		Expect(errStatus.Code()).To(Equal(codes.NotFound))
		existResp, err := client.Exist(context.TODO(), &databasev1.StreamRegistryServiceExistRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(existResp.HasGroup).To(BeTrue())
		Expect(existResp.HasStream).To(BeFalse())
		By("Creating a new stream")
		_, err = client.Create(context.TODO(), &databasev1.StreamRegistryServiceCreateRequest{Stream: getResp.GetStream()})
		Expect(err).ShouldNot(HaveOccurred())
		By("Verifying the new stream")
		getResp, err = client.Get(context.TODO(), &databasev1.StreamRegistryServiceGetRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(getResp).NotTo(BeNil())
		existResp, err = client.Exist(context.TODO(), &databasev1.StreamRegistryServiceExistRequest{
			Metadata: meta,
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(existResp.HasGroup).To(BeTrue())
		Expect(existResp.HasStream).To(BeTrue())
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
		Expect(errStatus.Code()).To(Equal(codes.NotFound))
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
		Expect(errStatus.Code()).To(Equal(codes.NotFound))
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

func setupForRegistry() func() {
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	Expect(err).NotTo(HaveOccurred())
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).NotTo(HaveOccurred())
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())

	tcp := grpc.NewServer(context.TODO(), pipeline, repo, metaSvc)
	preloadStreamSvc := &preloadStreamService{metaSvc: metaSvc}
	var flags []string
	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--metadata-root-path="+metaPath, "--etcd-listen-client-url="+listenClientURL,
		"--etcd-listen-peer-url="+listenPeerURL)
	deferFunc := test.SetUpModules(
		flags,
		repo,
		pipeline,
		metaSvc,
		preloadStreamSvc,
		tcp,
	)
	Eventually(
		helpers.HealthCheck("localhost:17912", 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(Succeed())
	return func() {
		deferFunc()
		metaDeferFunc()
	}
}

type preloadStreamService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-stream"
}

func (p *preloadStreamService) PreRun() error {
	return teststream.PreloadSchema(p.metaSvc.SchemaRegistry())
}
