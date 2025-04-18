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

package backup_test

import (
	"context"
	"fmt"
	"testing"
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
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesbackup "github.com/apache/skywalking-banyandb/test/cases/backup"
	"github.com/apache/skywalking-banyandb/test/integration/dockertesthelper"
)

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Backup Suite")
}

var (
	connection *grpc.ClientConn
	dir        string
	deferFunc  func()
	goods      []gleak.Goroutine
	dataAddr   string
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	goods = gleak.Goroutines()
	By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	var spaceDef func()
	dir, spaceDef, err = test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(dir))
	Expect(err).ShouldNot(HaveOccurred())
	<-server.ReadyNotify()
	By("Loading schema")
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(
		schema.Namespace(metadata.DefaultNamespace),
		schema.ConfigureServerEndpoints([]string{ep}),
	)
	Expect(err).NotTo(HaveOccurred())
	defer schemaRegistry.Close()
	ctx := context.Background()
	test_stream.PreloadSchema(ctx, schemaRegistry)
	test_measure.PreloadSchema(ctx, schemaRegistry)
	By("Starting data node 0")
	var closeDataNode0 func()
	dataAddr, dir, closeDataNode0 = setup.DataNodeWithAddrAndDir(ep)
	By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)
	By("Initializing test cases")
	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(liaisonAddr, now)
	conn, err := grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	defer conn.Close()
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
	Expect(err).NotTo(HaveOccurred())
	client := propertyv1.NewPropertyServiceClient(conn)
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
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Created).To(BeTrue())
	Expect(resp.TagsNum).To(Equal(uint32(2)))
	deferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}
	err = dockertesthelper.InitMinIOContainer()
	Expect(err).NotTo(HaveOccurred())
	return []byte(dataAddr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	casesbackup.SharedContext = helpers.BackupSharedContext{
		DataAddr:   dataAddr,
		Connection: connection,
		RootDir:    dir,
	}
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
	dockertesthelper.CloseMinioContainer()
}, func() {
	if deferFunc != nil {
		deferFunc()
	}
	Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
})
