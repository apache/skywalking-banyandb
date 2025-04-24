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

// Package backup and recovery tests for different storage types in distributed	mode.
package backup

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
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
)

// CommonTestVars holds common test variables.
type CommonTestVars struct {
	DeferFunc  func()
	Dir        string
	DestDir    string
	DataAddr   string
	FS         remote.FS
	Connection *grpc.ClientConn
	Goods      []gleak.Goroutine
}

// InitializeTestSuite initializes the test suite by setting up the necessary components.
func InitializeTestSuite() (*CommonTestVars, error) {
	var vars CommonTestVars
	var err error

	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	vars.Goods = gleak.Goroutines()
	ginkgo.By("Starting etcd server")
	ports, err := test.AllocateFreePorts(2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var spaceDef func()
	vars.Dir, spaceDef, err = test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	vars.DestDir, err = os.MkdirTemp("", "backup-restore-dest")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(vars.Dir))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	<-server.ReadyNotify()
	ginkgo.By("Loading schema")
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(
		schema.Namespace(metadata.DefaultNamespace),
		schema.ConfigureServerEndpoints([]string{ep}),
	)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer schemaRegistry.Close()
	ctx := context.Background()
	err = test_stream.PreloadSchema(ctx, schemaRegistry)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	err = test_measure.PreloadSchema(ctx, schemaRegistry)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Starting data node 0")
	var closeDataNode0 func()
	vars.DataAddr, vars.Dir, closeDataNode0 = setup.DataNodeWithAddrAndDir(ep)
	ginkgo.By("Starting liaison node")
	liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)
	ginkgo.By("Initializing test cases")
	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(liaisonAddr, now)
	conn, err := grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
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
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.Created).To(gomega.BeTrue())
	gomega.Expect(resp.TagsNum).To(gomega.Equal(uint32(2)))

	vars.DeferFunc = func() {
		closerLiaisonNode()
		closeDataNode0()
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}

	return &vars, nil
}

// SetupClientConnection sets up the connection to the server.
func SetupClientConnection(address string) (*grpc.ClientConn, error) {
	return grpchelper.Conn(address, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// SetupSharedContext sets up the context for testing.
func SetupSharedContext(vars *CommonTestVars, destURL string, s3Args []string) {
	casesbackup.SharedContext = helpers.BackupSharedContext{
		DataAddr:   vars.DataAddr,
		Connection: vars.Connection,
		RootDir:    vars.Dir,
		DestDir:    vars.DestDir,
		DestURL:    destURL,
		FS:         vars.FS,
		S3Args:     s3Args,
	}
}

// TeardownSuite cleans up the test suite.
func TeardownSuite(vars *CommonTestVars) {
	if vars.DeferFunc != nil {
		vars.DeferFunc()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(vars.Goods))
	gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
}
