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

package local_test

import (
	"context"
	"testing"
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
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesbackup "github.com/apache/skywalking-banyandb/test/cases/backup"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestBackup(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Backup Suite", ginkgo.Label(integration_standalone.Labels...))
}

var (
	connection *grpc.ClientConn
	dir        string
	deferFunc  func()
	goods      []gleak.Goroutine
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	var err error
	dir, _, err = test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var addr string
	addr, _, deferFunc = setup.ClosableStandalone(dir, ports)
	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(addr, now)

	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	casesbackup.SharedContext = helpers.BackupSharedContext{
		DataAddr:   string(address),
		Connection: connection,
		RootDir:    dir,
		FSType:     "local",
	}
	gClient := databasev1.NewGroupRegistryServiceClient(connection)
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
	pClient := databasev1.NewPropertyRegistryServiceClient(connection)
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
	client := propertyv1.NewPropertyServiceClient(connection)
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
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
}, func() {
	if deferFunc != nil {
		deferFunc()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
})
