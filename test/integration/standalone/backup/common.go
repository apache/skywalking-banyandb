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

// Package backup Conduct backup and recovery tests for different storage types in standalone mode.
package backup

import (
	"context"
	"os"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
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

// CommonTestVars is the common test variables.
type CommonTestVars struct {
	DeferFunc  func()
	Connection *grpc.ClientConn
	FS         remote.FS
	Dir        string
	DestDir    string
	Goods      []gleak.Goroutine
}

// InitStandaloneEnv initializes the standalone environment.
func InitStandaloneEnv() (*CommonTestVars, string, error) {
	var vars CommonTestVars
	var err error

	vars.Goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())

	vars.Dir, _, err = test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	vars.DestDir, err = os.MkdirTemp("", "backup-restore-dest")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ports, err := test.AllocateFreePorts(4)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var addr string
	addr, _, vars.DeferFunc = setup.ClosableStandalone(vars.Dir, ports)

	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(addr, now)

	return &vars, addr, nil
}

// SetupConnection sets up the connection to the server.
func SetupConnection(vars *CommonTestVars, addr string, destURL string, s3Args []string) error {
	var err error
	vars.Connection, err = grpchelper.Conn(addr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	casesbackup.SharedContext = helpers.BackupSharedContext{
		DataAddr:   addr,
		Connection: vars.Connection,
		RootDir:    vars.Dir,
		DestDir:    vars.DestDir,
		DestURL:    destURL,
		FS:         vars.FS,
		S3Args:     s3Args,
	}

	return initializeTestData(vars.Connection)
}

func initializeTestData(conn *grpc.ClientConn) error {
	gClient := databasev1.NewGroupRegistryServiceClient(conn)
	_, err := gClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "g"},
			Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
			},
		},
	})
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}

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
	if err != nil {
		return err
	}

	gomega.Expect(resp.Created).To(gomega.BeTrue())
	gomega.Expect(resp.TagsNum).To(gomega.Equal(uint32(2)))

	return nil
}

// TeardownSuite cleans up the test suite.
func TeardownSuite(vars *CommonTestVars) {
	if vars.DeferFunc != nil {
		vars.DeferFunc()
	}
	gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(vars.Goods))
	gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
}

// GetTestLabels returns the test labels.
func GetTestLabels() []string {
	return integration_standalone.Labels
}
