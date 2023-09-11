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

package schema

import (
	"fmt"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestSchema(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Schema Suite")
}

var (
	server   embeddedetcd.Server
	registry *etcdSchemaRegistry
)

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	ports, err := test.AllocateFreePorts(2)
	if err != nil {
		panic("fail to find free ports")
	}
	endpoints := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
	server, err = embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(randomTempDir()))
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	<-server.ReadyNotify()
	schemaRegistry, err := NewEtcdSchemaRegistry(ConfigureServerEndpoints(endpoints))
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	registry = schemaRegistry.(*etcdSchemaRegistry)
})

var _ = ginkgo.AfterSuite(func() {
	registry.Close()
	server.Close()
	<-server.StopNotify()
})
