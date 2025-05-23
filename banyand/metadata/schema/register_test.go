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

package schema_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = ginkgo.Describe("etcd_register", func() {
	var path string
	var defFn func()
	var endpoints, peers []string
	var goods []gleak.Goroutine
	var server embeddedetcd.Server
	var r schema.Registry
	const node string = "test"
	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Name: node,
			Kind: schema.KindNode,
		},
		Spec: &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: node,
			},
		},
	}
	ginkgo.BeforeEach(func() {
		var err error
		path, defFn, err = test.NewSpace()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		goods = gleak.Goroutines()
		ports, err := test.AllocateFreePorts(2)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		endpoints = []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
		peers = []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}
		server, err = embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener(endpoints, peers),
			embeddedetcd.RootDir(path),
			embeddedetcd.AutoCompactionMode("periodic"),
			embeddedetcd.AutoCompactionRetention("1h"),
			embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		<-server.ReadyNotify()
		r, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace("test"),
			schema.ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		gomega.Expect(r.Close()).ShouldNot(gomega.HaveOccurred())
		server.Close()
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		defFn()
	})

	ginkgo.It("should revoke the leaser", func() {
		gomega.Expect(r.Register(context.Background(), md, true)).ShouldNot(gomega.HaveOccurred())
		_, err := r.GetNode(context.Background(), node)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(r.Close()).ShouldNot(gomega.HaveOccurred())
		r, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace("test"),
			schema.ConfigureServerEndpoints(endpoints))
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		_, err = r.GetNode(context.Background(), node)
		gomega.Expect(err).Should(gomega.MatchError(schema.ErrGRPCResourceNotFound))
	})

	ginkgo.It("should register only once", func() {
		gomega.Expect(r.Register(context.Background(), md, false)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(r.Register(context.Background(), md, false)).Should(gomega.MatchError(schema.ErrGRPCAlreadyExists))
	})

	ginkgo.It("should reconnect", func() {
		gomega.Expect(r.Register(context.Background(), md, true)).ShouldNot(gomega.HaveOccurred())
		_, err := r.GetNode(context.Background(), node)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(server.Close()).ShouldNot(gomega.HaveOccurred())
		time.Sleep(1 * time.Second)
		os.RemoveAll(path)

		server, err = embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener(endpoints, peers),
			embeddedetcd.RootDir(path),
			embeddedetcd.AutoCompactionMode("periodic"),
			embeddedetcd.AutoCompactionRetention("1h"),
			embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		<-server.ReadyNotify()

		gomega.Eventually(func() error {
			_, err := r.GetNode(context.Background(), node)
			return err
		}, flags.EventuallyTimeout).ShouldNot(gomega.HaveOccurred())
	})
})
