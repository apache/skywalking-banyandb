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
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = ginkgo.Describe("etcd_register", func() {
	var endpoints []string
	var goods []gleak.Goroutine
	var server embeddedetcd.Server
	var r *etcdSchemaRegistry
	md := Metadata{
		TypeMeta: TypeMeta{
			Name: "test",
			Kind: KindNode,
		},
		Spec: &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: "test",
			},
		},
	}
	ginkgo.BeforeEach(func() {
		goods = gleak.Goroutines()
		ports, err := test.AllocateFreePorts(2)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		endpoints = []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
		server, err = embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
			embeddedetcd.RootDir(randomTempDir()))
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		<-server.ReadyNotify()
		schemaRegistry, err := NewEtcdSchemaRegistry(
			Namespace("test"),
			ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		r = schemaRegistry.(*etcdSchemaRegistry)
	})
	ginkgo.AfterEach(func() {
		gomega.Expect(r.Close()).ShouldNot(gomega.HaveOccurred())
		server.Close()
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should revoke the leaser", func() {
		gomega.Expect(r.register(context.Background(), md, true)).ShouldNot(gomega.HaveOccurred())
		k, err := md.key()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(r.get(context.Background(), k, &databasev1.Node{})).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(r.Close()).ShouldNot(gomega.HaveOccurred())
		schemaRegistry, err := NewEtcdSchemaRegistry(
			Namespace("test"),
			ConfigureServerEndpoints(endpoints))
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		r = schemaRegistry.(*etcdSchemaRegistry)
		gomega.Expect(r.get(context.Background(), k, &databasev1.Node{})).Should(gomega.MatchError(ErrGRPCResourceNotFound))
	})

	ginkgo.It("should register only once", func() {
		gomega.Expect(r.register(context.Background(), md, false)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(r.register(context.Background(), md, false)).Should(gomega.MatchError(errGRPCAlreadyExists))
	})
})
