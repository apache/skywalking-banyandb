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

package integration_setup_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const host = "127.0.0.1"

var _ = Describe("Node registration", func() {
	It("should register/unregister a liaison node successfully", func() {
		namespace := "liaison-test"
		nodeHost := "liaison-1"
		ports, err := test.AllocateFreePorts(2)
		Expect(err).NotTo(HaveOccurred())
		addr := fmt.Sprintf("%s:%d", host, ports[0])
		httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
		closeFn := setup.CMD("liaison",
			"--namespace", namespace,
			"--grpc-host="+host,
			fmt.Sprintf("--grpc-port=%d", ports[0]),
			"--http-host="+host,
			fmt.Sprintf("--http-port=%d", ports[1]),
			"--http-grpc-addr="+addr,
			"--etcd-endpoints", etcdEndpoint,
			"--node-host-provider", "flag",
			"--node-host", nodeHost)
		Eventually(helpers.HTTPHealthCheck(httpAddr), flags.EventuallyTimeout).Should(Succeed())
		Eventually(func() (map[string]*databasev1.Node, error) {
			return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(HaveLen(1))
		closeFn()
		Eventually(func() (map[string]*databasev1.Node, error) {
			return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(BeNil())
	})
	It("should register/unregister a data node successfully", func() {
		namespace := "data-test"
		nodeHost := "data-1"
		ports, err := test.AllocateFreePorts(1)
		Expect(err).NotTo(HaveOccurred())
		addr := fmt.Sprintf("%s:%d", host, ports[0])
		closeFn := setup.CMD("data",
			"--namespace", namespace,
			"--grpc-host="+host,
			fmt.Sprintf("--grpc-port=%d", ports[0]),
			"--etcd-endpoints", etcdEndpoint,
			"--node-host-provider", "flag",
			"--node-host", nodeHost)
		Eventually(
			helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(Succeed())
		Eventually(func() (map[string]*databasev1.Node, error) {
			return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(HaveLen(1))
		closeFn()
		Eventually(func() (map[string]*databasev1.Node, error) {
			return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(BeNil())
	})
})
