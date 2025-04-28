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

package embeddedserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestDefragment(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Defragment Suite")
}

var _ = ginkgo.Describe("Defragment", func() {
	var (
		etcdClient *clientv3.Client
		etcdServer embeddedetcd.Server
		path       string
		defFn      func()
		err        error
		endpoints  []string
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: "debug",
		})).To(gomega.Succeed())

		path, defFn, err = test.NewSpace()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ports, err := test.AllocateFreePorts(2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		endpoints = []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
		peerURLs := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}

		etcdServer, err = embeddedetcd.NewServer(
			embeddedetcd.RootDir(path),
			embeddedetcd.ConfigureListener(endpoints, peerURLs),
			embeddedetcd.AutoCompactionMode("periodic"),
			embeddedetcd.AutoCompactionRetention("1h"),
			embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		<-etcdServer.ReadyNotify()

		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 5 * time.Second,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		if etcdClient != nil {
			err = etcdClient.Close()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		if etcdServer != nil {
			err = etcdServer.Close()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			<-etcdServer.StopNotify()
		}
		defFn()
	})

	ginkgo.It("should successfully perform defragmentation", func() {
		ctx := context.Background()
		for i := 0; i < 100; i++ {
			_, err := etcdClient.Put(ctx, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 50; i++ {
			_, err := etcdClient.Delete(ctx, fmt.Sprintf("key-%d", i))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		err := performDefrag(endpoints, etcdClient)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("should handle invalid endpoints", func() {
		invalidEndpoints := []string{"http://invalid-host:12345"}
		err := performDefrag(invalidEndpoints, etcdClient)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})
