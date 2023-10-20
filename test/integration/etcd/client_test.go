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

package integration_etcd_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const host = "127.0.0.1"

const namespace = "liaison-test"

const nodeHost = "liaison-1"

var _ = Describe("Client Test", func() {
	var (
		dir                string
		dirSpaceDef        func()
		caFilePath         string
		serverKeyFilePath  string
		serverCertFilePath string
		clientKeyFilePath  string
		clientCertFilePath string
		goods              []gleak.Goroutine
	)
	BeforeEach(func() {
		var err error
		dir, dirSpaceDef, err = test.NewSpace()
		Expect(err).ShouldNot(HaveOccurred())
		_, currentFile, _, _ := runtime.Caller(0)
		basePath := filepath.Dir(currentFile)
		caFilePath = filepath.Join(basePath, "testdata/ca.crt")
		serverKeyFilePath = filepath.Join(basePath, "testdata/server-serverusage.key.insecure")
		serverCertFilePath = filepath.Join(basePath, "testdata/server-serverusage.crt")
		clientKeyFilePath = filepath.Join(basePath, "testdata/client-clientusage.key.insecure")
		clientCertFilePath = filepath.Join(basePath, "testdata/client-clientusage.crt")

		goods = gleak.Goroutines()
	})
	AfterEach(func() {
		dirSpaceDef()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	It("should be using user/password connect etcd server successfully", func() {
		serverTLSInfo := transport.TLSInfo{}
		clientTLSInfo := transport.TLSInfo{}
		clientConfig, err := clientTLSInfo.ClientConfig()
		Expect(err).ShouldNot(HaveOccurred())
		username := "banyandb"
		password := "banyandb"

		etcdServer, err := createEtcdServer(dir, serverTLSInfo)
		Expect(err).ShouldNot(HaveOccurred())
		// wait for e.Server to join the cluster
		<-etcdServer.Server.ReadyNotify()
		etcdEndpoint := etcdServer.Config().ListenClientUrls[0].String()
		defer etcdServer.Close()

		adminClient, err := clientv3.New(clientv3.Config{
			Endpoints: []string{etcdEndpoint},
		})
		Expect(err).ShouldNot(HaveOccurred())
		defer adminClient.Close()
		adminClient.RoleAdd(context.Background(), "root")
		adminClient.UserAdd(context.Background(), "root", "root")
		adminClient.UserGrantRole(context.Background(), "root", "root")
		adminClient.UserAdd(context.Background(), username, password)
		adminClient.UserGrantRole(context.Background(), username, "root")
		adminClient.AuthEnable(context.Background())

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
			"--node-host-provider", "flag",
			"--node-host", nodeHost,
			"--etcd-endpoints", etcdEndpoint,
			"--etcd-username", username,
			"--etcd-password", password)
		defer closeFn()

		Eventually(helpers.HTTPHealthCheck(httpAddr), flags.EventuallyTimeout).Should(Succeed())
		Eventually(func() (map[string]*databasev1.Node, error) {
			return listKeys(etcdEndpoint, username, password, clientConfig, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(HaveLen(1))
	})

	It("should be using cacert connect etcd server successfully", func() {
		serverTLSInfo := transport.TLSInfo{
			KeyFile:  serverKeyFilePath,
			CertFile: serverCertFilePath,
		}
		clientTLSInfo := transport.TLSInfo{
			TrustedCAFile: caFilePath,
		}
		clientConfig, err := clientTLSInfo.ClientConfig()
		Expect(err).ShouldNot(HaveOccurred())

		etcdServer, err := createEtcdServer(dir, serverTLSInfo)
		Expect(err).ShouldNot(HaveOccurred())
		// wait for e.Server to join the cluster
		<-etcdServer.Server.ReadyNotify()
		etcdEndpoint := etcdServer.Config().ListenClientUrls[0].String()
		defer etcdServer.Close()

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
			"--node-host-provider", "flag",
			"--node-host", nodeHost,
			"--etcd-endpoints", etcdEndpoint,
			"--etcd-tls-ca-file", caFilePath)
		defer closeFn()

		Eventually(helpers.HTTPHealthCheck(httpAddr), flags.EventuallyTimeout).Should(Succeed())
		Eventually(func() (map[string]*databasev1.Node, error) {
			return listKeys(etcdEndpoint, "", "", clientConfig, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(HaveLen(1))
	})

	It("should be using key pair connect etcd server successfully", func() {
		serverTLSInfo := transport.TLSInfo{
			KeyFile:        serverKeyFilePath,
			CertFile:       serverCertFilePath,
			TrustedCAFile:  caFilePath,
			ClientCertAuth: true,
		}
		clientTLSInfo := transport.TLSInfo{
			KeyFile:       clientKeyFilePath,
			CertFile:      clientCertFilePath,
			TrustedCAFile: caFilePath,
		}
		clientConfig, err := clientTLSInfo.ClientConfig()
		Expect(err).ShouldNot(HaveOccurred())

		etcdServer, err := createEtcdServer(dir, serverTLSInfo)
		Expect(err).ShouldNot(HaveOccurred())
		// wait for e.Server to join the cluster
		<-etcdServer.Server.ReadyNotify()
		etcdEndpoint := etcdServer.Config().ListenClientUrls[0].String()
		defer etcdServer.Close()

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
			"--node-host-provider", "flag",
			"--node-host", nodeHost,
			"--etcd-endpoints", etcdEndpoint,
			"--etcd-tls-ca-file", caFilePath,
			"--etcd-tls-cert-file", clientCertFilePath,
			"--etcd-tls-key-file", clientKeyFilePath)
		defer closeFn()

		Eventually(helpers.HTTPHealthCheck(httpAddr), flags.EventuallyTimeout).Should(Succeed())
		Eventually(func() (map[string]*databasev1.Node, error) {
			return listKeys(etcdEndpoint, "", "", clientConfig, fmt.Sprintf("/%s/nodes/%s:%d", namespace, nodeHost, ports[0]))
		}, flags.EventuallyTimeout).Should(HaveLen(1))
	})
})

func parseURLs(urls []string) ([]url.URL, error) {
	uu := make([]url.URL, 0, len(urls))
	for _, u := range urls {
		cURL, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		uu = append(uu, *cURL)
	}
	return uu, nil
}

func createEtcdServer(dir string, tlsInfo transport.TLSInfo) (e *embed.Etcd, err error) {
	ports, err := test.AllocateFreePorts(2)
	if err != nil {
		return nil, err
	}
	enableTLS := tlsInfo.TrustedCAFile != "" || tlsInfo.CertFile != "" || tlsInfo.KeyFile != ""
	httpProtocol := "http"
	if enableTLS {
		httpProtocol = "https"
	}
	clientURL := fmt.Sprintf("%s://%s:%d", httpProtocol, host, ports[0])
	peerURL := fmt.Sprintf("%s://%s:%d", httpProtocol, host, ports[1])
	cURLs, err := parseURLs([]string{clientURL})
	if err != nil {
		return nil, err
	}
	pURLs, err := parseURLs([]string{peerURL})
	if err != nil {
		return nil, err
	}

	etcdConfig := embed.NewConfig()
	etcdConfig.Name = "test-etcd-certificate"
	etcdConfig.Dir = dir
	etcdConfig.ListenClientUrls, etcdConfig.AdvertiseClientUrls = cURLs, cURLs
	etcdConfig.ListenPeerUrls, etcdConfig.AdvertisePeerUrls = pURLs, pURLs
	etcdConfig.InitialCluster = etcdConfig.InitialClusterFromName(etcdConfig.Name)
	etcdConfig.ClientTLSInfo = tlsInfo
	etcdConfig.PeerTLSInfo = tlsInfo

	return embed.StartEtcd(etcdConfig)
}

func listKeys(serverAddress string, username string, password string, tls *tls.Config, prefix string) (map[string]*databasev1.Node, error) {
	defaultDialTimeout := 5 * time.Second
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{serverAddress},
		TLS:         tls,
		Username:    username,
		Password:    password,
		DialTimeout: defaultDialTimeout,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	}

	nodeMap := make(map[string]*databasev1.Node)
	for _, kv := range resp.Kvs {
		md, err := schema.KindNode.Unmarshal(kv)
		if err != nil {
			return nil, err
		}
		nodeMap[string(kv.Key)] = md.Spec.(*databasev1.Node)
	}

	return nodeMap, nil
}
