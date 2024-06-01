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

// Package setup implements a real env in which to run tests.
package setup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/cmdsetup"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

const host = "localhost"

// Standalone wires standalone modules to build a testing ready runtime.
func Standalone(flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
	}, "", "", flags...)
}

// StandaloneWithTLS wires standalone modules to build a testing ready runtime with TLS enabled.
func StandaloneWithTLS(certFile, keyFile string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
	}, certFile, keyFile, flags...)
}

// EmptyStandalone wires standalone modules to build a testing ready runtime.
func EmptyStandalone(flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(nil, "", "", flags...)
}

// StandaloneWithSchemaLoaders wires standalone modules to build a testing ready runtime. It also allows to preload schema.
func StandaloneWithSchemaLoaders(schemaLoaders []SchemaLoader, certFile, keyFile string, flags ...string) (string, string, func()) {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, httpAddr, closeFn := standaloneServer(path, ports, schemaLoaders, certFile, keyFile, flags...)
	return addr, httpAddr, func() {
		closeFn()
		deferFn()
	}
}

// ClosableStandalone wires standalone modules to build a testing ready runtime.
func ClosableStandalone(path string, ports []int, flags ...string) (string, string, func()) {
	return standaloneServer(path, ports, []SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
	}, "", "", flags...)
}

// ClosableStandaloneWithSchemaLoaders wires standalone modules to build a testing ready runtime.
func ClosableStandaloneWithSchemaLoaders(path string, ports []int, schemaLoaders []SchemaLoader, flags ...string) (string, string, func()) {
	return standaloneServer(path, ports, schemaLoaders, "", "", flags...)
}

// EmptyClosableStandalone wires standalone modules to build a testing ready runtime.
func EmptyClosableStandalone(path string, ports []int, flags ...string) (string, string, func()) {
	return standaloneServer(path, ports, nil, "", "", flags...)
}

func standaloneServer(path string, ports []int, schemaLoaders []SchemaLoader, certFile, keyFile string, flags ...string) (string, string, func()) {
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	endpoint := fmt.Sprintf("http://%s:%d", host, ports[2])
	ff := []string{
		"--logging-env=dev",
		"--logging-level=" + testflags.LogLevel,
		"--grpc-host=" + host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host=" + host,
		fmt.Sprintf("--http-port=%d", ports[1]),
		"--http-grpc-addr=" + addr,
		"--stream-root-path=" + path,
		"--measure-root-path=" + path,
		"--metadata-root-path=" + path,
		fmt.Sprintf("--etcd-listen-client-url=%s", endpoint), fmt.Sprintf("--etcd-listen-peer-url=http://%s:%d", host, ports[3]),
	}
	tlsEnabled := false
	if certFile != "" && keyFile != "" {
		ff = append(ff, "--tls=true", "--cert-file="+certFile, "--key-file="+keyFile, "--http-grpc-cert-file="+certFile)
		tlsEnabled = true
	}
	if len(flags) > 0 {
		ff = append(ff, flags...)
	}
	cmdFlags := []string{"standalone"}
	cmdFlags = append(cmdFlags, ff...)
	closeFn := CMD(cmdFlags...)
	if tlsEnabled {
		creds, err := credentials.NewClientTLSFromFile(certFile, "localhost")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(creds)), testflags.EventuallyTimeout).
			Should(gomega.Succeed())
	} else {
		gomega.Eventually(
			helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
			testflags.EventuallyTimeout).Should(gomega.Succeed())
	}
	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr), testflags.EventuallyTimeout).Should(gomega.Succeed())

	if schemaLoaders != nil {
		schemaRegistry, err := schema.NewEtcdSchemaRegistry(
			schema.Namespace(metadata.DefaultNamespace),
			schema.ConfigureServerEndpoints([]string{endpoint}),
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer schemaRegistry.Close()
		var units []run.Unit
		for _, sl := range schemaLoaders {
			sl.SetRegistry(schemaRegistry)
			units = append(units, sl)
		}
		preloadGroup := run.NewGroup("preload")
		preloadGroup.Register(units...)
		err = preloadGroup.Run(context.Background())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	return addr, httpAddr, closeFn
}

// SchemaLoader is a service that can preload schema.
type SchemaLoader interface {
	run.Unit
	SetRegistry(registry schema.Registry)
}

type preloadService struct {
	registry schema.Registry
	name     string
}

func (p *preloadService) Name() string {
	return "preload-" + p.name
}

func (p *preloadService) PreRun(ctx context.Context) error {
	if p.name == "stream" {
		return test_stream.PreloadSchema(ctx, p.registry)
	}
	return test_measure.PreloadSchema(ctx, p.registry)
}

func (p *preloadService) SetRegistry(registry schema.Registry) {
	p.registry = registry
}

// CMD runs the command with given flags.
func CMD(flags ...string) func() {
	closer, closeFn := run.NewTester("closer")
	rootCmd := cmdsetup.NewRoot(closer)
	rootCmd.SetArgs(flags)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		gomega.Expect(rootCmd.Execute()).ShouldNot(gomega.HaveOccurred())
	}()
	return func() {
		closeFn()
		wg.Wait()
	}
}

// DataNode runs a data node.
func DataNode(etcdEndpoint string) func() {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ports, err := test.AllocateFreePorts(1)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	nodeHost := "127.0.0.1"
	closeFn := CMD("data",
		"--grpc-host="+host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--stream-root-path="+path,
		"--measure-root-path="+path,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost)
	gomega.Eventually(
		helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(gomega.Succeed())
	gomega.Eventually(func() (map[string]*databasev1.Node, error) {
		return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, ports[0]))
	}, testflags.EventuallyTimeout).Should(gomega.HaveLen(1))
	return func() {
		closeFn()
		deferFn()
	}
}

// LiaisonNode runs a liaison node.
func LiaisonNode(etcdEndpoint string) (string, func()) {
	ports, err := test.AllocateFreePorts(2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	nodeHost := "127.0.0.1"
	closeFn := CMD("liaison",
		"--grpc-host="+host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host="+host,
		fmt.Sprintf("--http-port=%d", ports[1]),
		"--http-grpc-addr="+addr,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost)
	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr), testflags.EventuallyTimeout).Should(gomega.Succeed())
	gomega.Eventually(func() (map[string]*databasev1.Node, error) {
		return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, ports[0]))
	}, testflags.EventuallyTimeout).Should(gomega.HaveLen(1))
	return addr, closeFn
}
