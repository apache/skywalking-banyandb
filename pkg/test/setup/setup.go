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
	"os"
	"path/filepath"
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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
)

const host = "localhost"

// Standalone wires standalone modules to build a testing ready runtime.
func Standalone(flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
	}, "", "", "", "", flags...)
}

// StandaloneWithAuth wires standalone modules to build a testing ready runtime with Auth.
func StandaloneWithAuth(username, password string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
	}, "", "", username, password, flags...)
}

// StandaloneWithTLS wires standalone modules to build a testing ready runtime with TLS enabled.
func StandaloneWithTLS(certFile, keyFile string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
	}, certFile, keyFile, "", "", flags...)
}

// EmptyStandalone wires standalone modules to build a testing ready runtime.
func EmptyStandalone(flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(nil, "", "", "", "", flags...)
}

// EmptyStandaloneWithAuth wires standalone modules to build a testing ready runtime with Auth.
func EmptyStandaloneWithAuth(username, password string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(nil, "", "", username, password, flags...)
}

// StandaloneWithSchemaLoaders wires standalone modules to build a testing ready runtime. It also allows to preload schema.
func StandaloneWithSchemaLoaders(schemaLoaders []SchemaLoader, certFile, keyFile string, username, password string, flags ...string) (string, string, func()) {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, httpAddr, closeFn := standaloneServerWithAuth(path, ports, schemaLoaders, certFile, keyFile, username, password, flags...)
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
		&preloadService{name: "trace"},
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
	return standaloneServerWithAuth(path, ports, schemaLoaders, certFile, keyFile, "", "", flags...)
}

func standaloneServerWithAuth(path string, ports []int, schemaLoaders []SchemaLoader, certFile, keyFile string,
	username, password string, flags ...string,
) (string, string, func()) {
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
		"--property-root-path=" + path,
		"--trace-root-path=" + path,
		fmt.Sprintf("--etcd-listen-client-url=%s", endpoint), fmt.Sprintf("--etcd-listen-peer-url=http://%s:%d", host, ports[3]),
	}
	tlsEnabled := false
	if certFile != "" && keyFile != "" {
		ff = append(ff, "--tls=true", "--cert-file="+certFile, "--key-file="+keyFile, "--http-grpc-cert-file="+certFile,
			"--http-tls=true", "--http-cert-file="+certFile, "--http-key-file="+keyFile)
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
		gomega.Eventually(helpers.HealthCheckWithAuth(addr, 10*time.Second, 10*time.Second,
			username, password, grpclib.WithTransportCredentials(creds)), testflags.EventuallyTimeout).
			Should(gomega.Succeed())
		gomega.Eventually(helpers.HTTPHealthCheckWithAuth(httpAddr, certFile, username, password), testflags.EventuallyTimeout).Should(gomega.Succeed())
	} else {
		gomega.Eventually(
			helpers.HealthCheckWithAuth(addr, 10*time.Second, 10*time.Second,
				username, password, grpclib.WithTransportCredentials(insecure.NewCredentials())),
			testflags.EventuallyTimeout).Should(gomega.Succeed())
		gomega.Eventually(helpers.HTTPHealthCheckWithAuth(httpAddr, "", username, password), testflags.EventuallyTimeout).Should(gomega.Succeed())
	}

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
	if p.name == "trace" {
		return test_trace.PreloadSchema(ctx, p.registry)
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

func startDataNode(etcdEndpoint, dataDir string, flags ...string) (string, string, func()) {
	ports, err := test.AllocateFreePorts(2)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	addr := fmt.Sprintf("%s:%d", host, ports[0])
	nodeHost := "127.0.0.1"

	flags = append(flags,
		"data",
		"--grpc-host="+host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		fmt.Sprintf("--property-repair-gossip-grpc-port=%d", ports[1]),
		"--stream-root-path="+dataDir,
		"--measure-root-path="+dataDir,
		"--property-root-path="+dataDir,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
	)

	closeFn := CMD(flags...)

	gomega.Eventually(
		helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(gomega.Succeed())

	gomega.Eventually(func() (map[string]*databasev1.Node, error) {
		return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, ports[0]))
	}, testflags.EventuallyTimeout).Should(gomega.HaveLen(1))

	return addr, fmt.Sprintf("%s:%d", host, ports[1]), closeFn
}

// DataNode runs a data node.
func DataNode(etcdEndpoint string, flags ...string) func() {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, _, closeFn := DataNodeFromDataDir(etcdEndpoint, path, flags...)
	return func() {
		fmt.Printf("Data tsdb path: %s\n", path)
		_ = filepath.Walk(path, func(path string, _ os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path)
			return nil
		})
		fmt.Println("done")
		closeFn()
		deferFn()
	}
}

// DataNodeFromDataDir runs a data node with a specific data directory.
func DataNodeFromDataDir(etcdEndpoint, dataDir string, flags ...string) (string, string, func()) {
	grpcAddr, propertyRepairAddr, closeFn := startDataNode(etcdEndpoint, dataDir, flags...)
	return grpcAddr, propertyRepairAddr, closeFn
}

// DataNodeWithAddrAndDir runs a data node and returns the address and root path.
func DataNodeWithAddrAndDir(etcdEndpoint string, flags ...string) (string, string, func()) {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, _, closeFn := startDataNode(etcdEndpoint, path, flags...)
	return addr, path, func() {
		closeFn()
		deferFn()
	}
}

// LiaisonNode runs a liaison node.
func LiaisonNode(etcdEndpoint string, flags ...string) (grpcAddr string, closeFn func()) {
	grpcAddr, _, closeFn = LiaisonNodeWithHTTP(etcdEndpoint, flags...)
	return
}

// LiaisonNodeWithHTTP runs a liaison node with HTTP enabled and returns the gRPC and HTTP addresses.
func LiaisonNodeWithHTTP(etcdEndpoint string, flags ...string) (string, string, func()) {
	ports, err := test.AllocateFreePorts(3)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	grpcAddr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	nodeHost := "127.0.0.1"
	path, deferFn, err := test.NewSpace()
	logger.Infof("liaison test directory: %s", path)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	flags = append(flags, "liaison",
		"--grpc-host="+host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host="+host,
		fmt.Sprintf("--http-port=%d", ports[1]),
		"--liaison-server-grpc-host="+host,
		fmt.Sprintf("--liaison-server-grpc-port=%d", ports[2]),
		"--http-grpc-addr="+grpcAddr,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--stream-root-path="+path,
		"--measure-root-path="+path,
		"--stream-flush-timeout=500ms",
		"--measure-flush-timeout=500ms",
		"--stream-sync-interval=1s",
		"--measure-sync-interval=1s",
	)
	closeFn := CMD(flags...)
	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr, ""), testflags.EventuallyTimeout).Should(gomega.Succeed())
	gomega.Eventually(func() (map[string]*databasev1.Node, error) {
		return helpers.ListKeys(etcdEndpoint, fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, ports[2]))
	}, testflags.EventuallyTimeout).Should(gomega.HaveLen(1))
	return grpcAddr, httpAddr, func() {
		fmt.Printf("Liaison %d write queue path: %s\n", ports[0], path)
		_ = filepath.Walk(path, func(path string, _ os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			fmt.Println(path)
			return nil
		})
		fmt.Println("done")
		closeFn()
		deferFn()
	}
}
