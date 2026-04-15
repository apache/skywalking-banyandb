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
	"sort"
	"sync"
	"time"

	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcmetadata "google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v3"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/file"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/pkg/cmdsetup"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
)

const (
	host = "localhost"

	// ModeFile is the file-based mode for node discovery.
	ModeFile = metadata.NodeDiscoveryModeFile
	// ModeProperty is the property-based mode for schema registry.
	ModeProperty = metadata.RegistryModeProperty
	// ModeNone is the none mode for node discovery (standalone).
	ModeNone = metadata.NodeDiscoveryModeNone
)

// NodeDiscoveryConfig configures node discovery mode.
type NodeDiscoveryConfig struct {
	FileWriter *DiscoveryFileWriter
	Mode       string // ModeFile or ModeNone
}

// SchemaRegistryConfig configures schema registry mode.
type SchemaRegistryConfig struct {
	Mode string // ModeProperty
}

func newDefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		NodeDiscovery:  NodeDiscoveryConfig{Mode: ModeNone},
		SchemaRegistry: SchemaRegistryConfig{Mode: ModeProperty},
	}
}

// ClusterConfig configures node discovery and schema registry for test clusters.
type ClusterConfig struct {
	NodeDiscovery     NodeDiscoveryConfig
	SchemaRegistry    SchemaRegistryConfig
	schemaServerAddrs []string
	loadedKinds       []schema.Kind
	mu                sync.Mutex
}

// SchemaServerAddrs returns the accumulated schema server addresses.
func (c *ClusterConfig) SchemaServerAddrs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.schemaServerAddrs))
	copy(result, c.schemaServerAddrs)
	return result
}

// AddSchemaServerAddr adds a schema server address to the cluster config.
func (c *ClusterConfig) AddSchemaServerAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemaServerAddrs = append(c.schemaServerAddrs, addr)
}

// AddLoadedKinds records which schema kinds have been preloaded.
func (c *ClusterConfig) AddLoadedKinds(kinds ...schema.Kind) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadedKinds = append(c.loadedKinds, kinds...)
}

func (c *ClusterConfig) getLoadedKinds() []schema.Kind {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]schema.Kind, len(c.loadedKinds))
	copy(result, c.loadedKinds)
	return result
}

// PropertyClusterConfig creates a ClusterConfig that uses file-based discovery and property-based schema.
func PropertyClusterConfig(fileWriter *DiscoveryFileWriter) *ClusterConfig {
	return &ClusterConfig{
		NodeDiscovery: NodeDiscoveryConfig{
			Mode:       ModeFile,
			FileWriter: fileWriter,
		},
		SchemaRegistry: SchemaRegistryConfig{
			Mode: ModeProperty,
		},
	}
}

// DiscoveryFileWriter manages a dynamic YAML file for file-based node discovery.
type DiscoveryFileWriter struct {
	path  string
	nodes []file.NodeConfig
	mu    sync.Mutex
}

// NewDiscoveryFileWriter creates a new DiscoveryFileWriter in the given directory.
func NewDiscoveryFileWriter(dir string) *DiscoveryFileWriter {
	filePath := filepath.Join(dir, "discovery.yaml")
	w := &DiscoveryFileWriter{path: filePath}
	w.flush()
	return w
}

// AddNode adds a node to the discovery file and writes it.
func (w *DiscoveryFileWriter) AddNode(name, address string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nodes = append(w.nodes, file.NodeConfig{Name: name, Address: address})
	w.flushLocked()
}

// RemoveNode removes a node from the discovery file by address and writes it.
func (w *DiscoveryFileWriter) RemoveNode(address string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	filtered := make([]file.NodeConfig, 0, len(w.nodes))
	for _, n := range w.nodes {
		if n.Address != address {
			filtered = append(filtered, n)
		}
	}
	w.nodes = filtered
	w.flushLocked()
}

// Path returns the path to the discovery YAML file.
func (w *DiscoveryFileWriter) Path() string {
	return w.path
}

func (w *DiscoveryFileWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushLocked()
}

func (w *DiscoveryFileWriter) flushLocked() {
	cfg := file.NodeFileConfig{Nodes: w.nodes}
	data, marshalErr := yaml.Marshal(&cfg)
	gomega.Expect(marshalErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(os.WriteFile(w.path, data, 0o600)).To(gomega.Succeed())
}

// PreloadSchemaViaProperty connects to property schema servers and runs the provided schema loader functions.
func PreloadSchemaViaProperty(config *ClusterConfig, loaders ...func(ctx context.Context, registry schema.Registry) error) {
	addrs := config.SchemaServerAddrs()
	nodes := make([]*databasev1.Node, len(addrs))
	for idx, addr := range addrs {
		nodes[idx] = &databasev1.Node{
			Metadata:                  &commonv1.Metadata{Name: fmt.Sprintf("test-node-%d", idx)},
			Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
			PropertySchemaGrpcAddress: addr,
		}
	}
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: nodes},
	})
	gomega.Expect(regErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = reg.Close() }()
	gomega.Eventually(func() int {
		return len(reg.ActiveNodeNames())
	}).WithTimeout(testflags.EventuallyTimeout).WithPolling(200 * time.Millisecond).
		Should(gomega.Equal(len(nodes)))
	ctx := context.Background()
	for _, loader := range loaders {
		gomega.Expect(loader(ctx, reg)).To(gomega.Succeed())
	}
}

// testNodeRegistry implements schema.Node for property-based schema preloading in tests.
type testNodeRegistry struct {
	nodes []*databasev1.Node
}

func (r *testNodeRegistry) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	var result []*databasev1.Node
	for _, n := range r.nodes {
		for _, nodeRole := range n.GetRoles() {
			if nodeRole == role {
				result = append(result, n)
				break
			}
		}
	}
	return result, nil
}

func (r *testNodeRegistry) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return nil
}

func (r *testNodeRegistry) GetNode(_ context.Context, _ string) (*databasev1.Node, error) {
	return nil, nil
}

func (r *testNodeRegistry) UpdateNode(_ context.Context, _ *databasev1.Node) error { return nil }

// QueryNodeGroups returns active group names from each schema server.
func QueryNodeGroups(config *ClusterConfig) map[string][]string {
	result := make(map[string][]string)
	addrs := config.SchemaServerAddrs()
	ctx := context.Background()
	for _, addr := range addrs {
		nodes := []*databasev1.Node{{
			Metadata:                  &commonv1.Metadata{Name: "verify-node"},
			Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
			PropertySchemaGrpcAddress: addr,
		}}
		reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:  10 * time.Second,
			NodeRegistry: &testNodeRegistry{nodes: nodes},
		})
		gomega.Expect(regErr).NotTo(gomega.HaveOccurred())
		gomega.Eventually(func() int {
			return len(reg.ActiveNodeNames())
		}).WithTimeout(testflags.EventuallyTimeout).WithPolling(200 * time.Millisecond).
			Should(gomega.Equal(1))
		groups, listErr := reg.ListGroup(ctx)
		gomega.Expect(listErr).NotTo(gomega.HaveOccurred())
		groupNames := make([]string, 0, len(groups))
		for _, group := range groups {
			groupNames = append(groupNames, group.GetMetadata().GetName())
		}
		sort.Strings(groupNames)
		result[addr] = groupNames
		_ = reg.Close()
	}
	return result
}

// Standalone wires standalone modules to build a testing ready runtime.
func Standalone(config *ClusterConfig, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(config, []SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
		&preloadService{name: "property"},
	}, "", "", "", "", flags...)
}

// StandaloneWithAuth wires standalone modules to build a testing ready runtime with Auth.
func StandaloneWithAuth(config *ClusterConfig, username, password string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(config, []SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
		&preloadService{name: "property"},
	}, "", "", username, password, flags...)
}

// StandaloneWithTLS wires standalone modules to build a testing ready runtime with TLS enabled.
func StandaloneWithTLS(config *ClusterConfig, certFile, keyFile string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(config, []SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
		&preloadService{name: "property"},
	}, certFile, keyFile, "", "", flags...)
}

// EmptyStandalone wires standalone modules to build a testing ready runtime.
func EmptyStandalone(config *ClusterConfig, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(config, nil, "", "", "", "", flags...)
}

// EmptyStandaloneWithAuth wires standalone modules to build a testing ready runtime with Auth.
func EmptyStandaloneWithAuth(config *ClusterConfig, username, password string, flags ...string) (string, string, func()) {
	return StandaloneWithSchemaLoaders(config, nil, "", "", username, password, flags...)
}

// StandaloneWithSchemaLoaders wires standalone modules to build a testing ready runtime. It also allows to preload schema.
func StandaloneWithSchemaLoaders(config *ClusterConfig, schemaLoaders []SchemaLoader,
	certFile, keyFile string, username, password string, flags ...string,
) (string, string, func()) {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if config == nil {
		config = newDefaultClusterConfig()
	}
	portCount := 5
	var ports []int
	ports, err = test.AllocateFreePorts(portCount)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, httpAddr, closeFn := standaloneServerWithAuth(config, path, ports, schemaLoaders, certFile, keyFile, username, password, flags...)
	return addr, httpAddr, func() {
		closeFn()
		deferFn()
	}
}

// ClosableStandalone wires standalone modules to build a testing ready runtime.
func ClosableStandalone(config *ClusterConfig, path string, ports []int, flags ...string) (string, string, func()) {
	return standaloneServer(config, path, ports, []SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
		&preloadService{name: "trace"},
		&preloadService{name: "property"},
	}, "", "", flags...)
}

// ClosableStandaloneWithSchemaLoaders wires standalone modules to build a testing ready runtime.
func ClosableStandaloneWithSchemaLoaders(config *ClusterConfig, path string, ports []int, schemaLoaders []SchemaLoader, flags ...string) (string, string, func()) {
	return standaloneServer(config, path, ports, schemaLoaders, "", "", flags...)
}

// EmptyClosableStandalone wires standalone modules to build a testing ready runtime.
func EmptyClosableStandalone(config *ClusterConfig, path string, ports []int, flags ...string) (string, string, func()) {
	return standaloneServer(config, path, ports, nil, "", "", flags...)
}

func standaloneServer(config *ClusterConfig, path string, ports []int, schemaLoaders []SchemaLoader,
	certFile, keyFile string, flags ...string,
) (string, string, func()) {
	return standaloneServerWithAuth(config, path, ports, schemaLoaders, certFile, keyFile, "", "", flags...)
}

func standaloneServerWithAuth(config *ClusterConfig, path string, ports []int, schemaLoaders []SchemaLoader, certFile, keyFile string,
	username, password string, flags ...string,
) (string, string, func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	schemaPort := ports[4]
	schemaAddr := fmt.Sprintf("127.0.0.1:%d", schemaPort)
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
		"--property-root-path=" + path,
		"--trace-root-path=" + path,
		"--schema-server-root-path=" + path,
		"--schema-registry-mode=" + config.SchemaRegistry.Mode,
		"--node-discovery-mode=" + config.NodeDiscovery.Mode,
		"--node-host-provider=flag",
		"--node-host=127.0.0.1",
		"--schema-server-grpc-host=127.0.0.1",
		fmt.Sprintf("--schema-server-grpc-port=%d", schemaPort),
	}
	if config.NodeDiscovery.Mode == ModeFile {
		ff = append(ff,
			fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}
	config.AddSchemaServerAddr(schemaAddr)
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
	if config.NodeDiscovery.FileWriter != nil {
		config.NodeDiscovery.FileWriter.AddNode(
			fmt.Sprintf("127.0.0.1:%d", ports[0]),
			fmt.Sprintf("127.0.0.1:%d", ports[0]),
		)
	}
	if schemaLoaders != nil {
		preloadStandaloneSchemaViaProperty(config, schemaLoaders)
		var dialOpts []grpclib.DialOption
		if tlsEnabled {
			creds, credErr := credentials.NewClientTLSFromFile(certFile, "localhost")
			gomega.Expect(credErr).NotTo(gomega.HaveOccurred())
			dialOpts = append(dialOpts, grpclib.WithTransportCredentials(creds))
		} else {
			dialOpts = append(dialOpts, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		}
		waitForSchemaSyncWithAuth(addr, username, password, dialOpts...)
		if config.NodeDiscovery.Mode == ModeFile {
			waitForNodeDiscovery(addr, dialOpts...)
		}
	}
	return addr, httpAddr, closeFn
}

func preloadStandaloneSchemaViaProperty(config *ClusterConfig, schemaLoaders []SchemaLoader) {
	addrs := config.SchemaServerAddrs()
	nodes := make([]*databasev1.Node, len(addrs))
	for idx, nodeAddr := range addrs {
		nodes[idx] = &databasev1.Node{
			Metadata:                  &commonv1.Metadata{Name: fmt.Sprintf("standalone-node-%d", idx)},
			Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
			PropertySchemaGrpcAddress: nodeAddr,
		}
	}
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: nodes},
	})
	gomega.Expect(regErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = reg.Close() }()
	var units []run.Unit
	for _, sl := range schemaLoaders {
		sl.SetRegistry(reg)
		units = append(units, sl)
	}
	preloadGroup := run.NewGroup("preload")
	preloadGroup.Register(units...)
	runErr := preloadGroup.Run(context.Background())
	gomega.Expect(runErr).NotTo(gomega.HaveOccurred())
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
	if p.name == "property" {
		return test_property.PreloadSchema(ctx, p.registry)
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
		waitCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(waitCh)
		}()
		select {
		case <-waitCh:
		case <-time.After(30 * time.Second):
		}
	}
}

func hasFlagValue(flags []string, key, value string) bool {
	for idx, f := range flags {
		if f == key+"="+value {
			return true
		}
		if f == key && idx+1 < len(flags) && flags[idx+1] == value {
			return true
		}
	}
	return false
}

func startDataNode(config *ClusterConfig, dataDir string, flags ...string) (string, string, func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	runSchemaServer := !hasFlagValue(flags, "--has-meta-role", "false")
	portCount := 2
	if runSchemaServer {
		portCount = 3
	}
	ports, err := test.AllocateFreePorts(portCount)
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
		"--trace-root-path="+dataDir,
		"--schema-server-root-path="+dataDir,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--logging-modules", "trace,sidx,property-schema-registry",
		"--logging-levels", "debug,debug,debug",
		"--schema-registry-mode="+config.SchemaRegistry.Mode,
		"--node-discovery-mode="+config.NodeDiscovery.Mode,
	)

	if config.NodeDiscovery.Mode == ModeFile {
		flags = append(flags,
			fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}

	if runSchemaServer {
		schemaPort := ports[2]
		schemaAddr := fmt.Sprintf("%s:%d", nodeHost, schemaPort)
		flags = append(flags,
			"--schema-server-grpc-host="+nodeHost,
			fmt.Sprintf("--schema-server-grpc-port=%d", schemaPort),
		)
		config.AddSchemaServerAddr(schemaAddr)
	}

	rawCloseFn := CMD(flags...)

	gomega.Eventually(
		helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(gomega.Succeed())

	nodeAddr := fmt.Sprintf("%s:%d", nodeHost, ports[0])
	if config.NodeDiscovery.FileWriter != nil {
		config.NodeDiscovery.FileWriter.AddNode(nodeAddr, nodeAddr)
	}

	closeFn := func() {
		if config.NodeDiscovery.FileWriter != nil {
			config.NodeDiscovery.FileWriter.RemoveNode(nodeAddr)
		}
		rawCloseFn()
	}

	return addr, fmt.Sprintf("%s:%d", host, ports[1]), closeFn
}

// DataNode runs a data node.
func DataNode(config *ClusterConfig, flags ...string) func() {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, _, closeFn := DataNodeFromDataDir(config, path, flags...)
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
func DataNodeFromDataDir(config *ClusterConfig, dataDir string, flags ...string) (string, string, func()) {
	grpcAddr, propertyRepairAddr, closeFn := startDataNode(config, dataDir, flags...)
	return grpcAddr, propertyRepairAddr, closeFn
}

// DataNodeWithAddrAndDir runs a data node and returns the address and root path.
func DataNodeWithAddrAndDir(config *ClusterConfig, flags ...string) (string, string, func()) {
	path, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, _, closeFn := startDataNode(config, path, flags...)
	return addr, path, func() {
		closeFn()
		deferFn()
	}
}

func startLiaisonNode(config *ClusterConfig, path string, flags ...string) (string, string, func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	ports, err := test.AllocateFreePorts(3)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	grpcAddr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	nodeHost := "127.0.0.1"
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
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--stream-root-path="+path,
		"--measure-root-path="+path,
		"--trace-root-path="+path,
		"--stream-flush-timeout=500ms",
		"--measure-flush-timeout=500ms",
		"--trace-flush-timeout=500ms",
		"--stream-sync-interval=1s",
		"--measure-sync-interval=1s",
		"--trace-sync-interval=1s",
		"--logging-modules", "trace,sidx,property-schema-registry",
		"--logging-levels", "debug,debug,debug",
		"--schema-registry-mode="+config.SchemaRegistry.Mode,
		"--node-discovery-mode="+config.NodeDiscovery.Mode,
	)
	if config.NodeDiscovery.Mode == ModeFile {
		flags = append(flags,
			fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}
	closeFn := CMD(flags...)
	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr, ""), testflags.EventuallyTimeout).Should(gomega.Succeed())
	if config.NodeDiscovery.FileWriter != nil {
		config.NodeDiscovery.FileWriter.AddNode(
			fmt.Sprintf("%s:%d", nodeHost, ports[2]),
			fmt.Sprintf("%s:%d", nodeHost, ports[2]),
		)
	}
	waitForActiveDataNodes(grpcAddr, config)

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
	}
}

// LiaisonNode runs a liaison node.
func LiaisonNode(config *ClusterConfig, flags ...string) (grpcAddr string, closeFn func()) {
	grpcAddr, _, closeFn = LiaisonNodeWithHTTP(config, flags...)
	return
}

// LiaisonNodeWithHTTP runs a liaison node with HTTP enabled and returns the gRPC and HTTP addresses.
func LiaisonNodeWithHTTP(config *ClusterConfig, flags ...string) (string, string, func()) {
	dataDir, deferFn, dirErr := test.NewSpace()
	gomega.Expect(dirErr).NotTo(gomega.HaveOccurred())
	grpcAddr, httpAddr, closeFn := startLiaisonNode(config, dataDir, flags...)
	return grpcAddr, httpAddr, func() {
		closeFn()
		deferFn()
	}
}

// LiaisonNodeWithAddrAndDir runs a liaison node and returns the gRPC address, root data path, and closer.
func LiaisonNodeWithAddrAndDir(config *ClusterConfig, flags ...string) (string, string, func()) {
	dataDir, deferFn, dirErr := test.NewSpace()
	gomega.Expect(dirErr).NotTo(gomega.HaveOccurred())
	grpcAddr, _, closeFn := startLiaisonNode(config, dataDir, flags...)
	return grpcAddr, dataDir, func() {
		closeFn()
		deferFn()
	}
}

func waitForSchemaSyncWithAuth(grpcAddr, username, password string, opts ...grpclib.DialOption) {
	if len(opts) == 0 {
		opts = append(opts, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, connErr := grpchelper.ConnWithAuth(grpcAddr, 10*time.Second, username, password, opts...)
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(g gomega.Gomega) {
		ctx := context.Background()
		if username != "" && password != "" {
			md := grpcmetadata.Pairs("username", username, "password", password)
			ctx = grpcmetadata.NewOutgoingContext(ctx, md)
		}
		resp, listErr := groupClient.List(ctx, &databasev1.GroupRegistryServiceListRequest{})
		g.Expect(listErr).NotTo(gomega.HaveOccurred())
		g.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
			"no groups found in standalone schema registry")
	}, testflags.EventuallyTimeout).Should(gomega.Succeed())
}

func waitForNodeDiscovery(grpcAddr string, dialOpts ...grpclib.DialOption) {
	conn, connErr := grpchelper.Conn(grpcAddr, 10*time.Second, dialOpts...)
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(g gomega.Gomega) {
		listResp, listErr := groupClient.List(
			context.Background(), &databasev1.GroupRegistryServiceListRequest{})
		g.Expect(listErr).NotTo(gomega.HaveOccurred())
		groups := listResp.GetGroup()
		g.Expect(groups).NotTo(gomega.BeEmpty())
		var groupName string
		for _, group := range groups {
			catalog := group.GetCatalog()
			if catalog == commonv1.Catalog_CATALOG_MEASURE ||
				catalog == commonv1.Catalog_CATALOG_STREAM ||
				catalog == commonv1.Catalog_CATALOG_TRACE {
				groupName = group.GetMetadata().GetName()
				break
			}
		}
		g.Expect(groupName).NotTo(gomega.BeEmpty(), "no data group found for node discovery check")
		_, inspectErr := groupClient.Inspect(
			context.Background(), &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		g.Expect(inspectErr).NotTo(gomega.HaveOccurred(),
			"node discovery not ready: inspect failed for group %s", groupName)
	}, testflags.EventuallyTimeout).Should(gomega.Succeed())
}

func waitForActiveDataNodes(grpcAddr string, config *ClusterConfig) {
	conn, connErr := grpchelper.Conn(grpcAddr, 10*time.Second,
		grpclib.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(g gomega.Gomega) {
		resp, listErr := groupClient.List(
			context.Background(), &databasev1.GroupRegistryServiceListRequest{})
		g.Expect(listErr).NotTo(gomega.HaveOccurred())
		g.Expect(resp.GetGroup()).NotTo(gomega.BeEmpty(),
			"no groups found in liaison schema registry")
	}, testflags.EventuallyTimeout).Should(gomega.Succeed())
	clusterClient := databasev1.NewClusterStateServiceClient(conn)
	gomega.Eventually(func(g gomega.Gomega) {
		state, stateErr := clusterClient.GetClusterState(
			context.Background(), &databasev1.GetClusterStateRequest{})
		g.Expect(stateErr).NotTo(gomega.HaveOccurred())
		tire1Table := state.GetRouteTables()["tire1"]
		g.Expect(tire1Table).NotTo(gomega.BeNil(), "tire1 route table not found")
		g.Expect(tire1Table.GetActive()).NotTo(gomega.BeEmpty(),
			"no active liaison nodes in tire1 route table")
		tire2Table := state.GetRouteTables()["tire2"]
		g.Expect(tire2Table).NotTo(gomega.BeNil(), "tire2 route table not found")
		g.Expect(tire2Table.GetActive()).NotTo(gomega.BeEmpty(),
			"no active data nodes in tire2 route table")
	}, testflags.EventuallyTimeout).Should(gomega.Succeed())
	for _, kind := range config.getLoadedKinds() {
		waitForSchemaKind(conn, kind)
	}
	time.Sleep(5 * time.Second)
}

func waitForSchemaKind(conn *grpclib.ClientConn, kind schema.Kind) {
	catalog := kindToCatalog(kind)
	if catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return
	}
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	gomega.Eventually(func(g gomega.Gomega) {
		groupResp, groupListErr := groupClient.List(
			context.Background(), &databasev1.GroupRegistryServiceListRequest{})
		g.Expect(groupListErr).NotTo(gomega.HaveOccurred())
		var matchingGroups int
		var syncedGroups int
		for _, grp := range groupResp.GetGroup() {
			if grp.GetCatalog() != catalog {
				continue
			}
			matchingGroups++
			groupName := grp.GetMetadata().GetName()
			found, schemaErr := hasSchemaInGroup(conn, kind, groupName)
			g.Expect(schemaErr).NotTo(gomega.HaveOccurred())
			if found {
				syncedGroups++
			}
		}
		g.Expect(matchingGroups).To(gomega.BeNumerically(">", 0),
			fmt.Sprintf("no groups with catalog %s found", catalog))
		g.Expect(syncedGroups).To(gomega.Equal(matchingGroups),
			fmt.Sprintf("only %d/%d %s groups have schemas synced",
				syncedGroups, matchingGroups, kind))
	}, testflags.EventuallyTimeout).Should(gomega.Succeed())
}

func kindToCatalog(kind schema.Kind) commonv1.Catalog {
	switch kind {
	case schema.KindStream:
		return commonv1.Catalog_CATALOG_STREAM
	case schema.KindMeasure:
		return commonv1.Catalog_CATALOG_MEASURE
	case schema.KindTrace:
		return commonv1.Catalog_CATALOG_TRACE
	default:
		return commonv1.Catalog_CATALOG_UNSPECIFIED
	}
}

func hasSchemaInGroup(conn *grpclib.ClientConn, kind schema.Kind, group string) (bool, error) {
	ctx := context.Background()
	switch kind {
	case schema.KindStream:
		client := databasev1.NewStreamRegistryServiceClient(conn)
		resp, listErr := client.List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: group})
		return listErr == nil && len(resp.GetStream()) > 0, listErr
	case schema.KindMeasure:
		client := databasev1.NewMeasureRegistryServiceClient(conn)
		resp, listErr := client.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: group})
		return listErr == nil && len(resp.GetMeasure()) > 0, listErr
	case schema.KindTrace:
		client := databasev1.NewTraceRegistryServiceClient(conn)
		resp, listErr := client.List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: group})
		return listErr == nil && len(resp.GetTrace()) > 0, listErr
	default:
	}
	return false, nil
}
