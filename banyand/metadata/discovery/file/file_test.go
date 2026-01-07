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

package file

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	testGRPCTimeout   = 2 * time.Second
	testFetchInterval = 200 * time.Millisecond
)

func TestNewService(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		configFile := createTempConfigFile(t, `
nodes:
  - name: node1
    grpc_address: 127.0.0.1:17912
`)
		defer os.Remove(configFile)

		svc, err := NewService(Config{
			FilePath:      configFile,
			GRPCTimeout:   testGRPCTimeout,
			FetchInterval: testFetchInterval,
		})
		require.NoError(t, err)
		require.NotNil(t, svc)
		require.NoError(t, svc.Close())
	})

	t.Run("empty file path", func(t *testing.T) {
		_, err := NewService(Config{FilePath: ""})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "file path cannot be empty")
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := NewService(Config{
			FilePath:      "/not/exist",
			GRPCTimeout:   testGRPCTimeout,
			FetchInterval: testFetchInterval,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to access file path")
	})
}

func TestStartWithInvalidConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid yaml", func(t *testing.T) {
		configFile := createTempConfigFile(t, `
nodes:
  - name: node1
    grpc_address: [invalid
`)
		defer os.Remove(configFile)

		svc, err := NewService(Config{
			FilePath:      configFile,
			GRPCTimeout:   testGRPCTimeout,
			FetchInterval: testFetchInterval,
		})
		require.NoError(t, err)
		defer svc.Close()

		err = svc.Start(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse YAML")
	})

	t.Run("missing address", func(t *testing.T) {
		configFile := createTempConfigFile(t, `
nodes:
  - name: node1
`)
		defer os.Remove(configFile)

		svc, err := NewService(Config{
			FilePath:      configFile,
			GRPCTimeout:   testGRPCTimeout,
			FetchInterval: testFetchInterval,
		})
		require.NoError(t, err)
		defer svc.Close()

		err = svc.Start(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required field: grpc_address")
	})

	t.Run("tls enabled without ca cert", func(t *testing.T) {
		configFile := createTempConfigFile(t, `
nodes:
  - name: node1
    grpc_address: 127.0.0.1:17912
    tls_enabled: true
`)
		defer os.Remove(configFile)

		svc, err := NewService(Config{
			FilePath:      configFile,
			GRPCTimeout:   testGRPCTimeout,
			FetchInterval: testFetchInterval,
		})
		require.NoError(t, err)
		defer svc.Close()

		err = svc.Start(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing ca_cert_path")
	})
}

func TestStartAndCacheNodes(t *testing.T) {
	listener, grpcServer, nodeServer := startMockGRPCServer(t)
	defer grpcServer.Stop()
	defer listener.Close()

	nodeName := "test-node"
	serverAddr := listener.Addr().String()
	nodeServer.setNode(newTestNode(nodeName, serverAddr))

	configFile := createTempConfigFile(t, fmt.Sprintf(`
nodes:
  - name: %s
    grpc_address: %s
`, nodeName, serverAddr))
	defer os.Remove(configFile)

	svc, err := NewService(Config{
		FilePath:      configFile,
		GRPCTimeout:   testGRPCTimeout,
		FetchInterval: testFetchInterval,
	})
	require.NoError(t, err)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, svc.Start(ctx))

	nodes, listErr := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
	require.NoError(t, listErr)
	require.Len(t, nodes, 1)
	assert.Equal(t, nodeName, nodes[0].GetMetadata().GetName())
	assert.Equal(t, serverAddr, nodes[0].GetGrpcAddress())

	nodeFromCache, getErr := svc.GetNode(ctx, nodeName)
	require.NoError(t, getErr)
	assert.Equal(t, nodeName, nodeFromCache.GetMetadata().GetName())
}

func TestHandlerNotifications(t *testing.T) {
	listenerOne, grpcServerOne, nodeServerOne := startMockGRPCServer(t)
	defer grpcServerOne.Stop()
	defer listenerOne.Close()
	addrOne := listenerOne.Addr().String()
	nodeServerOne.setNode(newTestNode("node-one", addrOne))

	listenerTwo, grpcServerTwo, nodeServerTwo := startMockGRPCServer(t)
	defer grpcServerTwo.Stop()
	defer listenerTwo.Close()
	addrTwo := listenerTwo.Addr().String()
	nodeServerTwo.setNode(newTestNode("node-two", addrTwo))

	configFile := createTempConfigFile(t, fmt.Sprintf(`
nodes:
  - name: node-one
    grpc_address: %s
`, addrOne))
	defer os.Remove(configFile)

	svc, err := NewService(Config{
		FilePath:      configFile,
		GRPCTimeout:   testGRPCTimeout,
		FetchInterval: testFetchInterval,
	})
	require.NoError(t, err)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	added := make([]string, 0)
	deleted := make([]string, 0)
	handler := &testEventHandler{
		onAdd: func(metadata schema.Metadata) {
			mu.Lock()
			defer mu.Unlock()
			added = append(added, metadata.Name)
		},
		onDelete: func(metadata schema.Metadata) {
			mu.Lock()
			defer mu.Unlock()
			deleted = append(deleted, metadata.Name)
		},
	}
	svc.RegisterHandler("test", handler)

	require.NoError(t, svc.Start(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(added) == 1 && added[0] == "node-one"
	}, 3*time.Second, 50*time.Millisecond)

	updateConfigFile(t, configFile, fmt.Sprintf(`
nodes:
  - name: node-two
    grpc_address: %s
`, addrTwo))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(added) == 2 && added[1] == "node-two" && len(deleted) == 1 && deleted[0] == "node-one"
	}, 5*time.Second, 50*time.Millisecond)

	nodes, listErr := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
	require.NoError(t, listErr)
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-two", nodes[0].GetMetadata().GetName())
}

func TestListNodeRoleFilter(t *testing.T) {
	listener, grpcServer, nodeServer := startMockGRPCServer(t)
	defer grpcServer.Stop()
	defer listener.Close()
	addr := listener.Addr().String()
	nodeServer.setNode(newTestNode("role-test-node", addr))

	configFile := createTempConfigFile(t, fmt.Sprintf(`
nodes:
  - name: role-test-node
    grpc_address: %s
`, addr))
	defer os.Remove(configFile)

	svc, err := NewService(Config{
		FilePath:      configFile,
		GRPCTimeout:   testGRPCTimeout,
		FetchInterval: testFetchInterval,
	})
	require.NoError(t, err)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, svc.Start(ctx))

	nodes, listErr := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
	require.NoError(t, listErr)
	require.Len(t, nodes, 1)

	nodes, listErr = svc.ListNode(ctx, databasev1.Role_ROLE_DATA)
	require.NoError(t, listErr)
	assert.Empty(t, nodes)
}

func TestGetNode(t *testing.T) {
	listener, grpcServer, nodeServer := startMockGRPCServer(t)
	defer grpcServer.Stop()
	defer listener.Close()
	addr := listener.Addr().String()
	nodeServer.setNode(newTestNode("cached-node", addr))

	configFile := createTempConfigFile(t, fmt.Sprintf(`
nodes:
  - name: cached-node
    grpc_address: %s
`, addr))
	defer os.Remove(configFile)

	svc, err := NewService(Config{
		FilePath:      configFile,
		GRPCTimeout:   testGRPCTimeout,
		FetchInterval: testFetchInterval,
	})
	require.NoError(t, err)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, svc.Start(ctx))

	node, getErr := svc.GetNode(ctx, addr)
	require.NoError(t, getErr)
	assert.Equal(t, "cached-node", node.GetMetadata().GetName())

	_, getErr = svc.GetNode(ctx, "non-existent")
	require.Error(t, getErr)
	assert.Contains(t, getErr.Error(), "not found")
}

func TestConcurrentAccess(t *testing.T) {
	listener, grpcServer, nodeServer := startMockGRPCServer(t)
	defer grpcServer.Stop()
	defer listener.Close()
	addr := listener.Addr().String()
	nodeServer.setNode(newTestNode("concurrent-node", addr))

	configFile := createTempConfigFile(t, fmt.Sprintf(`
nodes:
  - name: concurrent-node
    grpc_address: %s
`, addr))
	defer os.Remove(configFile)

	svc, err := NewService(Config{
		FilePath:      configFile,
		GRPCTimeout:   testGRPCTimeout,
		FetchInterval: testFetchInterval,
	})
	require.NoError(t, err)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, svc.Start(ctx))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, errList := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED); errList != nil {
					t.Errorf("list node: %v", errList)
				}
				if _, errGet := svc.GetNode(ctx, addr); errGet != nil {
					t.Errorf("get node: %v", errGet)
				}
			}
		}()
	}

	wg.Wait()
}

func createTempConfigFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "nodes.yaml")
	require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0o600))
	return tmpFile
}

func updateConfigFile(t *testing.T, path string, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func newTestNode(name, address string) *databasev1.Node {
	return &databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: name,
		},
		GrpcAddress: address,
		CreatedAt:   timestamppb.Now(),
	}
}

type testEventHandler struct {
	onAdd    func(metadata schema.Metadata)
	onDelete func(metadata schema.Metadata)
}

func (h *testEventHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
}

func (h *testEventHandler) OnAddOrUpdate(metadata schema.Metadata) {
	if h.onAdd != nil {
		h.onAdd(metadata)
	}
}

func (h *testEventHandler) OnDelete(metadata schema.Metadata) {
	if h.onDelete != nil {
		h.onDelete(metadata)
	}
}

type mockNodeQueryServer struct {
	databasev1.UnimplementedNodeQueryServiceServer
	node *databasev1.Node
	mu   sync.RWMutex
}

func (m *mockNodeQueryServer) GetCurrentNode(_ context.Context, _ *databasev1.GetCurrentNodeRequest) (*databasev1.GetCurrentNodeResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.node == nil {
		return nil, fmt.Errorf("no node available")
	}
	return &databasev1.GetCurrentNodeResponse{Node: m.node}, nil
}

func (m *mockNodeQueryServer) setNode(node *databasev1.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.node = node
}

func startMockGRPCServer(t *testing.T) (net.Listener, *grpc.Server, *mockNodeQueryServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mockServer := &mockNodeQueryServer{}
	grpcServer := grpc.NewServer()
	databasev1.RegisterNodeQueryServiceServer(grpcServer, mockServer)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return listener, grpcServer, mockServer
}
