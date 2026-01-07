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

// Package file implements file-based node discovery for distributed metadata management.
package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Service implements file-based node discovery.
type Service struct {
	nodeCache     map[string]*databasev1.Node
	closer        *run.Closer
	log           *logger.Logger
	metrics       *metrics
	handlers      map[string]schema.EventHandler
	filePath      string
	grpcTimeout   time.Duration
	fetchInterval time.Duration
	cacheMutex    sync.RWMutex
	handlersMutex sync.RWMutex
}

// Config holds configuration for file discovery service.
type Config struct {
	FilePath      string
	GRPCTimeout   time.Duration
	FetchInterval time.Duration
}

// NodeFileConfig represents the YAML configuration file structure.
type NodeFileConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

// NodeConfig represents a single node configuration.
type NodeConfig struct {
	Name       string `yaml:"name"`
	Address    string `yaml:"grpc_address"`
	CACertPath string `yaml:"ca_cert_path"`
	TLSEnabled bool   `yaml:"tls_enabled"`
}

// NewService creates a new file discovery service.
func NewService(cfg Config) (*Service, error) {
	if cfg.FilePath == "" {
		return nil, errors.New("file path cannot be empty")
	}

	// validate file exists and is readable
	if _, err := os.Stat(cfg.FilePath); err != nil {
		return nil, fmt.Errorf("failed to access file path %s: %w", cfg.FilePath, err)
	}

	svc := &Service{
		filePath:      cfg.FilePath,
		nodeCache:     make(map[string]*databasev1.Node),
		handlers:      make(map[string]schema.EventHandler),
		closer:        run.NewCloser(1),
		log:           logger.GetLogger("metadata-discovery-file"),
		grpcTimeout:   cfg.GRPCTimeout,
		fetchInterval: cfg.FetchInterval,
	}

	return svc, nil
}

// Start begins the file discovery background process.
func (s *Service) Start(ctx context.Context) error {
	s.log.Debug().Str("file_path", s.filePath).Msg("Starting file-based node discovery service")

	// initial load
	if err := s.loadAndParseFile(ctx); err != nil {
		return fmt.Errorf("failed to load initial configuration: %w", err)
	}

	// start periodic fetch loop
	go s.periodicFetch(ctx)

	return nil
}

func (s *Service) loadAndParseFile(ctx context.Context) error {
	startTime := time.Now()
	var parseErr error
	defer func() {
		if s.metrics != nil {
			duration := time.Since(startTime)
			s.metrics.fileLoadCount.Inc(1)
			s.metrics.fileLoadDuration.Observe(duration.Seconds())
			if parseErr != nil {
				s.metrics.fileLoadFailedCount.Inc(1)
			}
		}
	}()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		parseErr = fmt.Errorf("failed to read file: %w", err)
		return parseErr
	}

	var cfg NodeFileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		parseErr = fmt.Errorf("failed to parse YAML: %w", err)
		return parseErr
	}

	// validate required fields
	for idx, node := range cfg.Nodes {
		if node.Address == "" {
			parseErr = fmt.Errorf("node %s at index %d is missing required field: grpc_address", node.Name, idx)
			return parseErr
		}
		if node.TLSEnabled && node.CACertPath == "" {
			parseErr = fmt.Errorf("node %s at index %d has TLS enabled but missing ca_cert_path", node.Name, idx)
			return parseErr
		}
	}

	// update cache
	s.updateNodeCache(ctx, cfg.Nodes)

	s.log.Debug().Int("node_count", len(cfg.Nodes)).Msg("Successfully loaded configuration file")
	return nil
}

func (s *Service) fetchNodeMetadata(ctx context.Context, nodeConfig NodeConfig) (*databasev1.Node, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, s.grpcTimeout)
	defer cancel()

	// prepare TLS options
	dialOpts, err := grpchelper.SecureOptions(nil, nodeConfig.TLSEnabled, false, nodeConfig.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS config for node %s: %w", nodeConfig.Name, err)
	}

	// connect to node
	// nolint:contextcheck
	conn, connErr := grpchelper.Conn(nodeConfig.Address, s.grpcTimeout, dialOpts...)
	if connErr != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", nodeConfig.Address, connErr)
	}
	defer conn.Close()

	// query metadata of the node
	client := databasev1.NewNodeQueryServiceClient(conn)
	resp, callErr := client.GetCurrentNode(ctxTimeout, &databasev1.GetCurrentNodeRequest{})
	if callErr != nil {
		return nil, fmt.Errorf("failed to get current node from %s: %w", nodeConfig.Address, callErr)
	}

	return resp.GetNode(), nil
}

func (s *Service) updateNodeCache(ctx context.Context, newNodes []NodeConfig) {
	for _, n := range newNodes {
		s.cacheMutex.RLock()
		_, exists := s.nodeCache[n.Address]
		s.cacheMutex.RUnlock()

		if !exists {
			// fetch node metadata from gRPC
			node, fetchErr := s.fetchNodeMetadata(ctx, n)
			if fetchErr != nil {
				s.log.Warn().
					Err(fetchErr).
					Str("node", n.Name).
					Str("address", n.Address).
					Msg("Failed to fetch node metadata, will skip")
				continue
			}

			s.cacheMutex.Lock()
			if _, alreadyAdded := s.nodeCache[n.Address]; !alreadyAdded {
				s.nodeCache[n.Address] = node

				// notify handlers after releasing lock
				s.notifyHandlers(schema.Metadata{
					TypeMeta: schema.TypeMeta{
						Kind: schema.KindNode,
						Name: node.GetMetadata().GetName(),
					},
					Spec: node,
				}, true)

				s.log.Debug().
					Str("address", n.Address).
					Str("name", node.GetMetadata().GetName()).
					Msg("New node discovered and added to cache")
			}
			s.cacheMutex.Unlock()
		}
	}

	// collect nodes to delete first
	allAddr := make(map[string]bool)
	for _, n := range newNodes {
		allAddr[n.Address] = true
	}
	s.cacheMutex.Lock()
	nodesToDelete := make(map[string]*databasev1.Node)
	for addr, node := range s.nodeCache {
		if !allAddr[addr] {
			nodesToDelete[addr] = node
		}
	}

	// delete from cache while still holding lock
	for addr, node := range nodesToDelete {
		delete(s.nodeCache, addr)
		s.log.Debug().
			Str("address", addr).
			Str("name", node.GetMetadata().GetName()).
			Msg("Node removed from cache (no longer in file)")
	}
	cacheSize := len(s.nodeCache)
	s.cacheMutex.Unlock()

	// Notify handlers after releasing lock
	for _, node := range nodesToDelete {
		s.notifyHandlers(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
				Name: node.GetMetadata().GetName(),
			},
			Spec: node,
		}, false)
	}
	// update metrics
	if s.metrics != nil {
		s.metrics.totalNodesCount.Set(float64(cacheSize))
	}
}

func (s *Service) notifyHandlers(metadata schema.Metadata, isAddOrUpdate bool) {
	s.handlersMutex.RLock()
	defer s.handlersMutex.RUnlock()

	for _, handler := range s.handlers {
		if isAddOrUpdate {
			handler.OnAddOrUpdate(metadata)
		} else {
			handler.OnDelete(metadata)
		}
	}
}

// RegisterHandler registers an event handler for node changes.
func (s *Service) RegisterHandler(name string, handler schema.EventHandler) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()

	s.handlers[name] = handler
	s.log.Debug().Str("handler", name).Msg("Registered file node discovery handler")
}

// SetMetrics sets the OMR metrics.
func (s *Service) SetMetrics(factory observability.Factory) {
	s.metrics = newMetrics(factory)
}

// Close stops the file discovery service.
func (s *Service) Close() error {
	s.closer.Done()
	s.closer.CloseThenWait()

	return nil
}

// ListNode lists all existing nodes from cache.
func (s *Service) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()

	var result []*databasev1.Node
	for _, node := range s.nodeCache {
		// filter by role if specified
		if role != databasev1.Role_ROLE_UNSPECIFIED {
			hasRole := false
			for _, nodeRole := range node.GetRoles() {
				if nodeRole == role {
					hasRole = true
					break
				}
			}
			if !hasRole {
				continue
			}
		}
		result = append(result, node)
	}

	return result, nil
}

// GetNode gets a specific node from cache.
func (s *Service) GetNode(_ context.Context, nodeName string) (*databasev1.Node, error) {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()

	for _, node := range s.nodeCache {
		if node.GetMetadata() != nil && node.GetMetadata().GetName() == nodeName {
			return node, nil
		}
	}

	return nil, fmt.Errorf("node %s not found", nodeName)
}

// RegisterNode is not supported in file discovery mode.
func (s *Service) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return errors.New("manual node registration not supported in file discovery mode")
}

// UpdateNode is not supported in file discovery mode.
func (s *Service) UpdateNode(_ context.Context, _ *databasev1.Node) error {
	return errors.New("manual node update not supported in file discovery mode")
}

func (s *Service) periodicFetch(ctx context.Context) {
	fetchTicker := time.NewTicker(s.fetchInterval)
	defer fetchTicker.Stop()

	for {
		select {
		case <-fetchTicker.C:
			if err := s.loadAndParseFile(ctx); err != nil {
				s.log.Warn().Err(err).Msg("Failed to reload configuration file")
			}
		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}
