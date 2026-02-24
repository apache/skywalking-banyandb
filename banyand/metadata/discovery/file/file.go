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

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Service implements file-based node discovery.
type Service struct {
	*common.DiscoveryServiceBase
	addressToNodeConfig map[string]NodeConfig
	closer              *run.Closer
	metrics             *metrics
	watcher             *fsnotify.Watcher
	filePath            string
	grpcTimeout         time.Duration
	fetchInterval       time.Duration
	reloadMutex         sync.Mutex
	configMutex         sync.RWMutex
	started             bool
}

// Config holds configuration for file discovery service.
type Config struct {
	OMR                  observability.MetricsRegistry
	FilePath             string
	GRPCTimeout          time.Duration
	FetchInterval        time.Duration
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration
	RetryMultiplier      float64
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

	// validate retry config
	if cfg.RetryMultiplier < 1.0 {
		return nil, fmt.Errorf("retry multiplier must be >= 1.0, got %f", cfg.RetryMultiplier)
	}
	if cfg.RetryMaxInterval < cfg.RetryInitialInterval {
		return nil, fmt.Errorf("retry max interval (%v) must be >= initial interval (%v)",
			cfg.RetryMaxInterval, cfg.RetryInitialInterval)
	}
	if cfg.RetryInitialInterval <= 0 {
		return nil, errors.New("retry initial interval must be positive")
	}

	// create fsnotify watcher
	fileWatcher, watchErr := fsnotify.NewWatcher()
	if watchErr != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", watchErr)
	}

	svc := &Service{
		filePath:            cfg.FilePath,
		addressToNodeConfig: make(map[string]NodeConfig),
		closer:              run.NewCloser(3),
		grpcTimeout:         cfg.GRPCTimeout,
		fetchInterval:       cfg.FetchInterval,
		watcher:             fileWatcher,
	}

	// initialize discovery service base
	svc.DiscoveryServiceBase = common.NewServiceBase(
		"metadata-discovery-file",
		svc,
		common.RetryConfig{
			InitialInterval: cfg.RetryInitialInterval,
			MaxInterval:     cfg.RetryMaxInterval,
			Multiplier:      cfg.RetryMultiplier,
		},
	)

	// set callbacks for retry manager
	svc.RetryManager.SetSuccessCallback(func(_ string, _ *databasev1.Node) {
		if svc.metrics != nil {
			svc.metrics.nodeConnectedCount.Inc(1)
		}
	})

	if cfg.OMR != nil {
		factory := observability.RootScope.SubScope("metadata").SubScope("file_discovery")
		svc.metrics = newMetrics(cfg.OMR.With(factory))
		svc.DiscoveryServiceBase.SetMetrics(svc.metrics)
	}

	return svc, nil
}

// GetDialOptions implements GRPCDialOptionsProvider for file-specific per-node TLS.
func (s *Service) GetDialOptions(address string) ([]grpc.DialOption, error) {
	s.configMutex.RLock()
	nodeConfig, exists := s.addressToNodeConfig[address]
	s.configMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no node configuration found for address %s", address)
	}

	return grpchelper.SecureOptions(nil, nodeConfig.TLSEnabled, false, nodeConfig.CACertPath)
}

// Start begins the file discovery background process.
func (s *Service) Start(ctx context.Context) error {
	s.GetLogger().Debug().Str("file_path", s.filePath).Msg("Starting file-based node discovery service")
	s.NodeCacheBase.StartForNotification()

	// initial load
	if err := s.loadAndParseFile(ctx); err != nil {
		return fmt.Errorf("failed to load initial configuration: %w", err)
	}

	// start fsnotify watcher
	if err := s.watcher.Add(s.filePath); err != nil {
		return fmt.Errorf("failed to add file to watcher: %w", err)
	}

	// mark as started
	s.started = true

	// start goroutines
	go s.watchFileChanges(ctx)
	go s.periodicFetch(ctx)
	go s.retryScheduler(ctx)

	return nil
}

func (s *Service) loadAndParseFile(ctx context.Context) error {
	s.reloadMutex.Lock()
	defer s.reloadMutex.Unlock()
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
		return fmt.Errorf("failed to parse YAML: %w", err)
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

	s.GetLogger().Debug().Int("node_count", len(cfg.Nodes)).Msg("Successfully loaded configuration file")
	return nil
}

// FetchNodeWithRetry implements NodeFetcher interface for retry manager.
func (s *Service) FetchNodeWithRetry(ctx context.Context, address string) (*databasev1.Node, error) {
	// use common fetcher with address
	return common.FetchNodeMetadata(ctx, address, s.grpcTimeout, s)
}

func (s *Service) fetchNodeMetadata(ctx context.Context, nodeConfig NodeConfig) (*databasev1.Node, error) {
	node, err := common.FetchNodeMetadata(ctx, nodeConfig.Address, s.grpcTimeout, s)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata for node %s: %w", nodeConfig.Name, err)
	}

	return node, nil
}

func (s *Service) updateNodeCache(ctx context.Context, newNodes []NodeConfig) {
	newAddressToNodeConfig := make(map[string]NodeConfig)
	for _, nodeConfig := range newNodes {
		newAddressToNodeConfig[nodeConfig.Address] = nodeConfig
	}

	// save old config for comparison and replace with new one
	s.configMutex.Lock()
	oldAddressToNodeConfig := s.addressToNodeConfig
	s.addressToNodeConfig = newAddressToNodeConfig
	s.configMutex.Unlock()

	// process new or updated nodes
	for _, nodeConfig := range newNodes {
		_, existsInCache := s.GetCachedNode(nodeConfig.Address)
		// node already connected, skip
		if existsInCache {
			continue
		}

		// node in retry queue - check if config changed
		if s.RetryManager.IsInRetry(nodeConfig.Address) {
			oldConfig, hasOldConfig := oldAddressToNodeConfig[nodeConfig.Address]

			if hasOldConfig && nodeConfigChanged(oldConfig, nodeConfig) {
				s.GetLogger().Info().
					Str("address", nodeConfig.Address).
					Msg("Node configuration changed, resetting retry state")
				s.RetryManager.RemoveFromRetry(nodeConfig.Address)
			} else {
				// config unchanged, let retry scheduler handle it
				continue
			}
		}

		// try to fetch metadata for new node
		node, fetchErr := s.fetchNodeMetadata(ctx, nodeConfig)
		if fetchErr != nil {
			// add to retry queue
			s.RetryManager.AddToRetry(nodeConfig.Address, fetchErr)
			continue
		}

		// successfully fetched - add to cache
		if s.AddNodeAndNotify(nodeConfig.Address, node, "Node discovered and added to cache") {
			if s.metrics != nil {
				s.metrics.nodeConnectedCount.Inc(1)
			}
		}
	}

	// find nodes to delete
	currentAddresses := s.GetAllNodeAddresses()
	addressesToDelete := make([]string, 0)
	for _, addr := range currentAddresses {
		if _, inFile := newAddressToNodeConfig[addr]; !inFile {
			addressesToDelete = append(addressesToDelete, addr)
		}
	}

	// delete nodes and notify handlers
	s.RemoveNodesAndNotify(addressesToDelete, "Node removed from cache (no longer in file)")

	// clean up retry queue for removed nodes
	validAddresses := make(map[string]bool)
	for addr := range newAddressToNodeConfig {
		validAddresses[addr] = true
	}
	s.RetryManager.CleanupRetryQueue(validAddresses)

	// update metrics
	if s.metrics != nil {
		s.metrics.totalNodesCount.Set(float64(s.GetCacheSize()))
		s.metrics.retryQueueSize.Set(float64(s.RetryManager.GetQueueSize()))
	}
}

// SetMetrics sets the OMR metrics.
func (s *Service) SetMetrics(factory observability.Factory) {
	s.metrics = newMetrics(factory)
	s.DiscoveryServiceBase.SetMetrics(s.metrics)
}

// Close stops the file discovery service.
func (s *Service) Close() error {
	s.GetLogger().Debug().Msg("Closing file discovery service")

	// close fsnotify watcher
	if s.watcher != nil {
		if err := s.watcher.Close(); err != nil {
			s.GetLogger().Error().Err(err).Msg("Failed to close fsnotify watcher")
		}
	}

	// only wait for goroutines if Start() was called
	if s.started {
		s.closer.CloseThenWait()
	}

	return nil
}

// RegisterNode is not supported in file discovery mode.
func (s *Service) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return errors.New("manual node registration not supported in file discovery mode")
}

// UpdateNode is not supported in file discovery mode.
func (s *Service) UpdateNode(_ context.Context, _ *databasev1.Node) error {
	return errors.New("manual node update not supported in file discovery mode")
}

func (s *Service) watchFileChanges(ctx context.Context) {
	defer s.closer.Done()

	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				s.GetLogger().Info().Msg("fsnotify watcher events channel closed")
				return
			}

			s.GetLogger().Debug().
				Str("file", event.Name).
				Str("op", event.Op.String()).
				Msg("Detected node metadata file changed event")

			// handle relevant events (Write, Create, Remove, Rename)
			// generally same as tlsReloader
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				// re-add file to watcher if it was removed/renamed
				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					s.GetLogger().Info().Str("file", event.Name).Msg("File removed/renamed, re-adding to watcher")
					_ = s.watcher.Remove(s.filePath)

					// wait briefly for file operations to complete
					time.Sleep(1 * time.Second)

					// try to re-add with retry
					maxRetries := 5
					for attempt := 0; attempt < maxRetries; attempt++ {
						if addErr := s.watcher.Add(s.filePath); addErr == nil {
							s.GetLogger().Debug().Str("file", s.filePath).Msg("Re-added file to watcher")
							break
						} else if attempt == maxRetries-1 {
							s.GetLogger().Error().
								Err(addErr).
								Str("file", s.filePath).
								Msg("Failed to re-add file to watcher after retries")
						}
						time.Sleep(500 * time.Millisecond)
					}
				} else {
					// wait for file stability
					time.Sleep(200 * time.Millisecond)
				}

				// immediate reload
				if reloadErr := s.loadAndParseFile(ctx); reloadErr != nil {
					s.GetLogger().Warn().Err(reloadErr).Msg("Failed to reload configuration file (fsnotify)")
				} else {
					s.GetLogger().Info().Msg("Configuration file reloaded successfully (fsnotify)")
				}
			}

		case watchErr, ok := <-s.watcher.Errors:
			if !ok {
				s.GetLogger().Info().Msg("fsnotify watcher errors channel closed")
				return
			}
			s.GetLogger().Error().Err(watchErr).Msg("Error from fsnotify watcher")
			if s.metrics != nil {
				s.metrics.fileLoadFailedCount.Inc(1)
			}

		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) periodicFetch(ctx context.Context) {
	defer s.closer.Done()

	fetchTicker := time.NewTicker(s.fetchInterval)
	defer fetchTicker.Stop()

	for {
		select {
		case <-fetchTicker.C:
			if err := s.loadAndParseFile(ctx); err != nil {
				s.GetLogger().Warn().Err(err).Msg("Failed to reload configuration file (periodic)")
			} else {
				s.GetLogger().Debug().Msg("Configuration file reloaded successfully (periodic)")
			}
		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) retryScheduler(ctx context.Context) {
	defer s.closer.Done()

	// check retry queue every second
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-checkTicker.C:
			s.RetryManager.ProcessRetryQueue(ctx)

		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}

func nodeConfigChanged(older, newer NodeConfig) bool {
	return older.Address != newer.Address ||
		older.CACertPath != newer.CACertPath ||
		older.TLSEnabled != newer.TLSEnabled
}
