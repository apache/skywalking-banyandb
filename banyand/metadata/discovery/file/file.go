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
	"gopkg.in/yaml.v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// nodeRetryState tracks backoff retry state for a single node.
type nodeRetryState struct {
	nextRetryTime  time.Time
	lastError      error
	config         NodeConfig
	attemptCount   int
	currentBackoff time.Duration
}

// retryConfig holds retry backoff configuration.
type retryConfig struct {
	initialInterval time.Duration // Initial retry interval
	maxInterval     time.Duration // Maximum retry interval
	multiplier      float64       // Backoff growth factor
}

// Service implements file-based node discovery.
type Service struct {
	retryQueue    map[string]*nodeRetryState
	closer        *run.Closer
	log           *logger.Logger
	metrics       *metrics
	watcher       *fsnotify.Watcher
	nodeCache     map[string]*databasev1.Node
	filePath      string
	handlers      []schema.EventHandler
	retryCfg      retryConfig
	grpcTimeout   time.Duration
	fetchInterval time.Duration
	cacheMutex    sync.RWMutex
	handlersMutex sync.RWMutex
	retryMutex    sync.RWMutex
	reloadMutex   sync.Mutex
	started       bool
}

// Config holds configuration for file discovery service.
type Config struct {
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
		filePath:      cfg.FilePath,
		nodeCache:     make(map[string]*databasev1.Node),
		retryQueue:    make(map[string]*nodeRetryState),
		handlers:      make([]schema.EventHandler, 0),
		closer:        run.NewCloser(2),
		log:           logger.GetLogger("metadata-discovery-file"),
		grpcTimeout:   cfg.GRPCTimeout,
		fetchInterval: cfg.FetchInterval,
		watcher:       fileWatcher,
		retryCfg: retryConfig{
			initialInterval: cfg.RetryInitialInterval,
			maxInterval:     cfg.RetryMaxInterval,
			multiplier:      cfg.RetryMultiplier,
		},
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
	// build set of addresses from file
	fileAddresses := make(map[string]NodeConfig)
	for _, nodeConfig := range newNodes {
		fileAddresses[nodeConfig.Address] = nodeConfig
	}

	// process new or updated nodes
	for _, nodeConfig := range newNodes {
		s.cacheMutex.RLock()
		_, existsInCache := s.nodeCache[nodeConfig.Address]
		s.cacheMutex.RUnlock()
		// node already connected, skip
		if existsInCache {
			continue
		}

		s.retryMutex.RLock()
		retryState, existsInRetry := s.retryQueue[nodeConfig.Address]
		s.retryMutex.RUnlock()

		// node in retry queue - reset if config changed
		if existsInRetry {
			// check if config changed (TLS settings, name, etc)
			if s.nodeConfigChanged(retryState.config, nodeConfig) {
				s.log.Info().
					Str("address", nodeConfig.Address).
					Msg("Node configuration changed, resetting retry state")
				s.retryMutex.Lock()
				delete(s.retryQueue, nodeConfig.Address)
				s.retryMutex.Unlock()
			} else {
				// config unchanged, let retry scheduler handle it
				continue
			}
		}

		// try to fetch metadata for new node
		node, fetchErr := s.fetchNodeMetadata(ctx, nodeConfig)
		if fetchErr != nil {
			s.log.Warn().
				Err(fetchErr).
				Str("node", nodeConfig.Name).
				Str("address", nodeConfig.Address).
				Msg("Failed to fetch node metadata, adding to retry queue")

			// add to retry queue with initial backoff
			s.retryMutex.Lock()
			s.retryQueue[nodeConfig.Address] = &nodeRetryState{
				config:         nodeConfig,
				attemptCount:   1,
				nextRetryTime:  time.Now().Add(s.retryCfg.initialInterval),
				currentBackoff: s.retryCfg.initialInterval,
				lastError:      fetchErr,
			}
			s.retryMutex.Unlock()

			if s.metrics != nil {
				s.metrics.nodeRetryCount.Inc(1)
			}
			continue
		}

		// successfully fetched - add to cache
		var added bool
		var metadata schema.Metadata

		s.cacheMutex.Lock()
		if _, alreadyAdded := s.nodeCache[nodeConfig.Address]; !alreadyAdded {
			s.nodeCache[nodeConfig.Address] = node
			added = true
			metadata = schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
					Name: node.GetMetadata().GetName(),
				},
				Spec: node,
			}
		}
		s.cacheMutex.Unlock()

		if added {
			// notify handlers
			s.notifyHandlers(metadata, true)

			s.log.Debug().
				Str("address", nodeConfig.Address).
				Str("name", node.GetMetadata().GetName()).
				Msg("Node discovered and added to cache")

			if s.metrics != nil {
				s.metrics.nodeConnectedCount.Inc(1)
			}
		}
	}

	// clean up nodes removed from file
	s.cacheMutex.Lock()
	nodesToDelete := make(map[string]*databasev1.Node)
	for addr, node := range s.nodeCache {
		if _, inFile := fileAddresses[addr]; !inFile {
			nodesToDelete[addr] = node
		}
	}
	for addr := range nodesToDelete {
		delete(s.nodeCache, addr)
	}
	cacheSize := len(s.nodeCache)
	s.cacheMutex.Unlock()

	// also clean up retry queue for removed nodes
	s.retryMutex.Lock()
	for addr := range s.retryQueue {
		if _, inFile := fileAddresses[addr]; !inFile {
			delete(s.retryQueue, addr)
			s.log.Debug().
				Str("address", addr).
				Msg("Removed node from retry queue (no longer in file)")
		}
	}
	retryQueueSize := len(s.retryQueue)
	s.retryMutex.Unlock()

	// notify handlers for deletions
	for _, node := range nodesToDelete {
		s.notifyHandlers(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
				Name: node.GetMetadata().GetName(),
			},
			Spec: node,
		}, false)

		s.log.Debug().
			Str("address", node.GetGrpcAddress()).
			Str("name", node.GetMetadata().GetName()).
			Msg("Node removed from cache (no longer in file)")
	}

	// update metrics
	if s.metrics != nil {
		s.metrics.totalNodesCount.Set(float64(cacheSize))
		s.metrics.retryQueueSize.Set(float64(retryQueueSize))
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

	s.handlers = append(s.handlers, handler)
	s.log.Debug().Str("handler", name).Msg("Registered file node discovery handler")
}

// SetMetrics sets the OMR metrics.
func (s *Service) SetMetrics(factory observability.Factory) {
	s.metrics = newMetrics(factory)
}

// Close stops the file discovery service.
func (s *Service) Close() error {
	s.log.Debug().Msg("Closing file discovery service")

	// close fsnotify watcher
	if s.watcher != nil {
		if err := s.watcher.Close(); err != nil {
			s.log.Error().Err(err).Msg("Failed to close fsnotify watcher")
		}
	}

	// only wait for goroutines if Start() was called
	if s.started {
		s.closer.CloseThenWait()
	}

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

func (s *Service) watchFileChanges(ctx context.Context) {
	defer s.closer.Done()

	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				s.log.Info().Msg("fsnotify watcher events channel closed")
				return
			}

			s.log.Debug().
				Str("file", event.Name).
				Str("op", event.Op.String()).
				Msg("Detected node metadata file changed event")

			// handle relevant events (Write, Create, Remove, Rename)
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				// re-add file to watcher if it was removed/renamed
				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					s.log.Info().Str("file", event.Name).Msg("File removed/renamed, re-adding to watcher")
					_ = s.watcher.Remove(s.filePath)

					// wait briefly for file operations to complete
					time.Sleep(1 * time.Second)

					// try to re-add with retry
					maxRetries := 5
					for attempt := 0; attempt < maxRetries; attempt++ {
						if addErr := s.watcher.Add(s.filePath); addErr == nil {
							s.log.Debug().Str("file", s.filePath).Msg("Re-added file to watcher")
							break
						} else if attempt == maxRetries-1 {
							s.log.Error().
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
					s.log.Warn().Err(reloadErr).Msg("Failed to reload configuration file (fsnotify)")
				} else {
					s.log.Info().Msg("Configuration file reloaded successfully (fsnotify)")
				}
			}

		case watchErr, ok := <-s.watcher.Errors:
			if !ok {
				s.log.Info().Msg("fsnotify watcher errors channel closed")
				return
			}
			s.log.Error().Err(watchErr).Msg("Error from fsnotify watcher")
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
				s.log.Warn().Err(err).Msg("Failed to reload configuration file (periodic)")
			} else {
				s.log.Debug().Msg("Configuration file reloaded successfully (periodic)")
			}
		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) retryScheduler(ctx context.Context) {
	// check retry queue every second
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-checkTicker.C:
			s.processRetryQueue(ctx)

		case <-s.closer.CloseNotify():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) processRetryQueue(ctx context.Context) {
	now := time.Now()

	s.retryMutex.RLock()
	// collect nodes ready for retry
	nodesToRetry := make([]*nodeRetryState, 0)
	for _, retryState := range s.retryQueue {
		if retryState.nextRetryTime.Before(now) || retryState.nextRetryTime.Equal(now) {
			nodesToRetry = append(nodesToRetry, retryState)
		}
	}
	s.retryMutex.RUnlock()

	if len(nodesToRetry) == 0 {
		return
	}

	s.log.Debug().Int("count", len(nodesToRetry)).Msg("Processing retry queue")

	// attempt retry for each node
	for _, retryState := range nodesToRetry {
		s.attemptNodeRetry(ctx, retryState)
	}
}

func (s *Service) attemptNodeRetry(ctx context.Context, retryState *nodeRetryState) {
	addr := retryState.config.Address

	s.log.Debug().
		Str("address", addr).
		Int("attempt", retryState.attemptCount).
		Dur("backoff", retryState.currentBackoff).
		Msg("Attempting node metadata fetch retry")

	// try to fetch metadata
	node, fetchErr := s.fetchNodeMetadata(ctx, retryState.config)

	if fetchErr != nil {
		// retry failed - update backoff
		retryState.attemptCount++
		retryState.lastError = fetchErr

		// calculate next backoff interval: currentBackoff * multiplier
		nextBackoff := time.Duration(float64(retryState.currentBackoff) * s.retryCfg.multiplier)
		if nextBackoff > s.retryCfg.maxInterval {
			nextBackoff = s.retryCfg.maxInterval
		}
		retryState.currentBackoff = nextBackoff
		retryState.nextRetryTime = time.Now().Add(nextBackoff)

		s.log.Warn().
			Err(fetchErr).
			Str("address", addr).
			Int("attempt", retryState.attemptCount).
			Dur("next_backoff", nextBackoff).
			Time("next_retry", retryState.nextRetryTime).
			Msg("Node metadata fetch retry failed, scheduling next retry")

		s.retryMutex.Lock()
		s.retryQueue[addr] = retryState
		s.retryMutex.Unlock()

		if s.metrics != nil {
			s.metrics.nodeRetryFailedCount.Inc(1)
		}
		return
	}

	// success - move to connected state
	s.cacheMutex.Lock()
	s.nodeCache[addr] = node
	s.cacheMutex.Unlock()

	// remove from retry queue
	s.retryMutex.Lock()
	delete(s.retryQueue, addr)
	retryQueueSize := len(s.retryQueue)
	s.retryMutex.Unlock()

	// notify handlers
	s.notifyHandlers(schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind: schema.KindNode,
			Name: node.GetMetadata().GetName(),
		},
		Spec: node,
	}, true)

	s.log.Info().
		Str("address", addr).
		Str("name", node.GetMetadata().GetName()).
		Int("total_attempts", retryState.attemptCount).
		Msg("Node metadata fetched successfully after retry")

	// update metrics
	if s.metrics != nil {
		s.metrics.nodeConnectedCount.Inc(1)
		s.metrics.retryQueueSize.Set(float64(retryQueueSize))
		s.metrics.nodeRetrySuccessCount.Inc(1)
	}
}

func (s *Service) nodeConfigChanged(older, newer NodeConfig) bool {
	return older.Name != newer.Name ||
		older.Address != newer.Address ||
		older.CACertPath != newer.CACertPath ||
		older.TLSEnabled != newer.TLSEnabled
}
