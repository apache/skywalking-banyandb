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

// Package dns implements DNS-based node discovery for distributed metadata management.
package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

// Service implements DNS-based node discovery.
type Service struct {
	lastQueryTime        time.Time
	resolver             Resolver
	pathToReloader       map[string]*pkgtls.Reloader
	srvIndexToPath       map[int]string
	resolvedAddrToSRVIdx map[string]int
	nodeCache            map[string]*databasev1.Node
	closer               *run.Closer
	log                  *logger.Logger
	metrics              *metrics
	handlers             map[string]schema.EventHandler
	caCertPaths          []string
	srvAddresses         []string
	lastSuccessfulDNS    []string
	pollInterval         time.Duration
	initInterval         time.Duration
	initDuration         time.Duration
	grpcTimeout          time.Duration
	cacheMutex           sync.RWMutex
	handlersMutex        sync.RWMutex
	lastQueryMutex       sync.RWMutex
	resolvedAddrMutex    sync.RWMutex
	tlsEnabled           bool
}

// Config holds configuration for DNS discovery service.
type Config struct {
	CACertPaths  []string
	SRVAddresses []string
	InitInterval time.Duration
	InitDuration time.Duration
	PollInterval time.Duration
	GRPCTimeout  time.Duration
	TLSEnabled   bool
}

// Resolver defines the interface for DNS SRV lookups.
type Resolver interface {
	LookupSRV(ctx context.Context, name string) (string, []*net.SRV, error)
}

// defaultResolver wraps net.DefaultResolver to implement Resolver.
type defaultResolver struct{}

func (d *defaultResolver) LookupSRV(ctx context.Context, name string) (string, []*net.SRV, error) {
	return net.DefaultResolver.LookupSRV(ctx, "", "", name)
}

// NewService creates a new DNS discovery service.
func NewService(cfg Config) (*Service, error) {
	// validation
	if len(cfg.SRVAddresses) == 0 {
		return nil, errors.New("DNS SRV addresses cannot be empty")
	}

	// validate CA cert paths match SRV addresses when TLS is enabled
	if cfg.TLSEnabled {
		if len(cfg.CACertPaths) != len(cfg.SRVAddresses) {
			return nil, fmt.Errorf("number of CA cert paths (%d) must match number of SRV addresses (%d)",
				len(cfg.CACertPaths), len(cfg.SRVAddresses))
		}
	}

	svc := &Service{
		srvAddresses:         cfg.SRVAddresses,
		initInterval:         cfg.InitInterval,
		initDuration:         cfg.InitDuration,
		pollInterval:         cfg.PollInterval,
		grpcTimeout:          cfg.GRPCTimeout,
		tlsEnabled:           cfg.TLSEnabled,
		caCertPaths:          cfg.CACertPaths,
		nodeCache:            make(map[string]*databasev1.Node),
		handlers:             make(map[string]schema.EventHandler),
		lastSuccessfulDNS:    []string{},
		pathToReloader:       make(map[string]*pkgtls.Reloader),
		srvIndexToPath:       make(map[int]string),
		resolvedAddrToSRVIdx: make(map[string]int),
		closer:               run.NewCloser(1),
		log:                  logger.GetLogger("metadata-discovery-dns"),
		resolver:             &defaultResolver{},
	}

	// create shared reloaders for CA certificates
	if svc.tlsEnabled {
		for srvIdx, certPath := range cfg.CACertPaths {
			// Store the SRV index → cert path mapping
			svc.srvIndexToPath[srvIdx] = certPath

			// check if we already have a Reloader for this path
			if _, exists := svc.pathToReloader[certPath]; exists {
				svc.log.Debug().Str("certPath", certPath).Int("srvIndex", srvIdx).
					Msg("Reusing existing CA certificate reloader")
				continue
			}

			// create new Reloader for this unique path
			reloader, reloaderErr := pkgtls.NewClientCertReloader(certPath, svc.log)
			if reloaderErr != nil {
				// clean up any already-created reloaders
				for _, r := range svc.pathToReloader {
					r.Stop()
				}
				return nil, fmt.Errorf("failed to initialize CA certificate reloader for path %s (SRV index %d): %w",
					certPath, srvIdx, reloaderErr)
			}

			svc.pathToReloader[certPath] = reloader
			svc.log.Info().Str("certPath", certPath).Int("srvIndex", srvIdx).
				Str("srvAddress", cfg.SRVAddresses[srvIdx]).Msg("Initialized DNS CA certificate reloader")
		}
	}

	return svc, nil
}

// newServiceWithResolver creates a service with a custom resolver (for testing).
func newServiceWithResolver(cfg Config, resolver Resolver) (*Service, error) {
	svc, err := NewService(cfg)
	if err != nil {
		return nil, err
	}
	svc.resolver = resolver
	return svc, nil
}

func (s *Service) getTLSDialOptions(address string) ([]grpc.DialOption, error) {
	if !s.tlsEnabled {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	// look up which Reloader to use for this address
	if len(s.pathToReloader) > 0 {
		// Find which SRV address this resolved address came from
		s.resolvedAddrMutex.RLock()
		srvIdx, addrExists := s.resolvedAddrToSRVIdx[address]
		s.resolvedAddrMutex.RUnlock()

		if !addrExists {
			return nil, fmt.Errorf("no SRV mapping found for address %s", address)
		}

		// look up the cert path for this SRV index
		certPath, pathExists := s.srvIndexToPath[srvIdx]
		if !pathExists {
			return nil, fmt.Errorf("no cert path found for SRV index %d (address %s)", srvIdx, address)
		}

		// get the Reloader for this cert path
		reloader, reloaderExists := s.pathToReloader[certPath]
		if !reloaderExists {
			return nil, fmt.Errorf("no reloader found for cert path %s (address %s)", certPath, address)
		}

		// get fresh TLS config from the Reloader
		tlsConfig, configErr := reloader.GetClientTLSConfig("")
		if configErr != nil {
			return nil, fmt.Errorf("failed to get TLS config from reloader for address %s: %w", address, configErr)
		}

		creds := credentials.NewTLS(tlsConfig)
		return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
	}

	// fallback to static TLS config (when no reloaders configured)
	opts, err := grpchelper.SecureOptions(nil, s.tlsEnabled, false, "")
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS config: %w", err)
	}
	return opts, nil
}

// Start begins the DNS discovery background process.
func (s *Service) Start(ctx context.Context) error {
	s.log.Debug().Msg("Starting DNS-based node discovery service")

	// start all Reloaders
	if len(s.pathToReloader) > 0 {
		startedReloaders := make([]*pkgtls.Reloader, 0, len(s.pathToReloader))

		for certPath, reloader := range s.pathToReloader {
			if startErr := reloader.Start(); startErr != nil {
				// stop any already-started reloaders
				for _, r := range startedReloaders {
					r.Stop()
				}
				return fmt.Errorf("failed to start CA certificate reloader for path %s: %w", certPath, startErr)
			}
			startedReloaders = append(startedReloaders, reloader)
			s.log.Debug().Str("certPath", certPath).Msg("Started CA certificate reloader")
		}
	}

	go s.discoveryLoop(ctx)

	return nil
}

func (s *Service) discoveryLoop(ctx context.Context) {
	// add the init phase finish time
	initPhaseEnd := time.Now().Add(s.initDuration)

	for {
		if err := s.queryDNSAndUpdateNodes(ctx); err != nil {
			s.log.Err(err).Msg("failed to query DNS and update nodes")
		}

		// wait for next interval
		var interval time.Duration
		if time.Now().Before(initPhaseEnd) {
			interval = s.initInterval
		} else {
			interval = s.pollInterval
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.closer.CloseNotify():
			timer.Stop()
			return
		case <-timer.C:
			// continue to next iteration
		}
	}
}

func (s *Service) queryDNSAndUpdateNodes(ctx context.Context) error {
	// Record summary metrics
	startTime := time.Now()
	defer func() {
		if s.metrics != nil {
			duration := time.Since(startTime)
			s.metrics.discoveryCount.Inc(1)
			s.metrics.discoveryDuration.Observe(duration.Seconds())
			s.metrics.discoveryTotalDuration.Inc(duration.Seconds())
		}
	}()

	addresses, queryErr := s.queryAllSRVRecords(ctx)

	if queryErr != nil {
		s.log.Warn().Err(queryErr).Msg("DNS query failed, using last successful cache")
		addresses = s.lastSuccessfulDNS
		if len(addresses) == 0 {
			if s.metrics != nil {
				s.metrics.discoveryFailedCount.Inc(1)
			}
			return fmt.Errorf("DNS query failed and no cached addresses available: %w", queryErr)
		}
	} else {
		s.lastSuccessfulDNS = addresses
		if s.log.Debug().Enabled() {
			s.log.Debug().
				Int("count", len(addresses)).
				Strs("addresses", addresses).
				Strs("srv_addresses", s.srvAddresses).
				Msg("DNS query successful")
		}
	}

	// Update node cache based on DNS results
	updateErr := s.updateNodeCache(ctx, addresses)
	if updateErr != nil && s.metrics != nil {
		s.metrics.discoveryFailedCount.Inc(1)
	}
	s.lastQueryMutex.Lock()
	s.lastQueryTime = time.Now()
	s.lastQueryMutex.Unlock()
	return updateErr
}

func (s *Service) queryAllSRVRecords(ctx context.Context) ([]string, error) {
	startTime := time.Now()
	defer func() {
		if s.metrics != nil {
			duration := time.Since(startTime)
			s.metrics.dnsQueryCount.Inc(1)
			s.metrics.dnsQueryDuration.Observe(duration.Seconds())
			s.metrics.dnsQueryTotalDuration.Inc(duration.Seconds())
		}
	}()

	allAddresses := make(map[string]bool)
	// track which SRV address (by index) each resolved address came from
	newAddrToSRVIdx := make(map[string]int)
	var queryErrors []error

	for srvIdx, srvAddr := range s.srvAddresses {
		_, addrs, lookupErr := s.resolver.LookupSRV(ctx, srvAddr)
		if lookupErr != nil {
			queryErrors = append(queryErrors, fmt.Errorf("lookup %s failed: %w", srvAddr, lookupErr))
			continue
		}

		for _, srv := range addrs {
			address := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
			allAddresses[address] = true

			// track which SRV address this resolved to (first-wins strategy)
			if _, exists := newAddrToSRVIdx[address]; !exists {
				newAddrToSRVIdx[address] = srvIdx
			}
		}
	}

	// if there have any error occurred,
	// then just return the query error to ignore the result to make sure the cache correct
	if len(queryErrors) > 0 {
		if s.metrics != nil {
			s.metrics.dnsQueryFailedCount.Inc(1)
		}
		return nil, errors.Join(queryErrors...)
	}

	// update the resolved address to SRV index mapping
	s.resolvedAddrMutex.Lock()
	s.resolvedAddrToSRVIdx = newAddrToSRVIdx
	s.resolvedAddrMutex.Unlock()

	// convert map to slice
	result := make([]string, 0, len(allAddresses))
	for addr := range allAddresses {
		result = append(result, addr)
	}

	return result, nil
}

func (s *Service) updateNodeCache(ctx context.Context, addresses []string) error {
	addressSet := make(map[string]bool)
	for _, addr := range addresses {
		addressSet[addr] = true
	}

	var addErrors []error

	for addr := range addressSet {
		s.cacheMutex.RLock()
		_, exists := s.nodeCache[addr]
		s.cacheMutex.RUnlock()

		if !exists {
			// fetch node metadata from gRPC
			node, fetchErr := s.fetchNodeMetadata(ctx, addr)
			if fetchErr != nil {
				s.log.Warn().
					Err(fetchErr).
					Str("address", addr).
					Msg("Failed to fetch node metadata")
				addErrors = append(addErrors, fetchErr)
				continue
			}

			s.cacheMutex.Lock()
			if _, alreadyAdded := s.nodeCache[addr]; !alreadyAdded {
				s.nodeCache[addr] = node

				// notify handlers after releasing lock
				s.notifyHandlers(schema.Metadata{
					TypeMeta: schema.TypeMeta{
						Kind: schema.KindNode,
						Name: node.GetMetadata().GetName(),
					},
					Spec: node,
				}, true)

				s.log.Debug().
					Str("address", addr).
					Str("name", node.GetMetadata().GetName()).
					Msg("New node discovered and added to cache")
			}
			s.cacheMutex.Unlock()
		}
	}

	// collect nodes to delete first
	s.cacheMutex.Lock()
	nodesToDelete := make(map[string]*databasev1.Node)
	for addr, node := range s.nodeCache {
		if !addressSet[addr] {
			nodesToDelete[addr] = node
		}
	}

	// delete from cache while still holding lock
	for addr, node := range nodesToDelete {
		delete(s.nodeCache, addr)
		s.log.Debug().
			Str("address", addr).
			Str("name", node.GetMetadata().GetName()).
			Msg("Node removed from cache (no longer in DNS)")
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

	// update total nodes metric
	if s.metrics != nil {
		s.metrics.totalNodesCount.Set(float64(cacheSize))
	}

	if len(addErrors) > 0 {
		return errors.Join(addErrors...)
	}

	return nil
}

func (s *Service) fetchNodeMetadata(ctx context.Context, address string) (*databasev1.Node, error) {
	// record gRPC query metrics
	startTime := time.Now()
	var grpcErr error
	defer func() {
		if s.metrics != nil {
			duration := time.Since(startTime)
			s.metrics.grpcQueryCount.Inc(1)
			s.metrics.grpcQueryDuration.Observe(duration.Seconds())
			s.metrics.grpcQueryTotalDuration.Inc(duration.Seconds())
			if grpcErr != nil {
				s.metrics.grpcQueryFailedCount.Inc(1)
			}
		}
	}()

	ctxTimeout, cancel := context.WithTimeout(ctx, s.grpcTimeout)
	defer cancel()

	// for TLS connections with other nodes to getting metadata
	dialOpts, err := s.getTLSDialOptions(address)
	if err != nil {
		grpcErr = fmt.Errorf("failed to get TLS dial options: %w", err)
		return nil, grpcErr
	}
	// nolint:contextcheck
	conn, connErr := grpchelper.Conn(address, s.grpcTimeout, dialOpts...)
	if connErr != nil {
		grpcErr = fmt.Errorf("failed to connect to %s: %w", address, connErr)
		return nil, grpcErr
	}
	defer conn.Close()

	// query metadata of the node
	client := databasev1.NewNodeQueryServiceClient(conn)
	resp, callErr := client.GetCurrentNode(ctxTimeout, &databasev1.GetCurrentNodeRequest{})
	if callErr != nil {
		grpcErr = fmt.Errorf("failed to get current node from %s: %w", address, callErr)
		return nil, grpcErr
	}

	return resp.GetNode(), nil
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
	s.log.Debug().Str("handler", name).Msg("Registered DNS node discovery handler")
}

// SetMetrics set the OMR metrics.
func (s *Service) SetMetrics(factory observability.Factory) {
	s.metrics = newMetrics(factory)
}

// Close stops the DNS discovery service.
func (s *Service) Close() error {
	s.closer.Done()
	s.closer.CloseThenWait()

	// stop all Reloaders
	for certPath, reloader := range s.pathToReloader {
		reloader.Stop()
		s.log.Debug().Str("certPath", certPath).Msg("Stopped CA certificate reloader")
	}

	return nil
}

// ListNode list all existing nodes from cache.
func (s *Service) ListNode(ctx context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	// if the service is haven't begun/finished, then try to query and update DNS first
	s.lastQueryMutex.RLock()
	notQueried := s.lastQueryTime.IsZero()
	s.lastQueryMutex.RUnlock()
	if notQueried {
		if err := s.queryDNSAndUpdateNodes(ctx); err != nil {
			return nil, err
		}
	}
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

// GetNode current node from cache.
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

// RegisterNode update the configurations of a node, it should not be invoked.
func (s *Service) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return errors.New("manual node registration not supported in DNS discovery mode")
}

// UpdateNode update the configurations of a node, it should not be invoked.
func (s *Service) UpdateNode(_ context.Context, _ *databasev1.Node) error {
	return errors.New("manual node update not supported in DNS discovery mode")
}
