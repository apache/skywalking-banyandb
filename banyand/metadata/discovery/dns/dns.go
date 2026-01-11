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
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/common"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

// Service implements DNS-based node discovery.
type Service struct {
	*common.NodeCacheBase
	lastQueryTime     time.Time
	resolver          Resolver
	pathToReloader    map[string]*pkgtls.Reloader
	srvAddrToPath     map[string]string
	closer            *run.Closer
	metrics           *metrics
	lastSuccessfulDNS map[string][]string
	srvAddresses      []string
	caCertPaths       []string
	pollInterval      time.Duration
	initInterval      time.Duration
	initDuration      time.Duration
	grpcTimeout       time.Duration
	lastQueryMutex    sync.RWMutex
	tlsEnabled        bool
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
		NodeCacheBase:     common.NewNodeCacheBase("metadata-discovery-dns"),
		srvAddresses:      cfg.SRVAddresses,
		initInterval:      cfg.InitInterval,
		initDuration:      cfg.InitDuration,
		pollInterval:      cfg.PollInterval,
		grpcTimeout:       cfg.GRPCTimeout,
		tlsEnabled:        cfg.TLSEnabled,
		caCertPaths:       cfg.CACertPaths,
		lastSuccessfulDNS: make(map[string][]string),
		pathToReloader:    make(map[string]*pkgtls.Reloader),
		srvAddrToPath:     make(map[string]string),
		closer:            run.NewCloser(1),
		resolver:          &defaultResolver{},
	}

	// create shared reloaders for CA certificates
	if svc.tlsEnabled {
		for srvIdx, certPath := range cfg.CACertPaths {
			srvAddr := cfg.SRVAddresses[srvIdx]
			svc.srvAddrToPath[srvAddr] = certPath

			// check if we already have a Reloader for this path
			if _, exists := svc.pathToReloader[certPath]; exists {
				svc.GetLogger().Debug().Str("certPath", certPath).Int("srvIndex", srvIdx).
					Msg("Reusing existing CA certificate reloader")
				continue
			}

			// create new Reloader for this unique path
			reloader, reloaderErr := pkgtls.NewClientCertReloader(certPath, svc.GetLogger())
			if reloaderErr != nil {
				// clean up any already-created reloaders
				for _, r := range svc.pathToReloader {
					r.Stop()
				}
				return nil, fmt.Errorf("failed to initialize CA certificate reloader for path %s (SRV %s): %w",
					certPath, srvAddr, reloaderErr)
			}

			svc.pathToReloader[certPath] = reloader
			svc.GetLogger().Info().Str("certPath", certPath).Int("srvIndex", srvIdx).
				Str("srvAddress", srvAddr).Msg("Initialized DNS CA certificate reloader")
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

// GetDialOptions implements GRPCDialOptionsProvider for DNS-specific TLS setup.
func (s *Service) GetDialOptions(address string) ([]grpc.DialOption, error) {
	if !s.tlsEnabled {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	// find which SRV address this node address belongs to
	var srvAddr string
	for srv := range s.srvAddrToPath {
		srvAddr = srv
		break
	}

	// look up which Reloader to use for this address
	if len(s.pathToReloader) > 0 {
		// look up the cert path for this SRV address
		certPath, pathExists := s.srvAddrToPath[srvAddr]
		if !pathExists {
			// fallback: use any available cert path
			for _, path := range s.srvAddrToPath {
				certPath = path
				break
			}
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

func (s *Service) getTLSDialOptions(srvAddr, address string) ([]grpc.DialOption, error) {
	if !s.tlsEnabled {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	// look up which Reloader to use for this address
	if len(s.pathToReloader) > 0 {
		// look up the cert path for this SRV address
		certPath, pathExists := s.srvAddrToPath[srvAddr]
		if !pathExists {
			return nil, fmt.Errorf("no cert path found for SRV %s (address %s)", srvAddr, address)
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
	s.GetLogger().Debug().Msg("Starting DNS-based node discovery service")

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
			s.GetLogger().Debug().Str("certPath", certPath).Msg("Started CA certificate reloader")
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
			s.GetLogger().Err(err).Msg("failed to query DNS and update nodes")
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

	srvToAddresses, srvToErrors := s.queryAllSRVRecords(ctx)

	// merge with lastSuccessfulDNS for failed SRV addresses
	s.lastQueryMutex.Lock()
	finalAddresses := make(map[string][]string)

	for srv, addrs := range srvToAddresses {
		finalAddresses[srv] = addrs
		s.lastSuccessfulDNS[srv] = addrs
	}
	for srv, err := range srvToErrors {
		if cachedAddrs, exists := s.lastSuccessfulDNS[srv]; exists {
			finalAddresses[srv] = cachedAddrs
			s.GetLogger().Warn().
				Str("srv", srv).
				Err(err).
				Strs("cached_addresses", cachedAddrs).
				Msg("Using cached addresses for failed SRV query")
		} else {
			s.GetLogger().Warn().
				Str("srv", srv).
				Err(err).
				Msg("SRV query failed and no cached addresses available")
		}
	}
	s.lastQueryMutex.Unlock()

	// check if we have any addresses at all
	if len(finalAddresses) == 0 {
		if s.metrics != nil {
			s.metrics.discoveryFailedCount.Inc(1)
		}
		// collect all errors for reporting
		allErrors := make([]error, 0, len(srvToErrors))
		for srv, err := range srvToErrors {
			allErrors = append(allErrors, fmt.Errorf("SRV %s: %w", srv, err))
		}
		return fmt.Errorf("all DNS queries failed and no cached addresses available: %w", errors.Join(allErrors...))
	}

	if s.GetLogger().Debug().Enabled() {
		totalAddrs := 0
		for _, addrs := range finalAddresses {
			totalAddrs += len(addrs)
		}
		s.GetLogger().Debug().
			Int("total_addresses", totalAddrs).
			Int("successful_srvs", len(srvToAddresses)).
			Int("failed_srvs", len(srvToErrors)).
			Int("total_srvs", len(s.srvAddresses)).
			Msg("DNS query completed")
	}

	// Update node cache based on DNS results
	updateErr := s.updateNodeCache(ctx, finalAddresses)
	if updateErr != nil && s.metrics != nil {
		s.metrics.discoveryFailedCount.Inc(1)
	}
	s.lastQueryMutex.Lock()
	s.lastQueryTime = time.Now()
	s.lastQueryMutex.Unlock()
	return updateErr
}

func (s *Service) queryAllSRVRecords(ctx context.Context) (map[string][]string, map[string]error) {
	startTime := time.Now()
	defer func() {
		if s.metrics != nil {
			duration := time.Since(startTime)
			s.metrics.dnsQueryCount.Inc(1)
			s.metrics.dnsQueryDuration.Observe(duration.Seconds())
			s.metrics.dnsQueryTotalDuration.Inc(duration.Seconds())
		}
	}()

	srvToAddresses := make(map[string][]string)
	srvToErrors := make(map[string]error)

	for _, srvAddr := range s.srvAddresses {
		if s.metrics != nil {
			s.metrics.dnsSRVLookupCount.Inc(1)
		}
		_, addrs, lookupErr := s.resolver.LookupSRV(ctx, srvAddr)
		if lookupErr != nil {
			if s.metrics != nil {
				s.metrics.dnsSRVLookupFailedCount.Inc(1)
			}
			srvToErrors[srvAddr] = lookupErr
			continue
		}

		// store resolved addresses for this SRV
		resolvedAddrs := make([]string, 0, len(addrs))
		for _, srv := range addrs {
			address := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
			resolvedAddrs = append(resolvedAddrs, address)
		}
		srvToAddresses[srvAddr] = resolvedAddrs
	}

	// Update metrics if all queries failed
	if len(srvToAddresses) == 0 && len(srvToErrors) > 0 {
		if s.metrics != nil {
			s.metrics.dnsQueryFailedCount.Inc(1)
		}
	}

	return srvToAddresses, srvToErrors
}

func (s *Service) updateNodeCache(ctx context.Context, srvToAddresses map[string][]string) error {
	var addErrors []error
	for _, addrs := range srvToAddresses {
		for _, addr := range addrs {
			_, exists := s.GetCachedNode(addr)

			if !exists {
				// fetch node metadata from gRPC
				node, fetchErr := s.fetchNodeMetadata(ctx, addr)
				if fetchErr != nil {
					s.GetLogger().Warn().
						Err(fetchErr).
						Str("address", addr).
						Msg("Failed to fetch node metadata")
					addErrors = append(addErrors, fetchErr)
					continue
				}

				if added := s.AddNode(addr, node); added {
					// notify handlers
					s.NotifyHandlers(schema.Metadata{
						TypeMeta: schema.TypeMeta{
							Kind: schema.KindNode,
							Name: node.GetMetadata().GetName(),
						},
						Spec: node,
					}, true)

					s.GetLogger().Debug().
						Str("address", addr).
						Str("name", node.GetMetadata().GetName()).
						Msg("New node discovered and added to cache")
				}
			}
		}
	}

	// collect addresses to keep
	allAddr := make(map[string]bool)
	for _, addrs := range srvToAddresses {
		for _, addr := range addrs {
			allAddr[addr] = true
		}
	}

	// find nodes to delete
	currentAddresses := s.GetAllNodeAddresses()
	addressesToDelete := make([]string, 0)
	for _, addr := range currentAddresses {
		if !allAddr[addr] {
			addressesToDelete = append(addressesToDelete, addr)
		}
	}

	// delete nodes and collect removed
	nodesToDelete := s.RemoveNodes(addressesToDelete)

	// log deletions
	for addr, node := range nodesToDelete {
		s.GetLogger().Debug().
			Str("address", addr).
			Str("name", node.GetMetadata().GetName()).
			Msg("Node removed from cache (no longer in DNS)")
	}

	// notify handlers for deletions
	for _, node := range nodesToDelete {
		s.NotifyHandlers(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
				Name: node.GetMetadata().GetName(),
			},
			Spec: node,
		}, false)
	}

	// update total nodes metric
	if s.metrics != nil {
		s.metrics.totalNodesCount.Set(float64(s.GetCacheSize()))
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

	// use common fetcher
	node, err := common.FetchNodeMetadata(ctx, address, s.grpcTimeout, s)
	if err != nil {
		grpcErr = err
		return nil, err
	}
	return node, nil
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
		s.GetLogger().Debug().Str("certPath", certPath).Msg("Stopped CA certificate reloader")
	}

	return nil
}

// ListNode list all existing nodes from cache.
func (s *Service) ListNode(ctx context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	// if the service hasn't begun/finished, then try to query and update DNS first
	s.lastQueryMutex.RLock()
	notQueried := s.lastQueryTime.IsZero()
	s.lastQueryMutex.RUnlock()
	if notQueried {
		if err := s.queryDNSAndUpdateNodes(ctx); err != nil {
			return nil, err
		}
	}
	// delegate to base for filtering
	return s.NodeCacheBase.ListNode(ctx, role)
}

// RegisterNode update the configurations of a node, it should not be invoked.
func (s *Service) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return errors.New("manual node registration not supported in DNS discovery mode")
}

// UpdateNode update the configurations of a node, it should not be invoked.
func (s *Service) UpdateNode(_ context.Context, _ *databasev1.Node) error {
	return errors.New("manual node update not supported in DNS discovery mode")
}
