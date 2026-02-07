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

// Package client implements the metadata client service.
package client

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/dns"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/file"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/etcd"
	metadataproperty "github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// DefaultNamespace is the default namespace of the metadata stored in etcd.
	DefaultNamespace = "banyandb"
	// FlagEtcdEndpointsName is the default flag name for etcd endpoints.
	FlagEtcdEndpointsName = "etcd-endpoints"
)

const (
	// NodeDiscoveryModeEtcd represents etcd-based node discovery mode.
	NodeDiscoveryModeEtcd = "etcd"
	// NodeDiscoveryModeDNS represents DNS-based node discovery mode.
	NodeDiscoveryModeDNS = "dns"
	// NodeDiscoveryModeFile represents file-based node discovery mode.
	NodeDiscoveryModeFile = "file"
)

const (
	// RegistryModeEtcd represents etcd-based metadata storage mode.
	RegistryModeEtcd = "etcd"
	// RegistryModeProperty represents native property-based metadata storage mode.
	RegistryModeProperty = "property"
)

const flagEtcdUsername = "etcd-username"

const flagEtcdPassword = "etcd-password"

const flagEtcdTLSCAFile = "etcd-tls-ca-file"

const flagEtcdTLSCertFile = "etcd-tls-cert-file"

const flagEtcdTLSKeyFile = "etcd-tls-key-file"

// for the property based registry connect to the metadata node.
const (
	propertyRegistryInitRetryCount = 30
	propertyRegistryInitRetrySleep = time.Second * 3
)

// NewClient returns a new metadata client.
func NewClient(toRegisterNode, forceRegisterNode bool) (metadata.Service, error) {
	return &clientService{
		closer:            run.NewCloser(1),
		forceRegisterNode: forceRegisterNode,
		toRegisterNode:    toRegisterNode,
	}, nil
}

type clientService struct {
	schemaRegistry             schema.Registry
	nodeDiscovery              schema.NodeDiscovery
	omr                        observability.MetricsRegistry
	schemaUpdateClient         schemav1.SchemaUpdateServiceClient
	schemaManagementClient     schemav1.SchemaManagementServiceClient
	infoCollectorRegistry      *schema.InfoCollectorRegistry
	closer                     *run.Closer
	nodeInfo                   *databasev1.Node
	dataBroadcaster            bus.Broadcaster
	liaisonBroadcaster         bus.Broadcaster
	etcdTLSCertFile            string
	dnsCACertPaths             []string
	etcdPassword               string
	etcdTLSCAFile              string
	etcdUsername               string
	etcdTLSKeyFile             string
	namespace                  string
	nodeDiscoveryMode          string
	filePath                   string
	metadataRegistryMode       string
	metadataNodeTLSCertFile    string
	dnsSRVAddresses            []string
	endpoints                  []string
	registryTimeout            time.Duration
	dnsFetchInitInterval       time.Duration
	dnsFetchInitDuration       time.Duration
	dnsFetchInterval           time.Duration
	grpcTimeout                time.Duration
	propertySchemaSyncInterval time.Duration
	etcdFullSyncInterval       time.Duration
	fileFetchInterval          time.Duration
	fileRetryInitialInterval   time.Duration
	fileRetryMaxInterval       time.Duration
	fileRetryMultiplier        float64
	dnsRetryInitialInterval    time.Duration
	dnsRetryMaxInterval        time.Duration
	dnsRetryMultiplier         float64
	nodeInfoMux                sync.Mutex
	forceRegisterNode          bool
	toRegisterNode             bool
	dnsTLSEnabled              bool
	metadataNodeTLSEnabled     bool
}

func (s *clientService) SchemaRegistry() schema.Registry {
	return s.schemaRegistry
}

func (s *clientService) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVar(&s.namespace, "namespace", DefaultNamespace, "The namespace of the metadata stored in etcd")
	fs.StringSliceVar(&s.endpoints, FlagEtcdEndpointsName, []string{"http://localhost:2379"}, "A comma-delimited list of etcd endpoints")
	fs.StringVar(&s.etcdUsername, flagEtcdUsername, "", "A username of etcd")
	fs.StringVar(&s.etcdPassword, flagEtcdPassword, "", "A password of etcd user")
	fs.StringVar(&s.etcdTLSCAFile, flagEtcdTLSCAFile, "", "Trusted certificate authority")
	fs.StringVar(&s.etcdTLSCertFile, flagEtcdTLSCertFile, "", "Etcd client certificate")
	fs.StringVar(&s.etcdTLSKeyFile, flagEtcdTLSKeyFile, "", "Private key for the etcd client certificate.")
	fs.DurationVar(&s.registryTimeout, "node-registry-timeout", 2*time.Minute, "The timeout for the node registry")
	fs.DurationVar(&s.etcdFullSyncInterval, "etcd-full-sync-interval", 30*time.Minute, "The interval for full sync etcd")

	// node discovery configuration
	fs.StringVar(&s.nodeDiscoveryMode, "node-discovery-mode", NodeDiscoveryModeEtcd,
		"Node discovery mode: 'etcd' for etcd-based discovery, 'dns' for DNS-based discovery, 'file' for file-based discovery")
	fs.StringSliceVar(&s.dnsSRVAddresses, "node-discovery-dns-srv-addresses", []string{},
		"DNS SRV addresses for node discovery (e.g., _grpc._tcp.banyandb.svc.cluster.local)")
	fs.DurationVar(&s.dnsFetchInitInterval, "node-discovery-dns-fetch-init-interval", 5*time.Second,
		"DNS query interval during initialization phase")
	fs.DurationVar(&s.dnsFetchInitDuration, "node-discovery-dns-fetch-init-duration", 5*time.Minute,
		"Duration of the initialization phase for DNS discovery")
	fs.DurationVar(&s.dnsFetchInterval, "node-discovery-dns-fetch-interval", 15*time.Second,
		"DNS query interval after initialization phase")
	fs.DurationVar(&s.grpcTimeout, "node-discovery-grpc-timeout", 5*time.Second,
		"Timeout for gRPC calls to fetch node metadata")
	fs.BoolVar(&s.dnsTLSEnabled, "node-discovery-dns-tls", false,
		"Enable TLS for DNS discovery gRPC connections")
	fs.StringSliceVar(&s.dnsCACertPaths, "node-discovery-dns-ca-certs", []string{},
		"Comma-separated list of CA certificate files to verify DNS discovered nodes (one per SRV address, in same order)")
	fs.StringVar(&s.filePath, "node-discovery-file-path", "",
		"File path for static node configuration (file mode only)")
	fs.DurationVar(&s.fileFetchInterval, "node-discovery-file-fetch-interval", 5*time.Minute,
		"Interval to poll the discovery file in file discovery mode (fallback mechanism)")
	fs.DurationVar(&s.fileRetryInitialInterval, "node-discovery-file-retry-initial-interval", 1*time.Second,
		"Initial retry interval for failed node metadata fetches in file discovery mode")
	fs.DurationVar(&s.fileRetryMaxInterval, "node-discovery-file-retry-max-interval", 2*time.Minute,
		"Maximum retry interval for failed node metadata fetches in file discovery mode")
	fs.Float64Var(&s.fileRetryMultiplier, "node-discovery-file-retry-multiplier", 2.0,
		"Backoff multiplier for retry intervals in file discovery mode")
	fs.DurationVar(&s.dnsRetryInitialInterval, "node-discovery-dns-retry-initial-interval", 1*time.Second,
		"Initial retry interval for failed node metadata fetches in DNS discovery mode")
	fs.DurationVar(&s.dnsRetryMaxInterval, "node-discovery-dns-retry-max-interval", 2*time.Minute,
		"Maximum retry interval for failed node metadata fetches in DNS discovery mode")
	fs.Float64Var(&s.dnsRetryMultiplier, "node-discovery-dns-retry-multiplier", 2.0,
		"Backoff multiplier for retry intervals in DNS discovery mode")

	// schema management configuration
	fs.StringVar(&s.metadataRegistryMode, "schema-registry-mode", "etcd",
		"Schema storage mode: 'etcd' for etcd-based registry, 'property' for native property-based registry")
	fs.DurationVar(&s.propertySchemaSyncInterval, "schema-property-client-sync-interval", 30*time.Second,
		"Interval to sync property-based schema in client side with other nodes")
	fs.BoolVar(&s.metadataNodeTLSEnabled, "metadata-node-tls-enabled", false,
		"Enable TLS for gRPC connections to metadata nodes (property registry and etcd node discovery mode only)")
	fs.StringVar(&s.metadataNodeTLSCertFile, "metadata-node-tls-cert-file", "",
		"Client certificate file for TLS connections to metadata nodes (property registry and etcd node discovery mode only)")
	return fs
}

func (s *clientService) Validate() error {
	if s.nodeDiscoveryMode != NodeDiscoveryModeEtcd && s.nodeDiscoveryMode != NodeDiscoveryModeDNS &&
		s.nodeDiscoveryMode != NodeDiscoveryModeFile {
		return fmt.Errorf("invalid node-discovery-mode: %s, must be '%s', '%s', or '%s'",
			s.nodeDiscoveryMode, NodeDiscoveryModeEtcd, NodeDiscoveryModeDNS, NodeDiscoveryModeFile)
	}

	// Validate etcd endpoints if node discovery or metadata registry mode is etcd
	if (s.nodeDiscoveryMode == NodeDiscoveryModeEtcd || s.metadataRegistryMode == NodeDiscoveryModeEtcd) &&
		len(s.endpoints) == 0 {
		return errors.New("etcd endpoints cannot be empty")
	}

	// Validate DNS mode specific requirements
	if s.nodeDiscoveryMode == NodeDiscoveryModeDNS {
		if len(s.dnsSRVAddresses) == 0 {
			return errors.New("DNS mode requires non-empty DNS SRV addresses")
		}
		if s.dnsTLSEnabled {
			if len(s.dnsCACertPaths) == 0 {
				return errors.New("DNS TLS is enabled, but no CA certificate files were provided")
			}
			if len(s.dnsCACertPaths) != len(s.dnsSRVAddresses) {
				return fmt.Errorf("number of DNS CA cert paths (%d) must match number of SRV addresses (%d)",
					len(s.dnsCACertPaths), len(s.dnsSRVAddresses))
			}
		}
	}

	// Validate file mode specific requirements
	if s.nodeDiscoveryMode == NodeDiscoveryModeFile {
		if s.filePath == "" {
			return errors.New("file mode requires non-empty file path")
		}
		if _, err := os.Stat(s.filePath); err != nil {
			return fmt.Errorf("file path validation failed: %w", err)
		}
	}

	if s.metadataRegistryMode == RegistryModeProperty && s.nodeDiscoveryMode == NodeDiscoveryModeEtcd && s.metadataNodeTLSEnabled {
		if s.metadataNodeTLSCertFile == "" {
			return errors.New("metadata node TLS is enabled, but no client certificate file was provided")
		}
	}

	return nil
}

func (s *clientService) PreRun(ctx context.Context) error {
	stopCh := make(chan struct{})
	sn := make(chan os.Signal, 1)
	l := logger.GetLogger(s.Name())
	signal.Notify(sn,
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		select {
		case si := <-sn:
			logger.GetLogger(s.Name()).Info().Msgf("signal received: %s", si)
			close(stopCh)
		case <-s.closer.CloseNotify():
			close(stopCh)
		}
	}()

	if s.nodeDiscoveryMode == NodeDiscoveryModeDNS {
		l.Info().Strs("srv-addresses", s.dnsSRVAddresses).Msg("Initializing DNS-based node discovery")

		var createErr error
		s.nodeDiscovery, createErr = dns.NewService(dns.Config{
			OMR:                  s.omr,
			SRVAddresses:         s.dnsSRVAddresses,
			InitInterval:         s.dnsFetchInitInterval,
			InitDuration:         s.dnsFetchInitDuration,
			PollInterval:         s.dnsFetchInterval,
			GRPCTimeout:          s.grpcTimeout,
			TLSEnabled:           s.dnsTLSEnabled,
			CACertPaths:          s.dnsCACertPaths,
			RetryInitialInterval: s.dnsRetryInitialInterval,
			RetryMaxInterval:     s.dnsRetryMaxInterval,
			RetryMultiplier:      s.dnsRetryMultiplier,
		})
		if createErr != nil {
			return fmt.Errorf("failed to create DNS discovery service: %w", createErr)
		}
	}

	if s.nodeDiscoveryMode == NodeDiscoveryModeFile {
		l.Info().Str("file-path", s.filePath).Msg("Initializing file-based node discovery")

		var createErr error
		s.nodeDiscovery, createErr = file.NewService(file.Config{
			OMR:                  s.omr,
			FilePath:             s.filePath,
			GRPCTimeout:          s.grpcTimeout,
			FetchInterval:        s.fileFetchInterval,
			RetryInitialInterval: s.fileRetryInitialInterval,
			RetryMaxInterval:     s.fileRetryMaxInterval,
			RetryMultiplier:      s.fileRetryMultiplier,
		})
		if createErr != nil {
			return fmt.Errorf("failed to create file discovery service: %w", createErr)
		}
	}

	var etcdRegistry schema.Registry
	if s.nodeDiscoveryMode == NodeDiscoveryModeEtcd {
		var etcdNodeDiscovery schema.NodeDiscovery
		var initErr error
		etcdRegistry, etcdNodeDiscovery, initErr = s.initEtcdSchemaRegistry(l, stopCh)
		if initErr != nil {
			return initErr
		}
		s.nodeDiscovery = etcdNodeDiscovery
	}

	if s.metadataRegistryMode == RegistryModeEtcd {
		if etcdRegistry != nil {
			s.schemaRegistry = etcdRegistry
		} else {
			var initErr error
			s.schemaRegistry, _, initErr = s.initEtcdSchemaRegistry(l, stopCh)
			if initErr != nil {
				return initErr
			}
		}
	} else if s.metadataRegistryMode == RegistryModeProperty {
		if err := s.initPropertySchemaRegistry(ctx, l, stopCh); err != nil {
			return fmt.Errorf("failed to initialize property schema registry: %w", err)
		}
	}

	s.infoCollectorRegistry = schema.NewInfoCollectorRegistry(l, s.schemaRegistry)
	if s.dataBroadcaster != nil {
		s.infoCollectorRegistry.SetDataBroadcaster(s.dataBroadcaster)
	}
	if s.liaisonBroadcaster != nil {
		s.infoCollectorRegistry.SetLiaisonBroadcaster(s.liaisonBroadcaster)
	}

	return s.registerNodeIfNeed(ctx, l, stopCh)
}

func (s *clientService) initEtcdSchemaRegistry(l *logger.Logger, stopCh chan struct{}) (schema.Registry, schema.NodeDiscovery, error) {
	for {
		registry, nodeDiscovery, err := etcd.NewEtcdSchemaRegistry(
			etcd.Namespace(s.namespace),
			etcd.ConfigureServerEndpoints(s.endpoints),
			etcd.ConfigureEtcdUser(s.etcdUsername, s.etcdPassword),
			etcd.ConfigureEtcdTLSCAFile(s.etcdTLSCAFile),
			etcd.ConfigureEtcdTLSCertAndKey(s.etcdTLSCertFile, s.etcdTLSKeyFile),
			etcd.ConfigureWatchCheckInterval(s.etcdFullSyncInterval),
			etcd.ConfigureNodeTLSEnabled(s.metadataNodeTLSEnabled),
			etcd.ConfigureNodeTLSCertFile(s.metadataNodeTLSCertFile),
		)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			select {
			case <-stopCh:
				return nil, nil, errors.New("pre-run interrupted")
			case <-time.After(s.registryTimeout):
				return nil, nil, errors.New("pre-run timeout")
			case <-s.closer.CloseNotify():
				return nil, nil, errors.New("pre-run interrupted")
			default:
				l.Warn().Strs("etcd-endpoints", s.endpoints).Msg("the etcd init timeout, retrying...")
				time.Sleep(time.Second)
				continue
			}
		}
		if err != nil {
			return nil, nil, err
		}
		return registry, nodeDiscovery, nil
	}
}

func (s *clientService) registerNodeIfNeed(ctx context.Context, l *logger.Logger, stopCh chan struct{}) error {
	// skip node registration if DNS/file mode is enabled or node registration is disabled
	if !s.toRegisterNode || s.nodeDiscoveryMode == NodeDiscoveryModeDNS ||
		s.nodeDiscoveryMode == NodeDiscoveryModeFile {
		return nil
	}
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	val = ctx.Value(common.ContextNodeRolesKey)
	if val == nil {
		return errors.New("node roles is empty")
	}
	nodeRoles := val.([]databasev1.Role)
	nodeInfo := &databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: node.NodeID,
		},
		GrpcAddress: node.GrpcAddress,
		HttpAddress: node.HTTPAddress,
		Roles:       nodeRoles,
		Labels:      node.Labels,
		CreatedAt:   timestamppb.Now(),

		PropertyRepairGossipGrpcAddress: node.PropertyGossipGrpcAddress,
	}
	for {
		ctxCancelable, cancel := context.WithTimeout(ctx, time.Second*10)
		err := s.nodeDiscovery.RegisterNode(ctxCancelable, nodeInfo, s.forceRegisterNode)
		cancel()
		if errors.Is(err, schema.ErrGRPCAlreadyExists) ||
			errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Log the specific error
			l.Warn().Err(err).Strs("etcd-endpoints", s.endpoints).Msg("register node error")

			select {
			case <-stopCh:
				return errors.New("register node interrupted")
			case <-time.After(s.registryTimeout):
				return errors.New("register node timeout")
			case <-s.closer.CloseNotify():
				return errors.New("register node interrupted")
			default:
				time.Sleep(time.Second)
				continue
			}
		}
		if err == nil {
			l.Info().Stringer("info", nodeInfo).Msg("register node successfully")
			s.nodeInfoMux.Lock()
			s.nodeInfo = nodeInfo
			s.nodeInfoMux.Unlock()
		}
		return err
	}
}

func (s *clientService) Serve() run.StopNotify {
	if s.schemaRegistry != nil {
		s.schemaRegistry.StartWatcher()
	}

	if s.nodeDiscovery != nil {
		if startErr := s.nodeDiscovery.Start(s.closer.Ctx()); startErr != nil {
			logger.GetLogger(s.Name()).Error().Err(startErr).Msg("failed to start node discovery")
		}
	}

	return s.closer.CloseNotify()
}

func (s *clientService) GracefulStop() {
	s.closer.Done()
	s.closer.CloseThenWait()

	if s.nodeDiscovery != nil {
		if closeErr := s.nodeDiscovery.Close(); closeErr != nil {
			logger.GetLogger(s.Name()).Error().Err(closeErr).Msg("failed to close node discovery")
		}
	}

	if s.schemaRegistry != nil {
		if closeErr := s.schemaRegistry.Close(); closeErr != nil {
			logger.GetLogger(s.Name()).Error().Err(closeErr).Msg("failed to close schema registry")
		}
	}
}

func (s *clientService) RegisterHandler(name string, kind schema.Kind, handler schema.EventHandler) {
	if kind == schema.KindNode {
		s.nodeDiscovery.RegisterHandler(name, kind, handler)
		return
	}
	s.schemaRegistry.RegisterHandler(name, kind, handler)
}

func (s *clientService) StreamRegistry() schema.Stream {
	return s.schemaRegistry
}

func (s *clientService) IndexRuleRegistry() schema.IndexRule {
	return s.schemaRegistry
}

func (s *clientService) IndexRuleBindingRegistry() schema.IndexRuleBinding {
	return s.schemaRegistry
}

func (s *clientService) MeasureRegistry() schema.Measure {
	return s.schemaRegistry
}

func (s *clientService) TraceRegistry() schema.Trace {
	return s.schemaRegistry
}

func (s *clientService) GroupRegistry() schema.Group {
	return s.schemaRegistry
}

func (s *clientService) TopNAggregationRegistry() schema.TopNAggregation {
	return s.schemaRegistry
}

func (s *clientService) NodeRegistry() schema.Node {
	return s.nodeDiscovery
}

func (s *clientService) PropertyRegistry() schema.Property {
	return s.schemaRegistry
}

func (s *clientService) SetMetricsRegistry(omr observability.MetricsRegistry) {
	s.omr = omr
}

func (s *clientService) SetLocalPropertySchemaClient(mgrClient schemav1.SchemaManagementServiceClient, updateClient schemav1.SchemaUpdateServiceClient) {
	s.schemaManagementClient = mgrClient
	s.schemaUpdateClient = updateClient
}

func (s *clientService) Name() string {
	return "metadata"
}

func (s *clientService) IndexRules(ctx context.Context, subject *commonv1.Metadata) ([]*databasev1.IndexRule, error) {
	bindings, err := s.schemaRegistry.ListIndexRuleBinding(ctx, schema.ListOpt{Group: subject.Group})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	foundRules := make([]string, 0)
	for _, binding := range bindings {
		if binding.GetBeginAt().AsTime().After(now) ||
			binding.GetExpireAt().AsTime().Before(now) {
			continue
		}
		sub := binding.GetSubject()
		if sub.Name != subject.Name {
			continue
		}
		foundRules = append(foundRules, binding.Rules...)
	}
	result := make([]*databasev1.IndexRule, 0, len(foundRules))
	var indexRuleErr error
	for _, rule := range foundRules {
		r, getErr := s.schemaRegistry.GetIndexRule(ctx, &commonv1.Metadata{
			Name:  rule,
			Group: subject.Group,
		})
		if getErr != nil {
			indexRuleErr = multierr.Append(indexRuleErr, err)
			continue
		}
		result = append(result, r)
	}
	return result, indexRuleErr
}

func (s *clientService) Subjects(ctx context.Context, indexRule *databasev1.IndexRule, catalog commonv1.Catalog) ([]schema.Spec, error) {
	bindings, err := s.schemaRegistry.ListIndexRuleBinding(ctx, schema.ListOpt{Group: indexRule.GetMetadata().GetGroup()})
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var subjectErr error
	foundSubjects := make([]schema.Spec, 0)
	for _, binding := range bindings {
		if binding.GetBeginAt().AsTime().After(now) ||
			binding.GetExpireAt().AsTime().Before(now) {
			continue
		}
		sub := binding.GetSubject()
		if sub.GetCatalog() != catalog {
			continue
		}

		if !contains(binding.GetRules(), indexRule.GetMetadata().GetName()) {
			continue
		}

		switch catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			stream, getErr := s.schemaRegistry.GetStream(ctx, &commonv1.Metadata{
				Name:  sub.GetName(),
				Group: indexRule.GetMetadata().GetGroup(),
			})
			if getErr != nil {
				subjectErr = multierr.Append(subjectErr, getErr)
			}
			foundSubjects = append(foundSubjects, stream)
		case commonv1.Catalog_CATALOG_MEASURE:
			measure, getErr := s.schemaRegistry.GetMeasure(ctx, &commonv1.Metadata{
				Name:  sub.GetName(),
				Group: indexRule.GetMetadata().GetGroup(),
			})
			if getErr != nil {
				subjectErr = multierr.Append(subjectErr, getErr)
			}
			foundSubjects = append(foundSubjects, measure)
		default:
			continue
		}
	}

	return foundSubjects, subjectErr
}

func (s *clientService) CollectDataInfo(ctx context.Context, group string) ([]*databasev1.DataInfo, error) {
	return s.infoCollectorRegistry.CollectDataInfo(ctx, group)
}

func (s *clientService) CollectLiaisonInfo(ctx context.Context, group string) ([]*databasev1.LiaisonInfo, error) {
	return s.infoCollectorRegistry.CollectLiaisonInfo(ctx, group)
}

func (s *clientService) RegisterDataCollector(catalog commonv1.Catalog, collector schema.DataInfoCollector) {
	s.infoCollectorRegistry.RegisterDataCollector(catalog, collector)
}

func (s *clientService) RegisterLiaisonCollector(catalog commonv1.Catalog, collector schema.LiaisonInfoCollector) {
	s.infoCollectorRegistry.RegisterLiaisonCollector(catalog, collector)
}

func (s *clientService) SetDataBroadcaster(broadcaster bus.Broadcaster) {
	s.dataBroadcaster = broadcaster
}

func (s *clientService) SetLiaisonBroadcaster(broadcaster bus.Broadcaster) {
	s.liaisonBroadcaster = broadcaster
}

func (s *clientService) initPropertySchemaRegistry(ctx context.Context, l *logger.Logger, stopCh chan struct{}) error {
	var currentNode *metadataproperty.NodeInfo
	n := buildNodeFromContext(ctx)
	if n != nil {
		currentNode = &metadataproperty.NodeInfo{
			Node:               n,
			SchemaMgrClient:    s.schemaManagementClient,
			SchemaUpdateClient: s.schemaUpdateClient,
		}
	}

	registryCfg := &metadataproperty.ClientConfig{
		GRPCTimeout:  s.grpcTimeout,
		SyncInterval: s.propertySchemaSyncInterval,
		OMR:          s.omr,
		Node:         currentNode,
		NodeSchema:   s.NodeRegistry(),
		DialProvider: s.nodeDiscovery,
	}
	var initAttempt int
	for {
		registry, initErr := metadataproperty.NewSchemaRegistryClient(registryCfg) //nolint:contextcheck
		if errors.Is(initErr, context.DeadlineExceeded) || errors.Is(initErr, context.Canceled) {
			select {
			case <-stopCh:
				return errors.New("pre-run interrupted")
			case <-time.After(s.registryTimeout):
				return errors.New("pre-run timeout")
			case <-s.closer.CloseNotify():
				return errors.New("pre-run interrupted")
			default:
				l.Warn().Msg("the property schema registry init timeout, retrying...")
				time.Sleep(time.Second)
				continue
			}
		}
		if initErr != nil {
			initAttempt++
			if initAttempt >= propertyRegistryInitRetryCount {
				return fmt.Errorf("failed to initialize property schema registry after %d attempts: %w",
					initAttempt, initErr)
			}
			l.Warn().Err(initErr).Msgf("failed to initialize property schema registry, attempt %d/%d, retrying in %s...",
				initAttempt, propertyRegistryInitRetryCount, propertyRegistryInitRetrySleep)
			time.Sleep(propertyRegistryInitRetrySleep)
			continue
		}
		s.nodeDiscovery.RegisterHandler("property-schema-registry", schema.KindNode, registry)
		s.schemaRegistry = registry
		break
	}
	return nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func buildNodeFromContext(ctx context.Context) *databasev1.Node {
	nodeVal := ctx.Value(common.ContextNodeKey)
	rolesVal := ctx.Value(common.ContextNodeRolesKey)
	if nodeVal == nil || rolesVal == nil {
		return nil
	}
	node := nodeVal.(common.Node)
	nodeRoles := rolesVal.([]databasev1.Role)
	return &databasev1.Node{
		Metadata:                        &commonv1.Metadata{Name: node.NodeID},
		GrpcAddress:                     node.GrpcAddress,
		HttpAddress:                     node.HTTPAddress,
		Roles:                           nodeRoles,
		Labels:                          node.Labels,
		CreatedAt:                       timestamppb.Now(),
		PropertyRepairGossipGrpcAddress: node.PropertyGossipGrpcAddress,
	}
}
