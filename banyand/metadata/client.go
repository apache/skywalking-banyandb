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

package metadata

import (
	"context"
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
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// DefaultNamespace is the default namespace of the metadata stored in etcd.
	DefaultNamespace = "banyandb"
	// FlagEtcdEndpointsName is the default flag name for etcd endpoints.
	FlagEtcdEndpointsName = "etcd-endpoints"
)

const flagEtcdUsername = "etcd-username"

const flagEtcdPassword = "etcd-password"

const flagEtcdTLSCAFile = "etcd-tls-ca-file"

const flagEtcdTLSCertFile = "etcd-tls-cert-file"

const flagEtcdTLSKeyFile = "etcd-tls-key-file"

// NewClient returns a new metadata client.
func NewClient(toRegisterNode, forceRegisterNode bool) (Service, error) {
	return &clientService{
		closer:            run.NewCloser(1),
		forceRegisterNode: forceRegisterNode,
		toRegisterNode:    toRegisterNode,
	}, nil
}

type clientService struct {
	schemaRegistry       schema.Registry
	closer               *run.Closer
	nodeInfo             *databasev1.Node
	etcdTLSCertFile      string
	etcdPassword         string
	etcdTLSCAFile        string
	etcdUsername         string
	etcdTLSKeyFile       string
	namespace            string
	endpoints            []string
	registryTimeout      time.Duration
	etcdFullSyncInterval time.Duration
	nodeInfoMux          sync.Mutex
	forceRegisterNode    bool
	toRegisterNode       bool
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
	return fs
}

func (s *clientService) Validate() error {
	if s.endpoints == nil {
		return errors.New("endpoints is empty")
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

	for {
		var err error
		s.schemaRegistry, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace(s.namespace),
			schema.ConfigureServerEndpoints(s.endpoints),
			schema.ConfigureEtcdUser(s.etcdUsername, s.etcdPassword),
			schema.ConfigureEtcdTLSCAFile(s.etcdTLSCAFile),
			schema.ConfigureEtcdTLSCertAndKey(s.etcdTLSCertFile, s.etcdTLSKeyFile),
			schema.ConfigureWatchCheckInterval(s.etcdFullSyncInterval),
		)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			select {
			case <-stopCh:
				return errors.New("pre-run interrupted")
			case <-time.After(s.registryTimeout):
				return errors.New("pre-run timeout")
			case <-s.closer.CloseNotify():
				return errors.New("pre-run interrupted")
			default:
				l.Warn().Strs("etcd-endpoints", s.endpoints).Msg("the schema registry init timeout, retrying...")
				time.Sleep(time.Second)
				continue
			}
		}
		if err == nil {
			break
		}
		return err
	}
	if !s.toRegisterNode {
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
	}
	for {
		ctxCancelable, cancel := context.WithTimeout(ctx, time.Second*10)
		err := s.schemaRegistry.RegisterNode(ctxCancelable, nodeInfo, s.forceRegisterNode)
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
	return s.closer.CloseNotify()
}

func (s *clientService) GracefulStop() {
	s.closer.Done()
	s.closer.CloseThenWait()
	if s.schemaRegistry != nil {
		if err := s.schemaRegistry.Close(); err != nil {
			logger.GetLogger(s.Name()).Error().Err(err).Msg("failed to close schema registry")
		}
	}
}

func (s *clientService) RegisterHandler(name string, kind schema.Kind, handler schema.EventHandler) {
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
	return s.schemaRegistry
}

func (s *clientService) PropertyRegistry() schema.Property {
	return s.schemaRegistry
}

func (s *clientService) Name() string {
	return "metadata"
}

func (s *clientService) Role() databasev1.Role {
	return databasev1.Role_ROLE_META
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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
