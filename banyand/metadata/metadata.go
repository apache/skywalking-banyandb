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
	"errors"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

//IndexFilter provides methods to find a specific index related objects
type IndexFilter interface {
	//IndexRules fetches v1.IndexRule by subject defined in IndexRuleBinding
	IndexRules(ctx context.Context, subject *commonv1.Metadata) ([]*databasev1.IndexRule, error)
}

type Repo interface {
	IndexFilter
	StreamRegistry() schema.Stream
	IndexRuleRegistry() schema.IndexRule
	IndexRuleBindingRegistry() schema.IndexRuleBinding
	MeasureRegistry() schema.Measure
	GroupRegistry() schema.Group
}

type Service interface {
	Repo
	run.PreRunner
	run.Service
	run.Config
}

type service struct {
	schemaRegistry    schema.Registry
	stopCh            chan struct{}
	clientListenerURL string
	peerListenerURL   string
	rootDir           string
}

func (s *service) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVarP(&s.clientListenerURL, "listener-client-url", "", embed.DefaultListenClientURLs,
		"listener for client")
	fs.StringVarP(&s.peerListenerURL, "listener-peer-url", "", embed.DefaultListenPeerURLs,
		"listener for peer")
	fs.StringVarP(&s.rootDir, "etcd-root-path", "", "/tmp", "the root path of database")
	return fs
}

func (s *service) Validate() error {
	if s.clientListenerURL == "" || s.peerListenerURL == "" {
		return errors.New("listener cannot be set to empty")
	}
	if s.rootDir == "" {
		return errors.New("rootDir is empty")
	}
	return nil
}

func (s *service) PreRun() error {
	var err error
	s.schemaRegistry, err = schema.NewEtcdSchemaRegistry(schema.PreloadSchema(),
		schema.UseListener(s.clientListenerURL, s.peerListenerURL),
		schema.RootDir(s.rootDir))
	return err
}

func (s *service) Serve() error {
	s.stopCh = make(chan struct{})
	<-s.stopCh
	return nil
}

func (s *service) GracefulStop() {
	s.schemaRegistry.Close()
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

func NewService(_ context.Context) (Service, error) {
	return &service{}, nil
}

func (s *service) StreamRegistry() schema.Stream {
	return s.schemaRegistry
}

func (s *service) IndexRuleRegistry() schema.IndexRule {
	return s.schemaRegistry
}

func (s *service) IndexRuleBindingRegistry() schema.IndexRuleBinding {
	return s.schemaRegistry
}

func (s *service) MeasureRegistry() schema.Measure {
	return s.schemaRegistry
}

func (s *service) GroupRegistry() schema.Group {
	return s.schemaRegistry
}

func (s *service) Name() string {
	return "metadata"
}

func (s *service) IndexRules(ctx context.Context, subject *commonv1.Metadata) ([]*databasev1.IndexRule, error) {
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
