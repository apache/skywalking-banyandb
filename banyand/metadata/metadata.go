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

// Package metadata implements a Raft-based distributed metadata storage system.
// Powered by etcd.
package metadata

import (
	"context"
	"errors"
	"time"

	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// IndexFilter provides methods to find a specific index related objects and vice versa.
type IndexFilter interface {
	// IndexRules fetches v1.IndexRule by subject defined in IndexRuleBinding
	IndexRules(ctx context.Context, subject *commonv1.Metadata) ([]*databasev1.IndexRule, error)
	// Subjects fetches Subject(s) by index rule
	Subjects(ctx context.Context, indexRule *databasev1.IndexRule, catalog commonv1.Catalog) ([]schema.Spec, error)
}

// Repo is the facade to interact with the metadata repository.
type Repo interface {
	IndexFilter
	StreamRegistry() schema.Stream
	IndexRuleRegistry() schema.IndexRule
	IndexRuleBindingRegistry() schema.IndexRuleBinding
	MeasureRegistry() schema.Measure
	GroupRegistry() schema.Group
	TopNAggregationRegistry() schema.TopNAggregation
	PropertyRegistry() schema.Property
}

// Service is the metadata repository.
type Service interface {
	Repo
	run.PreRunner
	run.Service
	run.Config
	SchemaRegistry() schema.Registry
}

type service struct {
	schemaRegistry  schema.Registry
	rootDir         string
	listenClientURL string
	listenPeerURL   string
}

func (s *service) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVarP(&s.rootDir, "metadata-root-path", "", "/tmp", "the root path of metadata")
	fs.StringVarP(&s.listenClientURL, "etcd-listen-client-url", "", "http://localhost:2379", "A URL to listen on for client traffic")
	fs.StringVarP(&s.listenPeerURL, "etcd-listen-peer-url", "", "http://localhost:2380", "A URL to listen on for peer traffic")
	return fs
}

func (s *service) Validate() error {
	if s.rootDir == "" {
		return errors.New("rootDir is empty")
	}
	return nil
}

func (s *service) PreRun() error {
	var err error
	s.schemaRegistry, err = schema.NewEtcdSchemaRegistry(
		schema.ConfigureListener(s.listenClientURL, s.listenPeerURL),
		schema.RootDir(s.rootDir))
	if err != nil {
		return err
	}
	<-s.schemaRegistry.ReadyNotify()
	return nil
}

func (s *service) Serve() run.StopNotify {
	return s.schemaRegistry.StoppingNotify()
}

func (s *service) GracefulStop() {
	_ = s.schemaRegistry.Close()
	<-s.schemaRegistry.StopNotify()
}

// NewService returns a new metadata repository Service.
func NewService(_ context.Context) (Service, error) {
	return &service{}, nil
}

func (s *service) SchemaRegistry() schema.Registry {
	return s.schemaRegistry
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

func (s *service) TopNAggregationRegistry() schema.TopNAggregation {
	return s.schemaRegistry
}

func (s *service) PropertyRegistry() schema.Property {
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

func (s *service) Subjects(ctx context.Context, indexRule *databasev1.IndexRule, catalog commonv1.Catalog) ([]schema.Spec, error) {
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
