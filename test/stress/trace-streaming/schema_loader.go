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

package tracestreaming

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// SchemaLoader defines the interface for schema loading operations.
type SchemaLoader interface {
	Name() string
	PreRun(ctx context.Context) error
	SetRegistry(registry schema.Registry)
}

type schemaLoader struct {
	registry schema.Registry
	name     string
}

func (s *schemaLoader) Name() string {
	return "schema-loader-" + s.name
}

func (s *schemaLoader) PreRun(ctx context.Context) error {
	e := s.registry

	// Load groups
	if err := s.loadGroups(ctx, e); err != nil {
		return errors.WithStack(err)
	}

	// Load trace schemas
	if err := s.loadTraceSchemas(ctx, e); err != nil {
		return errors.WithStack(err)
	}

	// Load trace index rules
	if err := s.loadTraceIndexRules(ctx, e); err != nil {
		return errors.WithStack(err)
	}

	// Load trace index rule bindings
	if err := s.loadTraceIndexRuleBindings(ctx, e); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *schemaLoader) SetRegistry(registry schema.Registry) {
	s.registry = registry
}

func (s *schemaLoader) loadGroups(ctx context.Context, e schema.Registry) error {
	groupFile := filepath.Join("testdata", "schema", "group.json")
	data, err := os.ReadFile(groupFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read group file: %s", groupFile)
	}

	var groupData []json.RawMessage
	if err := json.Unmarshal(data, &groupData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal groups array")
	}

	for i, groupBytes := range groupData {
		var group commonv1.Group
		if err := protojson.Unmarshal(groupBytes, &group); err != nil {
			return errors.Wrapf(err, "failed to unmarshal group %d", i)
		}

		if err := e.CreateGroup(ctx, &group); err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Infof("Group %s already exists, skipping", group.Metadata.Name)
				continue
			}
			return errors.Wrapf(err, "failed to create group: %s", group.Metadata.Name)
		}
		logger.Infof("Created group: %s", group.Metadata.Name)
	}

	return nil
}

func (s *schemaLoader) loadTraceSchemas(ctx context.Context, e schema.Registry) error {
	traceFile := filepath.Join("testdata", "schema", "trace_schema.json")
	data, err := os.ReadFile(traceFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read trace schema file: %s", traceFile)
	}

	var trace databasev1.Trace
	if err = protojson.Unmarshal(data, &trace); err != nil {
		return errors.Wrapf(err, "failed to unmarshal trace schema")
	}

	_, err = e.CreateTrace(ctx, &trace)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Infof("Trace %s already exists, skipping", trace.Metadata.Name)
			return nil
		}
		return errors.Wrapf(err, "failed to create trace: %s", trace.Metadata.Name)
	}

	logger.Infof("Created trace: %s", trace.Metadata.Name)
	return nil
}

func (s *schemaLoader) loadTraceIndexRules(ctx context.Context, e schema.Registry) error {
	indexFile := filepath.Join("testdata", "schema", "trace_index_rules.json")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read trace index rules file: %s", indexFile)
	}

	var indexRuleData []json.RawMessage
	if err := json.Unmarshal(data, &indexRuleData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal trace index rules array")
	}

	for i, ruleBytes := range indexRuleData {
		var rule databasev1.IndexRule
		if err := protojson.Unmarshal(ruleBytes, &rule); err != nil {
			return errors.Wrapf(err, "failed to unmarshal trace index rule %d", i)
		}

		if err := e.CreateIndexRule(ctx, &rule); err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Infof("Trace index rule %s already exists, skipping", rule.Metadata.Name)
				continue
			}
			return errors.Wrapf(err, "failed to create trace index rule: %s", rule.Metadata.Name)
		}
		logger.Infof("Created trace index rule: %s", rule.Metadata.Name)
	}

	return nil
}

func (s *schemaLoader) loadTraceIndexRuleBindings(ctx context.Context, e schema.Registry) error {
	bindingFile := filepath.Join("testdata", "schema", "trace_index_rule_bindings.json")
	data, err := os.ReadFile(bindingFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read trace index rule bindings file: %s", bindingFile)
	}

	var bindingData []json.RawMessage
	if err := json.Unmarshal(data, &bindingData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal trace index rule bindings array")
	}

	for i, bindingBytes := range bindingData {
		var binding databasev1.IndexRuleBinding
		if err := protojson.Unmarshal(bindingBytes, &binding); err != nil {
			return errors.Wrapf(err, "failed to unmarshal trace index rule binding %d", i)
		}

		if err := e.CreateIndexRuleBinding(ctx, &binding); err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Infof("Trace index rule binding %s already exists, skipping", binding.Metadata.Name)
				continue
			}
			return errors.Wrapf(err, "failed to create trace index rule binding: %s", binding.Metadata.Name)
		}
		logger.Infof("Created trace index rule binding: %s", binding.Metadata.Name)
	}

	return nil
}

// NewSchemaLoader creates a new schema loader for the trace streaming performance test.
func NewSchemaLoader(name string) SchemaLoader {
	return &schemaLoader{
		name: name,
	}
}
