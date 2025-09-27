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

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// SchemaClient handles schema operations via gRPC
type SchemaClient struct {
	groupClient       databasev1.GroupRegistryServiceClient
	streamClient      databasev1.StreamRegistryServiceClient
	traceClient       databasev1.TraceRegistryServiceClient
	indexRuleClient   databasev1.IndexRuleRegistryServiceClient
	indexBindingClient databasev1.IndexRuleBindingRegistryServiceClient
}

// NewSchemaClient creates a new schema client
func NewSchemaClient(conn *grpc.ClientConn) *SchemaClient {
	return &SchemaClient{
		groupClient:       databasev1.NewGroupRegistryServiceClient(conn),
		streamClient:      databasev1.NewStreamRegistryServiceClient(conn),
		traceClient:       databasev1.NewTraceRegistryServiceClient(conn),
		indexRuleClient:   databasev1.NewIndexRuleRegistryServiceClient(conn),
		indexBindingClient: databasev1.NewIndexRuleBindingRegistryServiceClient(conn),
	}
}

// LoadStreamSchemas loads all stream-related schemas
func (s *SchemaClient) LoadStreamSchemas(ctx context.Context) error {
	// Load stream group
	if err := s.loadStreamGroup(ctx); err != nil {
		return fmt.Errorf("failed to load stream group: %w", err)
	}

	// Load stream schema
	if err := s.loadStreamSchema(ctx); err != nil {
		return fmt.Errorf("failed to load stream schema: %w", err)
	}

	// Load stream index rules
	if err := s.loadStreamIndexRules(ctx); err != nil {
		return fmt.Errorf("failed to load stream index rules: %w", err)
	}

	// Load stream index rule bindings
	if err := s.loadStreamIndexRuleBindings(ctx); err != nil {
		return fmt.Errorf("failed to load stream index rule bindings: %w", err)
	}

	return nil
}

// LoadTraceSchemas loads all trace-related schemas
func (s *SchemaClient) LoadTraceSchemas(ctx context.Context) error {
	// Load trace group
	if err := s.loadTraceGroup(ctx); err != nil {
		return fmt.Errorf("failed to load trace group: %w", err)
	}

	// Load trace schema
	if err := s.loadTraceSchema(ctx); err != nil {
		return fmt.Errorf("failed to load trace schema: %w", err)
	}

	// Load trace index rules
	if err := s.loadTraceIndexRules(ctx); err != nil {
		return fmt.Errorf("failed to load trace index rules: %w", err)
	}

	// Load trace index rule bindings
	if err := s.loadTraceIndexRuleBindings(ctx); err != nil {
		return fmt.Errorf("failed to load trace index rule bindings: %w", err)
	}

	return nil
}

func (s *SchemaClient) loadStreamGroup(ctx context.Context) error {
	groupFile := filepath.Join("../testdata", "schema", "group.json")
	data, err := os.ReadFile(groupFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read group file: %s", groupFile)
	}

	var groupData []json.RawMessage
	if err := json.Unmarshal(data, &groupData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal groups array")
	}

	// Load only stream group
	for _, groupBytes := range groupData {
		var group commonv1.Group
		if err := protojson.Unmarshal(groupBytes, &group); err != nil {
			return errors.Wrapf(err, "failed to unmarshal group")
		}

		if group.Metadata.Name == "stream_performance_test" {
			_, err := s.groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: &group,
			})
			if err != nil {
				if status.Code(err) == codes.AlreadyExists {
					logger.Infof("Group %s already exists, skipping", group.Metadata.Name)
					return nil
				}
				return errors.Wrapf(err, "failed to create group: %s", group.Metadata.Name)
			}
			logger.Infof("Created group: %s", group.Metadata.Name)
			return nil
		}
	}

	return fmt.Errorf("stream group not found in group.json")
}

func (s *SchemaClient) loadTraceGroup(ctx context.Context) error {
	groupFile := filepath.Join("../testdata", "schema", "group.json")
	data, err := os.ReadFile(groupFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read group file: %s", groupFile)
	}

	var groupData []json.RawMessage
	if err := json.Unmarshal(data, &groupData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal groups array")
	}

	// Load only trace group
	for _, groupBytes := range groupData {
		var group commonv1.Group
		if err := protojson.Unmarshal(groupBytes, &group); err != nil {
			return errors.Wrapf(err, "failed to unmarshal group")
		}

		if group.Metadata.Name == "trace_performance_test" {
			_, err := s.groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: &group,
			})
			if err != nil {
				if status.Code(err) == codes.AlreadyExists {
					logger.Infof("Group %s already exists, skipping", group.Metadata.Name)
					return nil
				}
				return errors.Wrapf(err, "failed to create group: %s", group.Metadata.Name)
			}
			logger.Infof("Created group: %s", group.Metadata.Name)
			return nil
		}
	}

	return fmt.Errorf("trace group not found in group.json")
}

func (s *SchemaClient) loadStreamSchema(ctx context.Context) error {
	streamFile := filepath.Join("../testdata", "schema", "stream_schema.json")
	data, err := os.ReadFile(streamFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read stream schema file: %s", streamFile)
	}

	var stream databasev1.Stream
	if err = protojson.Unmarshal(data, &stream); err != nil {
		return errors.Wrapf(err, "failed to unmarshal stream schema")
	}

	_, err = s.streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
		Stream: &stream,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Infof("Stream %s already exists, skipping", stream.Metadata.Name)
			return nil
		}
		return errors.Wrapf(err, "failed to create stream: %s", stream.Metadata.Name)
	}

	logger.Infof("Created stream: %s", stream.Metadata.Name)
	return nil
}

func (s *SchemaClient) loadTraceSchema(ctx context.Context) error {
	traceFile := filepath.Join("../testdata", "schema", "trace_schema.json")
	data, err := os.ReadFile(traceFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read trace schema file: %s", traceFile)
	}

	var trace databasev1.Trace
	if err = protojson.Unmarshal(data, &trace); err != nil {
		return errors.Wrapf(err, "failed to unmarshal trace schema")
	}

	_, err = s.traceClient.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{
		Trace: &trace,
	})
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

func (s *SchemaClient) loadStreamIndexRules(ctx context.Context) error {
	indexFile := filepath.Join("../testdata", "schema", "stream_index_rules.json")
	data, err := os.ReadFile(indexFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read stream index rules file: %s", indexFile)
	}

	var indexRuleData []json.RawMessage
	if err := json.Unmarshal(data, &indexRuleData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal stream index rules array")
	}

	for i, ruleBytes := range indexRuleData {
		var rule databasev1.IndexRule
		if err := protojson.Unmarshal(ruleBytes, &rule); err != nil {
			return errors.Wrapf(err, "failed to unmarshal stream index rule %d", i)
		}

		_, err := s.indexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
			IndexRule: &rule,
		})
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Infof("Stream index rule %s already exists, skipping", rule.Metadata.Name)
				continue
			}
			return errors.Wrapf(err, "failed to create stream index rule: %s", rule.Metadata.Name)
		}
		logger.Infof("Created stream index rule: %s", rule.Metadata.Name)
	}

	return nil
}

func (s *SchemaClient) loadTraceIndexRules(ctx context.Context) error {
	indexFile := filepath.Join("../testdata", "schema", "trace_index_rules.json")
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

		_, err := s.indexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
			IndexRule: &rule,
		})
		if err != nil {
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

func (s *SchemaClient) loadStreamIndexRuleBindings(ctx context.Context) error {
	bindingFile := filepath.Join("../testdata", "schema", "stream_index_rule_bindings.json")
	data, err := os.ReadFile(bindingFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read stream index rule bindings file: %s", bindingFile)
	}

	var bindingData []json.RawMessage
	if err := json.Unmarshal(data, &bindingData); err != nil {
		return errors.Wrapf(err, "failed to unmarshal stream index rule bindings array")
	}

	for i, bindingBytes := range bindingData {
		var binding databasev1.IndexRuleBinding
		if err := protojson.Unmarshal(bindingBytes, &binding); err != nil {
			return errors.Wrapf(err, "failed to unmarshal stream index rule binding %d", i)
		}

		_, err := s.indexBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: &binding,
		})
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Infof("Stream index rule binding %s already exists, skipping", binding.Metadata.Name)
				continue
			}
			return errors.Wrapf(err, "failed to create stream index rule binding: %s", binding.Metadata.Name)
		}
		logger.Infof("Created stream index rule binding: %s", binding.Metadata.Name)
	}

	return nil
}

func (s *SchemaClient) loadTraceIndexRuleBindings(ctx context.Context) error {
	bindingFile := filepath.Join("../testdata", "schema", "trace_index_rule_bindings.json")
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

		_, err := s.indexBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: &binding,
		})
		if err != nil {
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