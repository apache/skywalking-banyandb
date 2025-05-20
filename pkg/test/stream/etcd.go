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

// Package stream implements helpers to load schemas for testing.
package stream

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"path"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	streamDir           = "testdata/streams"
	indexRuleDir        = "testdata/index_rules"
	indexRuleBindingDir = "testdata/index_rule_bindings"
)

var (
	//go:embed testdata/index_rules/*.json
	indexRuleStore embed.FS
	//go:embed testdata/index_rule_bindings/*.json
	indexRuleBindingStore embed.FS
	//go:embed testdata/streams/*.json
	streamStore embed.FS
	//go:embed testdata/group.json
	groupJSON string
	//go:embed testdata/group_with_stages.json
	groupWithStagesJSON string
)

// loadSchemas loads streams, index rules, and index rule bindings.
func loadSchemas(ctx context.Context, e schema.Registry) error {
	streams, err := streamStore.ReadDir(streamDir)
	if err != nil {
		return err
	}
	var data []byte
	for _, entry := range streams {
		data, err = streamStore.ReadFile(path.Join(streamDir, entry.Name()))
		if err != nil {
			return err
		}
		var stream databasev1.Stream
		err = protojson.Unmarshal(data, &stream)
		if err != nil {
			return err
		}
		if _, innerErr := e.CreateStream(ctx, &stream); innerErr != nil {
			return innerErr
		}
	}

	entries, err := indexRuleStore.ReadDir(indexRuleDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err = indexRuleStore.ReadFile(path.Join(indexRuleDir, entry.Name()))
		if err != nil {
			return err
		}
		var idxRule databasev1.IndexRule
		err = protojson.Unmarshal(data, &idxRule)
		if err != nil {
			return err
		}
		if innerErr := e.CreateIndexRule(ctx, &idxRule); innerErr != nil {
			return innerErr
		}
	}
	indexRulesBindings, err := indexRuleBindingStore.ReadDir(indexRuleBindingDir)
	if err != nil {
		return err
	}
	for _, entry := range indexRulesBindings {
		data, err = indexRuleBindingStore.ReadFile(path.Join(indexRuleBindingDir, entry.Name()))
		if err != nil {
			return err
		}
		var idxRuleBinding databasev1.IndexRuleBinding
		err = protojson.Unmarshal(data, &idxRuleBinding)
		if err != nil {
			return err
		}
		if innerErr := e.CreateIndexRuleBinding(ctx, &idxRuleBinding); innerErr != nil {
			return innerErr
		}
	}

	return nil
}

// LoadSchemaWithStages loads schemas from files, including group stages.
func LoadSchemaWithStages(ctx context.Context, e schema.Registry) error {
	if e == nil {
		return nil
	}
	var rawGroups []json.RawMessage
	if err := json.Unmarshal([]byte(groupWithStagesJSON), &rawGroups); err != nil {
		return err
	}
	for _, raw := range rawGroups {
		g := &commonv1.Group{}
		if err := protojson.Unmarshal(raw, g); err != nil {
			return err
		}
		_, err := e.GetGroup(ctx, g.Metadata.Name)
		if !errors.Is(err, schema.ErrGRPCResourceNotFound) {
			logger.Infof("group %s already exists", g.Metadata.Name)
			return nil
		}
		if innerErr := e.CreateGroup(ctx, g); innerErr != nil {
			return innerErr
		}
	}
	return loadSchemas(ctx, e)
}

// PreloadSchema loads schemas from files in the booting process.
// This version loads group without stages.
func PreloadSchema(ctx context.Context, e schema.Registry) error {
	if e == nil {
		return nil
	}
	var rawGroups []json.RawMessage
	if err := json.Unmarshal([]byte(groupJSON), &rawGroups); err != nil {
		return err
	}
	for _, raw := range rawGroups {
		g := &commonv1.Group{}
		if err := protojson.Unmarshal(raw, g); err != nil {
			return err
		}
		_, err := e.GetGroup(ctx, g.Metadata.Name)
		if !errors.Is(err, schema.ErrGRPCResourceNotFound) {
			logger.Infof("group %s already exists", g.Metadata.Name)
			return nil
		}
		if innerErr := e.CreateGroup(ctx, g); innerErr != nil {
			return innerErr
		}
	}

	return loadSchemas(ctx, e)
}
