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

package tracepipeline_test

import (
	"context"
	"embed"
	"path"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	groupDir            = "testdata/groups"
	traceDir            = "testdata/traces"
	indexRuleDir        = "testdata/index_rules"
	indexRuleBindingDir = "testdata/index_rule_bindings"
)

//go:embed testdata/*
var store embed.FS

// PreloadSchema loads the pipeline test group and its trace/index-rule schemas.
// It is intentionally scoped to the test-trace-pipeline group so that suites
// preloading the shared trace fixtures are not affected by this group's data.
func PreloadSchema(ctx context.Context, e schema.Registry) error {
	loaders := []func(context.Context, schema.Registry) error{
		func(ctx context.Context, e schema.Registry) error {
			return loadSchema(groupDir, &commonv1.Group{}, func(group *commonv1.Group) error {
				_, innerErr := e.CreateGroup(ctx, group)
				return innerErr
			})
		},
		func(ctx context.Context, e schema.Registry) error {
			return loadSchema(traceDir, &databasev1.Trace{}, func(trace *databasev1.Trace) error {
				_, innerErr := e.CreateTrace(ctx, trace)
				return innerErr
			})
		},
		func(ctx context.Context, e schema.Registry) error {
			return loadSchema(indexRuleDir, &databasev1.IndexRule{}, func(indexRule *databasev1.IndexRule) error {
				_, innerErr := e.CreateIndexRule(ctx, indexRule)
				return innerErr
			})
		},
		func(ctx context.Context, e schema.Registry) error {
			return loadSchema(indexRuleBindingDir, &databasev1.IndexRuleBinding{}, func(indexRuleBinding *databasev1.IndexRuleBinding) error {
				_, innerErr := e.CreateIndexRuleBinding(ctx, indexRuleBinding)
				return innerErr
			})
		},
	}
	for _, loader := range loaders {
		if err := loader(ctx, e); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func loadSchema[T proto.Message](dir string, resource T, loadFn func(resource T) error) error {
	entries, err := store.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, readErr := store.ReadFile(path.Join(dir, entry.Name()))
		if readErr != nil {
			return readErr
		}
		if unmarshalErr := protojson.Unmarshal(data, resource); unmarshalErr != nil {
			return unmarshalErr
		}
		if createErr := loadFn(resource); createErr != nil {
			if errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
				continue
			}
			return createErr
		}
	}
	return nil
}
