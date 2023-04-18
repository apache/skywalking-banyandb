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

// Package measure implements helpers to load schemas for testing.
package measure

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
	measureDir          = "testdata/measures"
	indexRuleDir        = "testdata/index_rules"
	indexRuleBindingDir = "testdata/index_rule_bindings"
	topNAggregationDir  = "testdata/topn_aggregations"
)

//go:embed testdata/*
var store embed.FS

// PreloadSchema loads schemas from files in the booting process.
func PreloadSchema(e schema.Registry) error {
	if err := loadSchema(groupDir, &commonv1.Group{}, func(group *commonv1.Group) error {
		return e.CreateGroup(context.TODO(), group)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(measureDir, &databasev1.Measure{}, func(measure *databasev1.Measure) error {
		return e.CreateMeasure(context.TODO(), measure)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(indexRuleDir, &databasev1.IndexRule{}, func(indexRule *databasev1.IndexRule) error {
		return e.CreateIndexRule(context.TODO(), indexRule)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(indexRuleBindingDir, &databasev1.IndexRuleBinding{}, func(indexRuleBinding *databasev1.IndexRuleBinding) error {
		return e.CreateIndexRuleBinding(context.TODO(), indexRuleBinding)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(topNAggregationDir, &databasev1.TopNAggregation{}, func(topN *databasev1.TopNAggregation) error {
		return e.CreateTopNAggregation(context.TODO(), topN)
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func loadSchema[T proto.Message](dir string, resource T, loadFn func(resource T) error) error {
	entries, err := store.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := store.ReadFile(path.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		resource.ProtoReflect().Descriptor().RequiredNumbers()
		if err := protojson.Unmarshal(data, resource); err != nil {
			return err
		}
		if err := loadFn(resource); err != nil {
			return err
		}
	}
	return nil
}
