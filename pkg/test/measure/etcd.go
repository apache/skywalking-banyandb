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

package measure

import (
	"context"
	"embed"
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
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
)

var (
	//go:embed testdata/*
	store embed.FS
)

func PreloadSchema(e schema.Registry) error {
	if err := loadSchema(groupDir, &commonv1.Group{}, func(group proto.Message) error {
		return e.UpdateGroup(context.TODO(), group.(*commonv1.Group), true)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(measureDir, &databasev1.Measure{}, func(group proto.Message) error {
		return e.UpdateMeasure(context.TODO(), group.(*databasev1.Measure), true)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(indexRuleDir, &databasev1.IndexRule{}, func(group proto.Message) error {
		return e.UpdateIndexRule(context.TODO(), group.(*databasev1.IndexRule), true)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := loadSchema(indexRuleBindingDir, &databasev1.IndexRuleBinding{}, func(group proto.Message) error {
		return e.UpdateIndexRuleBinding(context.TODO(), group.(*databasev1.IndexRuleBinding), true)
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func RandomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", uuid.New().String()))
}

func loadSchema(dir string, resource proto.Message, loadFn func(resource proto.Message) error) error {
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
