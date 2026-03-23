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

// Package trace implements helpers to load replicated schemas for testing.
package trace

import (
	"context"
	"embed"
	"path"
	"reflect"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	testtrace "github.com/apache/skywalking-banyandb/pkg/test/trace"
)

const groupDir = "testdata/groups"

//go:embed testdata/*
var store embed.FS

// PreloadSchema loads schemas from files in the booting process.
func PreloadSchema(ctx context.Context, e schema.Registry) error {
	if err := loadSchema(groupDir, &commonv1.Group{}, func(group *commonv1.Group) error {
		return e.CreateGroup(ctx, group)
	}); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(testtrace.PreloadResourcesOnly(ctx, e))
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
		newResource := newProtoMessage(resource)
		if unmarshalErr := protojson.Unmarshal(data, newResource); unmarshalErr != nil {
			return unmarshalErr
		}
		if loadErr := loadFn(newResource); loadErr != nil {
			if errors.Is(loadErr, schema.ErrGRPCAlreadyExists) {
				continue
			}
			return loadErr
		}
	}
	return nil
}

// newProtoMessage creates a new instance of the same type as the template.
func newProtoMessage[T proto.Message](template T) T {
	v := reflect.New(reflect.TypeOf(template).Elem()).Interface().(T)
	return v
}
