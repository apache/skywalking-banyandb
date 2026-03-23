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

// Package replicatedstream implements helpers to load replicated schemas for testing.
package replicatedstream

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

var (
	//go:embed testdata/group.json
	groupJSON string
	//go:embed testdata/group_with_stages.json
	groupWithStagesJSON string
)

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
	return teststream.PreloadResourcesOnly(ctx, e)
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
	return teststream.PreloadResourcesOnly(ctx, e)
}
