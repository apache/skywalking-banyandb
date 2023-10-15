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
	RegisterHandler(string, schema.Kind, schema.EventHandler)
}

// Service is the metadata repository.
type Service interface {
	Repo
	run.PreRunner
	run.Service
	run.Config
	SchemaRegistry() schema.Registry
}
