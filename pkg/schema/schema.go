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

// Package schema implements a framework to sync schema info from the metadata repository.
package schema

import (
	"io"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// EventType defines actions of events.
type EventType uint8

// EventType support Add/Update and Delete.
// All events are idempotent.
const (
	EventAddOrUpdate EventType = iota
	EventDelete
)

// EventKind defines category of events.
type EventKind uint8

// This framework groups events to a hierarchy. A group is the root node.
const (
	EventKindGroup EventKind = iota
	EventKindResource
	EventKindIndexRule
	EventKindIndexRuleBinding
)

// Group is the root node, allowing get resources from its sub nodes.
type Group interface {
	GetSchema() *commonv1.Group
	SupplyTSDB() io.Closer
}

// MetadataEvent is the syncing message between metadata repo and this framework.
type MetadataEvent struct {
	Metadata ResourceSchema
	Typ      EventType
	Kind     EventKind
}

// ResourceSchema allows get the metadata.
type ResourceSchema interface {
	GetMetadata() *commonv1.Metadata
}

// Resource allows access metadata from a local cache.
type Resource interface {
	Schema() ResourceSchema
	Delegated() IndexListener
}

// Supplier allows open a tsdb.
type Supplier interface {
	SupplyTSDB() io.Closer
}

// IndexListener listens to the index update.
type IndexListener interface {
	OnIndexUpdate(index []*databasev1.IndexRule)
}

// ResourceSchemaSupplier allows get a ResourceSchema from the metadata.
type ResourceSchemaSupplier interface {
	ResourceSchema(metadata *commonv1.Metadata) (ResourceSchema, error)
	OpenResource(spec Resource) (IndexListener, error)
}

// ResourceSupplier allows open a resource and its embedded tsdb.
type ResourceSupplier interface {
	ResourceSchemaSupplier
	OpenDB(groupSchema *commonv1.Group) (DB, error)
}

// DB is the interface of a tsdb.
type DB interface {
	io.Closer
	UpdateOptions(opts *commonv1.ResourceOpts)
}

// Repository is the collection of several hierarchies groups by a "Group".
type Repository interface {
	Watcher()
	Init(schema.Kind) ([]string, []int64)
	SendMetadataEvent(MetadataEvent)
	LoadGroup(name string) (Group, bool)
	LoadAllGroups() []Group
	LoadResource(metadata *commonv1.Metadata) (Resource, bool)
	Close()
	StopCh() <-chan struct{}
}
