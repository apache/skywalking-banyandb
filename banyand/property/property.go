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

// Package property provides the property service interface.
package property

import (
	"context"
	"strconv"
	"strings"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// WithDeleteTime wraps a property with its delete time for cross-node sync.
type WithDeleteTime struct {
	Property   *propertyv1.Property
	DeleteTime int64
}

// DirectService provides direct access to property operations without going through pipeline.
type DirectService interface {
	// DirectInsert directly insert a property.
	DirectInsert(ctx context.Context, group string, shardID uint32, id []byte, property *propertyv1.Property) error
	// DirectUpdate directly update a property.
	DirectUpdate(ctx context.Context, group string, shardID uint32, id []byte, property *propertyv1.Property) error
	// DirectDelete directly deletes properties by their document IDs.
	DirectDelete(ctx context.Context, ids [][]byte) error
	// DirectQuery directly queries properties with deleteTime info.
	DirectQuery(ctx context.Context, req *propertyv1.QueryRequest) ([]*WithDeleteTime, error)
	// DirectGet directly gets a single property (filters out deleted).
	DirectGet(ctx context.Context, group, name, id string) (*propertyv1.Property, error)
	// DirectExist checks if a non-deleted property exists without unmarshalling the full data.
	DirectExist(ctx context.Context, group, name, id string) (bool, error)
	// DirectRepair repairs a property on this node with specified deleteTime.
	DirectRepair(ctx context.Context, shardID uint64, id []byte, prop *propertyv1.Property, deleteTime int64) error
}

// Service is the interface for property service.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
	route.TableProvider
	DirectService

	GetGossIPGrpcPort() *uint32
	GetGossIPMessenger() gossip.Messenger
	NewMetadataRegister() run.Unit
}

// GroupStoreConfig holds per-group storage configuration for the inverted index.
type GroupStoreConfig struct {
	BatchWaitSec       int64
	WaitForPersistence bool
}

// GetPropertyID returns the property ID based on the property metadata and revision.
func GetPropertyID(prop *propertyv1.Property) []byte {
	return convert.StringToBytes(GetEntity(prop) + "/" + strconv.FormatInt(prop.Metadata.ModRevision, 10))
}

// GetEntity returns the entity string for the property.
func GetEntity(prop *propertyv1.Property) string {
	return strings.Join([]string{prop.Metadata.Group, prop.Metadata.Name, prop.Id}, "/")
}
