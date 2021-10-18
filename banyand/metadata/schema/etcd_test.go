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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

type HasMetadata interface {
	GetMetadata() *commonv1.Metadata
}

func useRandomTempDir() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.rootDir = RandomTempDir()
	}
}

func useUnixDomain() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.listenerClientURL, config.listenerPeerURL = RandomUnixDomainListener()
	}
}

func Test_Etcd_Entity_Get(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry(PreloadSchema(), useUnixDomain(), useRandomTempDir())
	tester.NoError(err)
	tester.NotNil(registry)
	defer registry.Close()

	tests := []struct {
		name        string
		meta        *commonv1.Metadata
		get         func(Registry, *commonv1.Metadata) (HasMetadata, error)
		expectedErr bool
	}{
		{
			name: "Get Stream",
			meta: &commonv1.Metadata{Name: "sw", Group: "default"},
			get: func(r Registry, meta *commonv1.Metadata) (HasMetadata, error) {
				stm, innerErr := registry.GetStream(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return HasMetadata(stm), nil
			},
		},
		{
			name: "Get IndexRuleBinding",
			meta: &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"},
			get: func(r Registry, meta *commonv1.Metadata) (HasMetadata, error) {
				e, innerErr := registry.GetIndexRuleBinding(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return HasMetadata(e), nil
			},
		},
		{
			name: "Get IndexRule",
			meta: &commonv1.Metadata{Name: "db.instance", Group: "default"},
			get: func(r Registry, meta *commonv1.Metadata) (HasMetadata, error) {
				e, innerErr := registry.GetIndexRule(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return HasMetadata(e), nil
			},
		},
		{
			name: "Get unknown Measure",
			meta: &commonv1.Metadata{Name: "unknown-measure", Group: "default"},
			get: func(r Registry, meta *commonv1.Metadata) (HasMetadata, error) {
				e, innerErr := registry.GetMeasure(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return HasMetadata(e), nil
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			entity, err := tt.get(registry, tt.meta)
			if !tt.expectedErr {
				req.NoError(err)
				req.NotNil(entity)
				req.Equal(entity.GetMetadata().GetGroup(), tt.meta.GetGroup())
				req.Equal(entity.GetMetadata().GetName(), tt.meta.GetName())
			} else {
				req.Error(err)
			}
		})
	}
}

func Test_Etcd_Entity_List(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry(PreloadSchema(), useUnixDomain(), useRandomTempDir())
	tester.NoError(err)
	tester.NotNil(registry)
	defer registry.Close()

	tests := []struct {
		name        string
		list        func(Registry) (int, error)
		expectedLen int
	}{
		{
			name: "List Stream without Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListStream(context.TODO(), ListOpt{})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List Stream with Group default",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListStream(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRuleBinding without Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRuleBinding(context.TODO(), ListOpt{})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRuleBinding with Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRuleBinding(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRule without Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), ListOpt{})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 10,
		},
		{
			name: "List IndexRule with Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 10,
		},
		{
			name: "List Measure without Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListMeasure(context.TODO(), ListOpt{})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 0,
		},
		{
			name: "List Measure with Group",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListMeasure(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			entitiesLen, listErr := tt.list(registry)
			req.NoError(listErr)
			req.Equal(entitiesLen, tt.expectedLen)
		})
	}
}
