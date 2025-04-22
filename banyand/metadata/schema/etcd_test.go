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

package schema_test

import (
	"context"
	"embed"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const indexRuleDir = "testdata/index_rules"

var (
	//go:embed testdata/index_rules/*.json
	indexRuleStore embed.FS
	//go:embed testdata/index_rule_binding.json
	indexRuleBindingJSON string
	//go:embed testdata/stream.json
	streamJSON string
	//go:embed testdata/group.json
	groupJSON string
)

func preloadSchema(e schema.Registry) error {
	g := &commonv1.Group{}
	if err := protojson.Unmarshal([]byte(groupJSON), g); err != nil {
		return err
	}
	if err := e.CreateGroup(context.TODO(), g); err != nil {
		return err
	}

	s := &databasev1.Stream{}
	if err := protojson.Unmarshal([]byte(streamJSON), s); err != nil {
		return err
	}
	_, err := e.CreateStream(context.Background(), s)
	if err != nil {
		return err
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err = protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
		return err
	}
	err = e.CreateIndexRuleBinding(context.Background(), indexRuleBinding)
	if err != nil {
		return err
	}

	entries, err := indexRuleStore.ReadDir(indexRuleDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := indexRuleStore.ReadFile(path.Join(indexRuleDir, entry.Name()))
		if err != nil {
			return err
		}
		var idxRule databasev1.IndexRule
		err = protojson.Unmarshal(data, &idxRule)
		if err != nil {
			return err
		}
		err = e.CreateIndexRule(context.Background(), &idxRule)
		if err != nil {
			return err
		}
	}

	return nil
}

func initServerAndRegister(t *testing.T) (schema.Registry, func()) {
	path, defFn := test.Space(require.New(t))
	req := require.New(t)
	ports, err := test.AllocateFreePorts(2)
	if err != nil {
		panic("fail to find free ports")
	}
	endpoints := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(path),
		embeddedetcd.AutoCompactionMode("periodic"),
		embeddedetcd.AutoCompactionRetention("1h"),
		embeddedetcd.QuotaBackendBytes(2*1024*1024),
	)
	req.NoError(err)
	req.NotNil(server)
	<-server.ReadyNotify()
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(schema.ConfigureServerEndpoints(endpoints))
	req.NoError(err)
	req.NotNil(server)
	return schemaRegistry, func() {
		server.Close()
		<-server.StopNotify()
		schemaRegistry.Close()
		defFn()
	}
}

func Test_Etcd_Entity_Get(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()
	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		meta        *commonv1.Metadata
		get         func(schema.Registry, *commonv1.Metadata) (schema.HasMetadata, error)
		name        string
		expectedErr bool
	}{
		{
			name: "Get Group",
			meta: &commonv1.Metadata{Name: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				stm, innerErr := registry.GetGroup(context.TODO(), meta.GetName())
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(stm), nil
			},
		},
		{
			name: "Get Stream",
			meta: &commonv1.Metadata{Name: "sw", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				stm, innerErr := registry.GetStream(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(stm), nil
			},
		},
		{
			name: "Get IndexRuleBinding",
			meta: &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetIndexRuleBinding(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
			},
		},
		{
			name: "Get IndexRule",
			meta: &commonv1.Metadata{Name: "db.instance", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetIndexRule(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
			},
		},
		{
			name: "Get unknown Measure",
			meta: &commonv1.Metadata{Name: "unknown-stream", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetMeasure(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
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
				req.Greater(entity.GetMetadata().GetCreateRevision(), int64(0))
				req.Greater(entity.GetMetadata().GetModRevision(), int64(0))
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
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		list        func(schema.Registry) (int, error)
		name        string
		expectedLen int
	}{
		{
			name: "List Group",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListGroup(context.TODO())
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List Stream",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListStream(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRuleBinding",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRuleBinding(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRule",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 10,
		},
		{
			name: "List Measure",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListMeasure(context.TODO(), schema.ListOpt{Group: "default"})
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

func Test_Etcd_Delete(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		list              func(schema.Registry) (int, error)
		delete            func(schema.Registry) error
		name              string
		expectedLenBefore int
		expectedLenAfter  int
	}{
		{
			name: "Delete IndexRule",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r schema.Registry) error {
				_, innerErr := r.DeleteIndexRule(context.TODO(), &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				return innerErr
			},
			expectedLenBefore: 10,
			expectedLenAfter:  9,
		},
		{
			name: "Delete Group",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r schema.Registry) error {
				_, innerErr := r.DeleteGroup(context.TODO(), "default")
				return innerErr
			},
			expectedLenBefore: 9,
			expectedLenAfter:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			num, err := tt.list(registry)
			ast.NoError(err)
			ast.Equal(num, tt.expectedLenBefore)
			err = tt.delete(registry)
			ast.NoError(err)
			num, err = tt.list(registry)
			ast.NoError(err)
			ast.Equal(num, tt.expectedLenAfter)
		})
	}
}

func Test_Etcd_Entity_Update(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		updateFunc     func(context.Context, schema.Registry) error
		validationFunc func(context.Context, schema.Registry) bool
		name           string
	}{
		{
			name: "update indexRule when none metadata-id",
			updateFunc: func(ctx context.Context, r schema.Registry) error {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return err
				}
				// reset
				ir.Metadata.Id = 0
				ir.Type = databasev1.IndexRule_TYPE_INVERTED
				return r.UpdateIndexRule(ctx, ir)
			},
			validationFunc: func(ctx context.Context, r schema.Registry) bool {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return false
				}
				if ir.Metadata.Id != 1 {
					return false
				}
				if ir.Type != databasev1.IndexRule_TYPE_INVERTED {
					return false
				}
				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			err := tt.updateFunc(context.TODO(), registry)
			req.NoError(err)

			req.True(tt.validationFunc(context.TODO(), registry))
		})
	}
}
