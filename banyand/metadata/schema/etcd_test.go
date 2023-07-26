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
	"embed"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
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

	_ EventHandler = (*mockedEventHandler)(nil)
)

type mockedEventHandler struct {
	mock.Mock
}

func (m *mockedEventHandler) OnAddOrUpdate(metadata Metadata) {
	m.Called(metadata)
}

func (m *mockedEventHandler) OnDelete(metadata Metadata) {
	m.Called(metadata)
}

func preloadSchema(e Registry) error {
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
	err := e.CreateStream(context.Background(), s)
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
		data, err := indexRuleStore.ReadFile(indexRuleDir + "/" + entry.Name())
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

func randomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", uuid.New().String()))
}

func initServerAndRegister(t *testing.T) (Registry, func()) {
	req := require.New(t)
	ports, err := test.AllocateFreePorts(2)
	if err != nil {
		panic("fail to find free ports")
	}
	endpoints := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(randomTempDir()))
	req.NoError(err)
	req.NotNil(server)
	<-server.ReadyNotify()
	schemaRegistry, err := NewEtcdSchemaRegistry(ConfigureServerEndpoints(endpoints))
	req.NoError(err)
	req.NotNil(server)
	return schemaRegistry, func() {
		server.Close()
		<-server.StopNotify()
		schemaRegistry.Close()
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
		get         func(Registry, *commonv1.Metadata) (HasMetadata, error)
		name        string
		expectedErr bool
	}{
		{
			name: "Get Group",
			meta: &commonv1.Metadata{Name: "default"},
			get: func(r Registry, meta *commonv1.Metadata) (HasMetadata, error) {
				stm, innerErr := registry.GetGroup(context.TODO(), meta.GetName())
				if innerErr != nil {
					return nil, innerErr
				}
				return HasMetadata(stm), nil
			},
		},
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
			meta: &commonv1.Metadata{Name: "unknown-stream", Group: "default"},
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
		list        func(Registry) (int, error)
		name        string
		expectedLen int
	}{
		{
			name: "List Group",
			list: func(r Registry) (int, error) {
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
			name: "List IndexRuleBinding",
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
			name: "List IndexRule",
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
			name: "List Measure",
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

func Test_Etcd_Delete(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		list              func(Registry) (int, error)
		delete            func(Registry) error
		name              string
		expectedLenBefore int
		expectedLenAfter  int
	}{
		{
			name: "Delete IndexRule",
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r Registry) error {
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
			list: func(r Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r Registry) error {
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

func Test_Notify(t *testing.T) {
	req := require.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	req.NoError(err)

	tests := []struct {
		testFunc       func(context.Context, Registry) error
		validationFunc func(*mockedEventHandler) bool
		name           string
	}{
		{
			name: "modify indexRule",
			testFunc: func(ctx context.Context, r Registry) error {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return err
				}

				ir.Type = databasev1.IndexRule_TYPE_TREE
				return r.UpdateIndexRule(ctx, ir)
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 1) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 0)
			},
		},
		{
			name: "modify indexRule without modification",
			testFunc: func(ctx context.Context, r Registry) error {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return err
				}

				return r.UpdateIndexRule(ctx, ir)
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 0) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 0)
			},
		},
		{
			name: "delete indexRule",
			testFunc: func(ctx context.Context, r Registry) error {
				deleted, err := r.DeleteIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})

				if !deleted {
					return errors.New("fail to delete object")
				}

				return err
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 0) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 1)
			},
		},
		{
			name: "update indexRuleBinding",
			testFunc: func(ctx context.Context, r Registry) error {
				irb, err := r.GetIndexRuleBinding(ctx, &commonv1.Metadata{
					Name:  "sw-index-rule-binding",
					Group: "default",
				})
				if err != nil {
					return err
				}

				irb.Rules = []string{"trace_id", "duration"}
				return r.UpdateIndexRuleBinding(ctx, irb)
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 1) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 0)
			},
		},
		{
			name: "update indexRuleBinding without modification",
			testFunc: func(ctx context.Context, r Registry) error {
				irb, err := r.GetIndexRuleBinding(ctx, &commonv1.Metadata{
					Name:  "sw-index-rule-binding",
					Group: "default",
				})
				if err != nil {
					return err
				}

				return r.UpdateIndexRuleBinding(ctx, irb)
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 0) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 0)
			},
		},
		{
			name: "delete indexRuleBinding",
			testFunc: func(ctx context.Context, r Registry) error {
				_, err := r.DeleteIndexRuleBinding(ctx, &commonv1.Metadata{
					Name:  "sw-index-rule-binding",
					Group: "default",
				})
				if err != nil {
					return err
				}

				return nil
			},
			validationFunc: func(mocked *mockedEventHandler) bool {
				return mocked.AssertNumberOfCalls(t, "OnAddOrUpdate", 0) &&
					mocked.AssertNumberOfCalls(t, "OnDelete", 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			mockedObj := new(mockedEventHandler)
			mockedObj.On("OnAddOrUpdate", mock.Anything).Return()
			mockedObj.On("OnDelete", mock.Anything).Return()
			registry.RegisterHandler(KindStream|KindIndexRuleBinding|KindIndexRule, mockedObj)

			err := tt.testFunc(context.TODO(), registry)
			req.NoError(err)

			req.True(tt.validationFunc(mockedObj))
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
		updateFunc     func(context.Context, Registry) error
		validationFunc func(context.Context, Registry) bool
		name           string
	}{
		{
			name: "update indexRule when none metadata-id",
			updateFunc: func(ctx context.Context, r Registry) error {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return err
				}
				// reset
				ir.Metadata.Id = 0
				ir.Type = databasev1.IndexRule_TYPE_TREE
				return r.UpdateIndexRule(ctx, ir)
			},
			validationFunc: func(ctx context.Context, r Registry) bool {
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
				if ir.Type != databasev1.IndexRule_TYPE_TREE {
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
