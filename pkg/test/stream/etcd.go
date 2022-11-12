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

package stream

import (
	"context"
	"embed"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
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

func PreloadSchema(e schema.Registry) error {
	g := &commonv1.Group{}
	if err := protojson.Unmarshal([]byte(groupJSON), g); err != nil {
		return err
	}
	if innerErr := e.CreateGroup(context.TODO(), g); innerErr != nil {
		return innerErr
	}

	s := &databasev1.Stream{}
	if unmarshalErr := protojson.Unmarshal([]byte(streamJSON), s); unmarshalErr != nil {
		return unmarshalErr
	}
	if innerErr := e.CreateStream(context.TODO(), s); innerErr != nil {
		return innerErr
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if unmarshalErr := protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); unmarshalErr != nil {
		return unmarshalErr
	}
	if innerErr := e.CreateIndexRuleBinding(context.TODO(), indexRuleBinding); innerErr != nil {
		return innerErr
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
		if innerErr := e.CreateIndexRule(context.TODO(), &idxRule); innerErr != nil {
			return innerErr
		}
	}

	return nil
}

func RandomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", uuid.New().String()))
}

var rpcTimeout = 10 * time.Second

func RegisterForNew(addr string) error {
	conn, err := grpchelper.Conn(addr, 1*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := context.Background()
	g := &commonv1.Group{}
	if err = protojson.Unmarshal([]byte(groupJSON), g); err != nil {
		return err
	}
	if err = grpchelper.Request(ctx, rpcTimeout, func(rpcCtx context.Context) (err error) {
		_, err = databasev1.NewGroupRegistryServiceClient(conn).
			Create(rpcCtx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: g,
			})
		return err
	}); err != nil {
		return err
	}

	s := &databasev1.Stream{}
	if unmarshalErr := protojson.Unmarshal([]byte(streamJSON), s); unmarshalErr != nil {
		return unmarshalErr
	}
	if err = grpchelper.Request(ctx, rpcTimeout, func(rpcCtx context.Context) (err error) {
		_, err = databasev1.NewStreamRegistryServiceClient(conn).
			Create(rpcCtx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: s,
			})
		return err
	}); err != nil {
		return err
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if unmarshalErr := protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); unmarshalErr != nil {
		return unmarshalErr
	}
	if err = grpchelper.Request(ctx, rpcTimeout, func(rpcCtx context.Context) (err error) {
		_, err = databasev1.NewIndexRuleBindingRegistryServiceClient(conn).
			Create(rpcCtx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
				IndexRuleBinding: indexRuleBinding,
			})
		return err
	}); err != nil {
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
		idxRule := &databasev1.IndexRule{}
		err = protojson.Unmarshal(data, idxRule)
		if err != nil {
			return err
		}
		if err = grpchelper.Request(ctx, rpcTimeout, func(rpcCtx context.Context) (err error) {
			_, err = databasev1.NewIndexRuleRegistryServiceClient(conn).
				Create(rpcCtx, &databasev1.IndexRuleRegistryServiceCreateRequest{
					IndexRule: idxRule,
				})
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
