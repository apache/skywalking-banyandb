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

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
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

	_, err := e.GetGroup(context.TODO(), g.GetMetadata().GetName())
	if err != nil && schema.IsNotFound(err) {
		if innerErr := e.CreateGroup(context.TODO(), g); innerErr != nil {
			return innerErr
		}
	} else if err != nil {
		return err
	} else {
		if innerErr := e.UpdateGroup(context.TODO(), g); innerErr != nil {
			return innerErr
		}
	}

	s := &databasev1.Stream{}
	if err := protojson.Unmarshal([]byte(streamJSON), s); err != nil {
		return err
	}
	_, err = e.GetStream(context.Background(), s.GetMetadata())
	if err != nil && schema.IsNotFound(err) {
		if innerErr := e.CreateStream(context.TODO(), s); innerErr != nil {
			return innerErr
		}
	} else if err != nil {
		return err
	} else {
		if innerErr := e.UpdateStream(context.TODO(), s); innerErr != nil {
			return innerErr
		}
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err = protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
		return err
	}

	_, err = e.GetIndexRuleBinding(context.Background(), indexRuleBinding.GetMetadata())
	if err != nil && schema.IsNotFound(err) {
		if innerErr := e.CreateIndexRuleBinding(context.TODO(), indexRuleBinding); innerErr != nil {
			return innerErr
		}
	} else if err != nil {
		return err
	} else {
		if innerErr := e.UpdateIndexRuleBinding(context.TODO(), indexRuleBinding); innerErr != nil {
			return innerErr
		}
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
		_, err = e.GetIndexRule(context.Background(), idxRule.GetMetadata())
		if err != nil && schema.IsNotFound(err) {
			if innerErr := e.CreateIndexRule(context.TODO(), &idxRule); innerErr != nil {
				return innerErr
			}
		} else if err != nil {
			return err
		} else {
			if innerErr := e.UpdateIndexRule(context.TODO(), &idxRule); innerErr != nil {
				return innerErr
			}
		}
	}

	return nil
}

func RandomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", uuid.New().String()))
}
