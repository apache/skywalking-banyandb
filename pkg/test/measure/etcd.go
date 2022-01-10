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

package measure

import (
	"context"
	"embed"
	"fmt"
	"math/rand"
	"os"
	"path"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const indexRuleDir = "testdata/index_rules"

var (
	//go:embed testdata/index_rules/*.json
	indexRuleStore embed.FS
	//go:embed testdata/index_rule_binding.json
	indexRuleBindingJSON string
	//go:embed testdata/measure.json
	measureJSON string
)

func PreloadSchema(e schema.Registry) error {
	s := &databasev1.Measure{}
	if err := protojson.Unmarshal([]byte(measureJSON), s); err != nil {
		return err
	}
	err := e.UpdateMeasure(context.Background(), s)
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
		err = e.UpdateIndexRule(context.Background(), &idxRule)
		if err != nil {
			return err
		}
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err = protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
		return err
	}
	err = e.UpdateIndexRuleBinding(context.Background(), indexRuleBinding)
	if err != nil {
		return err
	}

	return nil
}

func RandomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", uuid.New().String()))
}

func RandomUnixDomainListener() (string, string) {
	i := rand.Uint64()
	return fmt.Sprintf("%s://localhost:%d%06d", "unix", os.Getpid(), i),
		fmt.Sprintf("%s://localhost:%d%06d", "unix", os.Getpid(), i+1)
}
