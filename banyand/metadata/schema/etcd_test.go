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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func Test_Etcd_Get_NotFound(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry()
	defer registry.Close()
	tester.NoError(err)
	tester.NotNil(registry)

	err = registry.preload()
	tester.NoError(err)

	stm, err := registry.GetStream(context.TODO(), &commonv1.Metadata{Name: "unknown", Group: "default"})
	tester.Nil(stm)
	tester.ErrorIs(err, ErrEntityNotFound)
}

func Test_Etcd_Stream_Get_Found(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry()
	defer registry.Close()
	tester.NoError(err)
	tester.NotNil(registry)

	err = registry.preload()
	tester.NoError(err)

	stm, err := registry.GetStream(context.TODO(), &commonv1.Metadata{Name: "sw", Group: "default"})
	tester.NotNil(stm)
	tester.NoError(err)
	tester.Equal(stm.GetMetadata().GetName(), "sw")
}

func Test_Etcd_IndexRuleBinding_Get_Found(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry()
	defer registry.Close()
	tester.NoError(err)
	tester.NotNil(registry)

	err = registry.preload()
	tester.NoError(err)

	entity, err := registry.GetIndexRuleBinding(context.TODO(), &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"})
	tester.NotNil(entity)
	tester.NoError(err)
	tester.Equal(entity.GetMetadata().GetName(), "sw-index-rule-binding")
}

func Test_Etcd_IndexRuleRule_Get_Found(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry()
	defer registry.Close()
	tester.NoError(err)
	tester.NotNil(registry)

	err = registry.preload()
	tester.NoError(err)

	entity, err := registry.GetIndexRuleBinding(context.TODO(), &commonv1.Metadata{Name: "db.instance", Group: "default"})
	tester.NotNil(entity)
	tester.NoError(err)
	tester.Equal(entity.GetMetadata().GetName(), "db.instance")
}

func Test_Etcd_IndexRuleRule_List_Found(t *testing.T) {
	tester := assert.New(t)
	registry, err := NewEtcdSchemaRegistry()
	defer registry.Close()
	tester.NoError(err)
	tester.NotNil(registry)

	err = registry.preload()
	tester.NoError(err)

	entities, err := registry.ListIndexRule(context.TODO(), ListOpt{Group: "default"})
	tester.NoError(err)
	tester.Len(entities, 10)
}
