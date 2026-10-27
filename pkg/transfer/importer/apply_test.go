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

package importer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

func TestBarrierKeySkipsOnlyPropertyGroups(t *testing.T) {
	group := func(catalog commonv1.Catalog) *SchemaAction {
		return &SchemaAction{Kind: schemasync.KindGroup, Group: "g", Spec: &commonv1.Group{Metadata: &commonv1.Metadata{Name: "g"}, Catalog: catalog}}
	}
	assert.Equal(t, &schemav1.SchemaKey{Kind: "group", Name: "g"}, barrierKey(group(commonv1.Catalog_CATALOG_MEASURE)))
	assert.Nil(t, barrierKey(group(commonv1.Catalog_CATALOG_PROPERTY)), "a property group")
	assert.Equal(t, &schemav1.SchemaKey{Kind: "index_rule_binding", Group: "g", Name: "b"},
		barrierKey(&SchemaAction{Kind: schemasync.KindIndexRuleBinding, Group: "g", Name: "b", Spec: &databasev1.IndexRuleBinding{}}))
	assert.Equal(t, &schemav1.SchemaKey{Kind: "index_rule", Group: "g", Name: "r"},
		barrierKey(&SchemaAction{Kind: schemasync.KindIndexRule, Group: "g", Name: "r", Spec: &databasev1.IndexRule{}}))
	assert.Equal(t, &schemav1.SchemaKey{Kind: "property", Group: "g", Name: "p"},
		barrierKey(&SchemaAction{Kind: schemasync.KindProperty, Group: "g", Name: "p", Spec: &databasev1.Property{}}))
}
