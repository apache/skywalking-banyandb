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

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestPickEmptySelector(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	setupGroup(selector)
	_, err := selector.Pick("group1", "", 0)
	assert.Error(t, err)
}

func TestPickUnknownGroup(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	_, err := selector.Pick("group1", "", 0)
	assert.Error(t, err)
	setupGroup(selector)
	_, err = selector.Pick("group1", "", 100)
	assert.Error(t, err)
}

func TestPickSingleSelection(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	setupGroup(selector)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	node, err := selector.Pick("group1", "", 0)
	assert.NoError(t, err)
	assert.Equal(t, "node1", node)
}

func TestPickMultipleSelections(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	setupGroup(selector)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node2"}})

	_, err := selector.Pick("group1", "", 1)
	assert.NoError(t, err)
	node1, err := selector.Pick("group1", "", 0)
	assert.NoError(t, err)
	node2, err := selector.Pick("group1", "", 1)
	assert.NoError(t, err)
	assert.NotEqual(t, node1, node2, "Different shardIDs in the same group should not result in the same node")
}

func TestPickNodeRemoval(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	setupGroup(selector)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node2"}})
	selector.RemoveNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	node, err := selector.Pick("group1", "", 0)
	assert.NoError(t, err)
	assert.Equal(t, "node2", node)
}

func TestPickConsistentSelectionAfterRemoval(t *testing.T) {
	selector := NewRoundRobinSelector(nil)
	setupGroup(selector)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node2"}})
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node3"}})
	_, err := selector.Pick("group1", "", 0)
	assert.NoError(t, err)
	_, err = selector.Pick("group1", "", 1)
	assert.NoError(t, err)
	node, err := selector.Pick("group1", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "node2", node)
	selector.RemoveNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node2"}})
	node, err = selector.Pick("group1", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "node3", node)
}

func TestCleanupGroup(t *testing.T) {
	selector := &roundRobinSelector{
		nodes: make([]string, 0),
	}
	setupGroup(selector)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node2"}})
	_, err := selector.Pick("group1", "", 0)
	assert.NoError(t, err)
	selector.OnDelete(groupSchema)
	_, err = selector.Pick("group1", "", 0)
	assert.Error(t, err)
}

var groupSchema = schema.Metadata{
	TypeMeta: schema.TypeMeta{
		Kind: schema.KindGroup,
	},
	Spec: &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: "group1",
		},
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
		},
	},
}

func setupGroup(selector Selector) {
	selector.(*roundRobinSelector).OnAddOrUpdate(groupSchema)
}
