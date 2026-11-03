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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func listOf(groups ...*commonv1.Group) GroupLister {
	return func(context.Context) ([]*commonv1.Group, error) { return groups, nil }
}

func TestNewStageSelectorAdmitsOnlyMatchingNodes(t *testing.T) {
	sel, ls, err := NewStageSelector(context.Background(), "type=warm", listOf(groupSchema.Spec.(*commonv1.Group)))
	require.NoError(t, err)
	assert.True(t, ls.Matches(map[string]string{"type": "warm"}))
	sel.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "hot-0"}, Labels: map[string]string{"type": "hot"}})
	sel.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "warm-0"}, Labels: map[string]string{"type": "warm"}})
	sel.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "warm-1"}, Labels: map[string]string{"type": "warm"}})
	for shard := uint32(0); shard < 2; shard++ {
		n, pickErr := sel.Pick("group1", "", shard, 0)
		require.NoError(t, pickErr)
		assert.Contains(t, []string{"warm-0", "warm-1"}, n)
	}
	_, err = sel.Pick("group1", "", 2, 0)
	assert.Error(t, err, "a shard past the group's shard number has no place")
}

func TestNewStageSelectorSkipsInvalidGroups(t *testing.T) {
	sel, _, err := NewStageSelector(context.Background(), "", listOf(
		&commonv1.Group{Metadata: &commonv1.Metadata{Name: "no-opts"}, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		groupSchema.Spec.(*commonv1.Group),
	))
	require.NoError(t, err)
	sel.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "node1"}})
	_, err = sel.Pick("no-opts", "", 0, 0)
	assert.Error(t, err)
	n, err := sel.Pick("group1", "", 0, 0)
	require.NoError(t, err)
	assert.Equal(t, "node1", n)
}

func TestNewStageSelectorErrors(t *testing.T) {
	_, _, err := NewStageSelector(context.Background(), "type=", listOf())
	assert.ErrorIs(t, err, ErrInvalidNodeSelector)

	listErr := errors.New("registry down")
	_, _, err = NewStageSelector(context.Background(), "type=warm", func(context.Context) ([]*commonv1.Group, error) { return nil, listErr })
	assert.ErrorIs(t, err, listErr)
	assert.NotErrorIs(t, err, ErrInvalidNodeSelector)
}
