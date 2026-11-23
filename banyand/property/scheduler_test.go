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

package property

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func newGroupListScheduler(t *testing.T, groups []*commonv1.Group, listErr error) *repairScheduler {
	ctrl := gomock.NewController(t)
	groupRegistry := schema.NewMockGroup(ctrl)
	groupRegistry.EXPECT().ListGroup(gomock.Any()).Return(groups, listErr)
	repo := metadata.NewMockRepo(ctrl)
	repo.EXPECT().GroupRegistry().Return(groupRegistry)
	return &repairScheduler{metadata: repo}
}

func testGroup(name string, catalog commonv1.Catalog, replicas uint32) *commonv1.Group {
	return &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: name},
		Catalog:      catalog,
		ResourceOpts: &commonv1.ResourceOpts{ShardNum: 2, Replicas: replicas},
	}
}

func TestRandomSelectGroup_NoRepairableGroup(t *testing.T) {
	scheduler := newGroupListScheduler(t, []*commonv1.Group{
		testGroup("single-copy-property", commonv1.Catalog_CATALOG_PROPERTY, 0),
		testGroup("replicated-stream", commonv1.Catalog_CATALOG_STREAM, 1),
	}, nil)

	_, _, err := scheduler.randomSelectGroup(context.Background())
	require.ErrorIs(t, err, errNoRepairableGroup)
}

func TestRandomSelectGroup_ListGroupFailure(t *testing.T) {
	listErr := errors.New("registry unavailable")
	scheduler := newGroupListScheduler(t, nil, listErr)

	_, _, err := scheduler.randomSelectGroup(context.Background())
	require.ErrorIs(t, err, listErr)
	require.NotErrorIs(t, err, errNoRepairableGroup)
}
