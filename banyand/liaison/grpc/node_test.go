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

package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

func TestClusterNodeRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	pipeline := queue.NewMockClient(ctrl)

	sel, err := node.NewPickFirstSelector()
	assert.NoError(t, err)

	cnr := &clusterNodeService{
		pipeline: pipeline,
		sel:      sel,
	}
	pipeline.EXPECT().Register(gomock.Eq(cnr)).Return().Times(1)
	cnr.init()
	fakeNodeID := "data-node-1"
	cnr.OnAddOrUpdate(schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:  schema.KindNode,
			Group: "",
			Name:  fakeNodeID,
		},
		Spec: &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Group: "",
				Name:  fakeNodeID,
			},
		},
	})
	nodeID, err := cnr.Locate("metrics", "instance_traffic", 0)
	assert.NoError(t, err)
	assert.Equal(t, fakeNodeID, nodeID)
}
