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
)

func TestMaglevSelector(t *testing.T) {
	sel, err := NewMaglevSelector()
	assert.NoError(t, err)
	sel.AddNode(&databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: "data-node-1",
		},
	})
	sel.AddNode(&databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: "data-node-2",
		},
	})
	nodeID1, err := sel.Pick("sw_metrics", "traffic_instance", 0)
	assert.NoError(t, err)
	assert.Contains(t, []string{"data-node-1", "data-node-2"}, nodeID1)
	nodeID2, err := sel.Pick("sw_metrics", "traffic_instance", 0)
	assert.NoError(t, err)
	assert.Equal(t, nodeID2, nodeID1)
}
