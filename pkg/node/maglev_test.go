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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

const (
	dataNodeTemplate = "data-node-%d"
	targetEpsilon    = 0.1
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

func TestMaglevSelector_EvenDistribution(t *testing.T) {
	sel, err := NewMaglevSelector()
	assert.NoError(t, err)
	dataNodeNum := 10
	for i := 0; i < dataNodeNum; i++ {
		sel.AddNode(&databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: fmt.Sprintf(dataNodeTemplate, i),
			},
		})
	}
	counterMap := make(map[string]int)
	trialCount := 100_000
	for j := 0; j < trialCount; j++ {
		dataNodeID, _ := sel.Pick("sw_metrics", uuid.NewString(), 0)
		val, exist := counterMap[dataNodeID]
		if !exist {
			counterMap[dataNodeID] = 1
		} else {
			counterMap[dataNodeID] = val + 1
		}
	}
	assert.Len(t, counterMap, dataNodeNum)
	for _, count := range counterMap {
		assert.InEpsilon(t, trialCount/dataNodeNum, count, targetEpsilon)
	}
}

func TestMaglevSelector_DiffNode(t *testing.T) {
	fullSel, _ := NewMaglevSelector()
	brokenSel, _ := NewMaglevSelector()
	dataNodeNum := 10
	for i := 0; i < dataNodeNum; i++ {
		fullSel.AddNode(&databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: fmt.Sprintf(dataNodeTemplate, i),
			},
		})
		if i != dataNodeNum-1 {
			brokenSel.AddNode(&databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: fmt.Sprintf(dataNodeTemplate, i),
				},
			})
		}
	}
	diff := 0
	trialCount := 100_000
	for j := 0; j < trialCount; j++ {
		metricName := uuid.NewString()
		fullDataNodeID, _ := fullSel.Pick("sw_metrics", metricName, 0)
		brokenDataNodeID, _ := brokenSel.Pick("sw_metrics", metricName, 0)
		if fullDataNodeID != brokenDataNodeID {
			diff++
		}
	}
	assert.InEpsilon(t, trialCount/dataNodeNum, diff, targetEpsilon*2)
}

func BenchmarkMaglevSelector_Pick(b *testing.B) {
	sel, _ := NewMaglevSelector()
	dataNodeNum := 10
	for i := 0; i < dataNodeNum; i++ {
		sel.AddNode(&databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: fmt.Sprintf(dataNodeTemplate, i),
			},
		})
	}
	metricsCount := 10_000
	metricNames := make([]string, 0, metricsCount)
	for i := 0; i < metricsCount; i++ {
		metricNames = append(metricNames, uuid.NewString())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick("sw_metrics", metricNames[i%metricsCount], 0)
	}
}
