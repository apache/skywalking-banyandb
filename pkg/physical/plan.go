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

package physical

import (
	"context"
	"errors"

	"github.com/hashicorp/terraform/dag"

	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

//go:generate mockgen -destination=./plan_mock.go -package=physical . ExecutionContext
type ExecutionContext interface {
	context.Context
	UniModel() series.UniModel
	IndexRepo() index.Repo
}

// Plan is the physical Plan
type Plan struct {
	lp             *logical.Plan
	transformSteps map[dag.Vertex]Transform
}

func (plan *Plan) Run(ec ExecutionContext) (Future, error) {
	var terminatedVertices []dag.Vertex
	// 1) first find all vertices that have outwards = 0
	for _, v := range plan.lp.Vertices() {
		if len(plan.lp.EdgesFrom(v)) == 0 {
			terminatedVertices = append(terminatedVertices, v)
		}
	}

	if len(terminatedVertices) == 0 {
		return nil, errors.New("invalid execution plan")
	}

	visited := make(map[dag.Vertex]Future)

	// 2) run topology-sort reversely
	for _, v := range terminatedVertices {
		plan.topologySort(ec, v, plan.lp.EdgesTo, visited)
	}

	return visited[terminatedVertices[0]], nil
}

type DirectedEdges func(v dag.Vertex) []dag.Edge

func (plan *Plan) topologySort(ec ExecutionContext, vertex dag.Vertex, getEdges DirectedEdges, visited map[dag.Vertex]Future) {
	edges := getEdges(vertex)
	for _, connectedEdge := range edges {
		// check whether we have already run this `Transform`
		if _, ok := visited[connectedEdge.Target()]; !ok {
			plan.topologySort(ec, connectedEdge.Target(), getEdges, visited)
		}
		plan.transformSteps[vertex].AppendParent(visited[connectedEdge.Target()])
	}

	// It's OK here, since DAG must not have cycles
	visited[vertex] = plan.transformSteps[vertex].Run(ec)
}
