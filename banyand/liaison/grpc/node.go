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
	"sync"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

var (
	_ schema.EventHandler = (*clusterNodeService)(nil)
	_ NodeRegistry        = (*clusterNodeService)(nil)
)

// NodeRegistry is for locating data node with group/name of the metadata
// together with the shardID calculated from the incoming data.
type NodeRegistry interface {
	Locate(group, name string, shardID uint32) (string, error)
}

type clusterNodeService struct {
	pipeline queue.Client
	sel      node.Selector
	sync.Once
}

// NewClusterNodeRegistry creates a cluster node registry.
func NewClusterNodeRegistry(pipeline queue.Client, selector node.Selector) NodeRegistry {
	nr := &clusterNodeService{
		pipeline: pipeline,
		sel:      selector,
	}
	nr.init()
	return nr
}

func (n *clusterNodeService) init() {
	n.Do(func() {
		n.pipeline.Register(n)
	})
}

func (n *clusterNodeService) Locate(group, name string, shardID uint32) (string, error) {
	nodeID, err := n.sel.Pick(group, name, shardID)
	if err != nil {
		return "", errors.Wrapf(err, "fail to locate %s/%s(%d)", group, name, shardID)
	}
	return nodeID, nil
}

func (n *clusterNodeService) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		inputNode := metadata.Spec.(*databasev1.Node)
		if inputNode.Metadata.GetName() == "" {
			return
		}
		n.sel.AddNode(inputNode)
	default:
	}
}

func (n *clusterNodeService) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		dNode := metadata.Spec.(*databasev1.Node)
		if dNode.Metadata.GetName() == "" {
			return
		}
		n.sel.RemoveNode(dNode)
	default:
	}
}

type localNodeService struct{}

// NewLocalNodeRegistry creates a local(fake) node registry.
func NewLocalNodeRegistry() NodeRegistry {
	return localNodeService{}
}

// Locate of localNodeService always returns local.
func (localNodeService) Locate(_, _ string, _ uint32) (string, error) {
	return "local", nil
}
