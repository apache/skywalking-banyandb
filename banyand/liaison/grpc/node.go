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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

var (
	_ schema.EventHandler = (*clusterNodeService)(nil)
	_ NodeRegistry        = (*clusterNodeService)(nil)
)

type NodeRegistry interface {
	Locate(group, name string, shardId uint32) (string, error)
}

type clusterNodeService struct {
	metaRepo metadata.Repo
	sel      node.Selector
}

func NewClusterNodeRegistry(metaRepo metadata.Repo) NodeRegistry {
	cns := &clusterNodeService{
		metaRepo: metaRepo,
		sel:      node.NewPickFirstSelector(),
	}
	cns.metaRepo.RegisterHandler("cluster-node-service", schema.KindNode, cns)
	return cns
}

func (n *clusterNodeService) Locate(group, name string, shardId uint32) (string, error) {
	nodeId, err := n.sel.Pick(group, name, shardId)
	if err != nil {
		return "", errors.Wrapf(err, "fail to locate %s/%s(%d)", group, name, shardId)
	}
	return nodeId, nil
}

func (n *clusterNodeService) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		n.sel.AddNode(metadata.Name)
	default:
	}
}

func (n *clusterNodeService) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		n.sel.RemoveNode(metadata.Name)
	default:
	}
}

type localNodeService struct{}

func NewLocalNodeRegistry() NodeRegistry {
	return localNodeService{}
}

// Locate of localNodeService always returns
func (localNodeService) Locate(_group, _name string, _shardId uint32) (string, error) {
	return "local", nil
}
