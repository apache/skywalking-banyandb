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
	"sort"
	"sync"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
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
	metaRepo  queue.Client
	nodes     []string
	nodeMutex sync.RWMutex
	sync.Once
}

// NewClusterNodeRegistry creates a cluster node registry.
func NewClusterNodeRegistry(metaRepo queue.Client) NodeRegistry {
	nr := &clusterNodeService{
		metaRepo: metaRepo,
	}
	metaRepo.Register(nr)
	return nr
}

func (n *clusterNodeService) Locate(_, _ string, shardID uint32) (string, error) {
	// Use round-robin to select the node.
	n.nodeMutex.RLock()
	defer n.nodeMutex.RUnlock()
	if len(n.nodes) == 0 {
		return "", errors.New("no node available")
	}
	return n.nodes[shardID%uint32(len(n.nodes))], nil
}

func (n *clusterNodeService) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		n.nodeMutex.Lock()
		defer n.nodeMutex.Unlock()
		for _, node := range n.nodes {
			if node == metadata.Spec.(*databasev1.Node).Metadata.Name {
				return
			}
		}
		n.nodes = append(n.nodes, metadata.Spec.(*databasev1.Node).Metadata.Name)
		sort.Strings(n.nodes)
	default:
	}
}

func (n *clusterNodeService) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindNode:
		n.nodeMutex.Lock()
		defer n.nodeMutex.Unlock()
		for i, node := range n.nodes {
			if node == metadata.Spec.(*databasev1.Node).Metadata.Name {
				n.nodes = append(n.nodes[:i], n.nodes[i+1:]...)
				break
			}
		}
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
