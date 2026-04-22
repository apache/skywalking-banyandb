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
	"fmt"
	"sort"
	"sync"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

var (
	_ schema.EventHandler = (*clusterNodeService)(nil)
	_ NodeRegistry        = (*clusterNodeService)(nil)
)

// NodeRegistry is for locating data node with group/name of the metadata
// together with the shardID calculated from the incoming data.
type NodeRegistry interface {
	// Locate returns the data node assigned to the given (group, name, shardID, replicaID).
	Locate(group, name string, shardID, replicaID uint32) (string, error)
	// LocateAll returns all distinct data nodes owning the given shard across all replicas.
	LocateAll(group string, shardID uint32, replicas int) ([]string, error)
	fmt.Stringer
}

type clusterNodeService struct {
	schema.UnimplementedOnInitHandler
	pipeline queue.Client
	sel      node.Selector
	l        *logger.Logger
	topic    bus.Topic
	sync.Once
}

// NewClusterNodeRegistry creates a cluster node registry.
func NewClusterNodeRegistry(topic bus.Topic, pipeline queue.Client, selector node.Selector) NodeRegistry {
	nr := &clusterNodeService{
		pipeline: pipeline,
		sel:      selector,
		topic:    topic,
		l:        logger.GetLogger("cluster-node-registry-" + topic.String()),
	}
	nr.Do(func() {
		nr.pipeline.Register(nr.topic, nr)
	})
	return nr
}

func (n *clusterNodeService) Locate(group, name string, shardID, replicaID uint32) (string, error) {
	nodeID, err := n.sel.Pick(group, name, shardID, replicaID)
	if err != nil {
		return "", errors.Wrapf(err, "fail to locate %s/%s(%d,%d)", group, name, shardID, replicaID)
	}
	return nodeID, nil
}

func (n *clusterNodeService) LocateAll(group string, shardID uint32, replicas int) ([]string, error) {
	if replicas < 1 {
		return nil, fmt.Errorf("replicas must be >= 1, got %d", replicas)
	}
	nodeSet := make(map[string]struct{}, replicas)
	for replica := 0; replica < replicas; replica++ {
		nodeID, err := n.Locate(group, "", shardID, uint32(replica))
		if err != nil {
			return nil, errors.Wrapf(err, "fail to locate %s/%d/%d", group, shardID, replica)
		}
		nodeSet[nodeID] = struct{}{}
	}
	nodes := make([]string, 0, len(nodeSet))
	for nodeID := range nodeSet {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	return nodes, nil
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

func (n *clusterNodeService) String() string {
	return n.sel.String()
}

type localNodeService struct{}

func (l localNodeService) String() string {
	return "local"
}

// NewLocalNodeRegistry creates a local(fake) node registry.
func NewLocalNodeRegistry() NodeRegistry {
	return localNodeService{}
}

// Locate of localNodeService always returns local.
func (localNodeService) Locate(_, _ string, _, _ uint32) (string, error) {
	return "local", nil
}

// LocateAll of localNodeService always returns [local].
func (localNodeService) LocateAll(_ string, _ uint32, _ int) ([]string, error) {
	return []string{"local"}, nil
}
