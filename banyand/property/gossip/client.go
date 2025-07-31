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

package gossip

import (
	"context"
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func (s *service) Propagation(nodes []string, group string, shardID uint32) error {
	if len(nodes) < 2 {
		return fmt.Errorf("must provide at least 2 node")
	}
	// building propagation context
	ctx := &propertyv1.PropagationContext{
		Nodes:      nodes,
		OriginNode: s.nodeID,
	}

	// two rounds of all nodes except the lasted node
	// such when there have three nodes A, B, C,
	// the propagation will be A -> B, B -> C, C -> A, A -> B
	ctx.MaxPropagationCount = int32(len(nodes)*2 - 3)

	// building propagation message request
	request := &propertyv1.PropagationRequest{
		Context: ctx,
		Group:   group,
		ShardId: shardID,
	}

	var sendTo func(context.Context, *propertyv1.PropagationRequest) (*propertyv1.PropagationResponse, error)

	// send it to the current node if it is the first node
	if nodes[0] == s.nodeID {
		sendTo = func(ctx context.Context, req *propertyv1.PropagationRequest) (*propertyv1.PropagationResponse, error) {
			return s.protocolHandler.Propagation(ctx, req)
		}
	} else {
		sendTo = func(ctx context.Context, request *propertyv1.PropagationRequest) (*propertyv1.PropagationResponse, error) {
			node, exist := s.getRegisteredNode(nodes[0])
			if !exist {
				return nil, fmt.Errorf("node %s not found", nodes[0])
			}
			conn, err := s.newConnectionFromNode(node)
			if err != nil {
				return nil, err
			}
			defer func() {
				_ = conn.Close()
			}()
			return propertyv1.NewGossipServiceClient(conn).Propagation(ctx, request)
		}
	}

	go func() {
		cancelCtx, cancelFunc := context.WithTimeout(context.Background(), s.totalTimeout)
		defer cancelFunc()
		_, err := sendTo(cancelCtx, request)
		if err != nil {
			s.log.Warn().Err(err).Msg("propagation failed")
			return
		}
	}()

	return nil
}

func (s *service) LocateNodes(group string, shardID, replicasCount uint32) ([]string, error) {
	result := make([]string, 0, replicasCount)
	for r := range replicasCount {
		node, err := s.sel.Pick(group, "", shardID, r)
		if err != nil {
			return nil, fmt.Errorf("failed to locate node for group %s, shardID %d, replica %d: %w", group, shardID, r, err)
		}
		result = append(result, node)
	}
	return result, nil
}

func (s *service) getRegisteredNode(id string) (*databasev1.Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exist := s.registered[id]
	return node, exist
}

func (s *service) OnAddOrUpdate(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		s.log.Warn().Msg("invalid metadata type")
		return
	}
	s.sel.AddNode(node)
	address := node.PropertyRepairGossipGrpcAddress
	if address == "" {
		s.log.Warn().Stringer("node", node).Msg("node does not have gossip address, skipping registration")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		s.log.Warn().Stringer("node", node).Msg("node does not have a name, skipping registration")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.registered[name] = node
	s.log.Debug().Stringer("node", node).Msg("registered gossip node")
}

func (s *service) OnDelete(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		s.log.Warn().Msg("invalid metadata type")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		s.log.Warn().Stringer("node", node).Msg("node does not have a name, skipping deregistration")
		return
	}
	s.sel.RemoveNode(node)

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.registered, name)
	s.log.Debug().Stringer("node", node).Msg("deregistered gossip node")
}
