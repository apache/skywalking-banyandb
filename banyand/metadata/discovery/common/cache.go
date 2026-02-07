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

// Package common provides shared functionality for node discovery implementations.
package common

import (
	"context"
	"fmt"
	"sync"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NodeCacheBase provides common node cache management functionality for discovery services.
type NodeCacheBase struct {
	nodeCache     map[string]*databasev1.Node
	log           *logger.Logger
	handlers      []schema.EventHandler
	cacheMutex    sync.RWMutex
	handlersMutex sync.RWMutex
}

// NewNodeCacheBase creates a new base with initialized maps.
func NewNodeCacheBase(loggerName string) *NodeCacheBase {
	return &NodeCacheBase{
		nodeCache: make(map[string]*databasev1.Node),
		handlers:  make([]schema.EventHandler, 0),
		log:       logger.GetLogger(loggerName),
	}
}

// GetLogger returns the logger.
func (b *NodeCacheBase) GetLogger() *logger.Logger {
	return b.log
}

// ListNode lists all nodes from cache, filtering by role.
func (b *NodeCacheBase) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	b.cacheMutex.RLock()
	defer b.cacheMutex.RUnlock()

	var result []*databasev1.Node
	for _, node := range b.nodeCache {
		if role != databasev1.Role_ROLE_UNSPECIFIED {
			hasRole := false
			for _, nodeRole := range node.GetRoles() {
				if nodeRole == role {
					hasRole = true
					break
				}
			}
			if !hasRole {
				continue
			}
		}
		result = append(result, node)
	}
	return result, nil
}

// StartForNotification replays all cached nodes to registered handlers.
// This case for the cache already cache the nodes(PreRun Phase) before the handlers are registered.
func (b *NodeCacheBase) StartForNotification() {
	b.cacheMutex.RLock()
	cachedNodes := make([]*databasev1.Node, 0, len(b.nodeCache))
	for _, node := range b.nodeCache {
		cachedNodes = append(cachedNodes, node)
	}
	b.cacheMutex.RUnlock()

	for _, handler := range b.handlers {
		for _, node := range cachedNodes {
			handler.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
					Name: node.GetMetadata().GetName(),
				},
				Spec: node,
			})
		}
	}
}

// GetNode retrieves a specific node by name.
func (b *NodeCacheBase) GetNode(_ context.Context, nodeName string) (*databasev1.Node, error) {
	b.cacheMutex.RLock()
	defer b.cacheMutex.RUnlock()

	for _, node := range b.nodeCache {
		if node.GetMetadata() != nil && node.GetMetadata().GetName() == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("node %s not found", nodeName)
}

// RegisterHandler registers an event handler for node changes.
func (b *NodeCacheBase) RegisterHandler(name string, _ schema.Kind, handler schema.EventHandler) {
	b.handlersMutex.Lock()
	defer b.handlersMutex.Unlock()

	b.handlers = append(b.handlers, handler)
	b.log.Debug().Str("handler", name).Msg("Registered node discovery handler")
}

// NotifyHandlers notifies all registered handlers of node changes.
func (b *NodeCacheBase) NotifyHandlers(metadata schema.Metadata, isAddOrUpdate bool) {
	b.handlersMutex.RLock()
	defer b.handlersMutex.RUnlock()

	for _, handler := range b.handlers {
		if isAddOrUpdate {
			handler.OnAddOrUpdate(metadata)
		} else {
			handler.OnDelete(metadata)
		}
	}
}

// AddNode adds a node to the cache.
// Returns true if the node was added, false if it already existed.
func (b *NodeCacheBase) AddNode(address string, node *databasev1.Node) bool {
	b.cacheMutex.Lock()
	defer b.cacheMutex.Unlock()

	if _, exists := b.nodeCache[address]; exists {
		return false
	}
	b.nodeCache[address] = node
	return true
}

// GetCachedNode retrieves a node from cache by address.
// Returns the node and true if found, nil and false otherwise.
func (b *NodeCacheBase) GetCachedNode(address string) (*databasev1.Node, bool) {
	b.cacheMutex.RLock()
	defer b.cacheMutex.RUnlock()
	node, exists := b.nodeCache[address]
	return node, exists
}

// GetCacheSize returns the current number of nodes in the cache.
func (b *NodeCacheBase) GetCacheSize() int {
	b.cacheMutex.RLock()
	defer b.cacheMutex.RUnlock()
	return len(b.nodeCache)
}

// GetAllNodeAddresses returns all addresses currently in the cache.
func (b *NodeCacheBase) GetAllNodeAddresses() []string {
	b.cacheMutex.RLock()
	defer b.cacheMutex.RUnlock()

	addresses := make([]string, 0, len(b.nodeCache))
	for addr := range b.nodeCache {
		addresses = append(addresses, addr)
	}
	return addresses
}

// RemoveNodes removes multiple nodes from the cache.
// Returns a map of removed nodes keyed by address.
func (b *NodeCacheBase) RemoveNodes(addresses []string) map[string]*databasev1.Node {
	b.cacheMutex.Lock()
	defer b.cacheMutex.Unlock()

	removed := make(map[string]*databasev1.Node)
	for _, addr := range addresses {
		if node, exists := b.nodeCache[addr]; exists {
			delete(b.nodeCache, addr)
			removed[addr] = node
		}
	}
	return removed
}

// AddNodeAndNotify adds a node to the cache and notifies handlers if added successfully.
// Returns true if the node was added, false if it already existed.
func (b *NodeCacheBase) AddNodeAndNotify(address string, node *databasev1.Node, logMsg string) bool {
	if added := b.AddNode(address, node); added {
		// notify handlers
		b.NotifyHandlers(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
				Name: node.GetMetadata().GetName(),
			},
			Spec: node,
		}, true)

		b.log.Debug().
			Str("address", address).
			Str("name", node.GetMetadata().GetName()).
			Msg(logMsg)

		return true
	}
	return false
}

// RemoveNodesAndNotify removes nodes and notifies handlers for each removed node.
// Returns a map of removed nodes keyed by address.
func (b *NodeCacheBase) RemoveNodesAndNotify(addresses []string, logMsg string) map[string]*databasev1.Node {
	removed := b.RemoveNodes(addresses)

	// notify handlers for deletions
	for addr, node := range removed {
		b.NotifyHandlers(schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
				Name: node.GetMetadata().GetName(),
			},
			Spec: node,
		}, false)

		b.log.Debug().
			Str("address", addr).
			Str("name", node.GetMetadata().GetName()).
			Msg(logMsg)
	}

	return removed
}
