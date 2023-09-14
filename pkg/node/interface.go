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

// Package node provides node selector for liaison.
package node

import (
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

var (
	_ Selector = (*pickFirstSelector)(nil)

	// ErrNoAvailableNode will be returned if no node is available.
	ErrNoAvailableNode = errors.New("selector: no available node")
)

// Selector keeps all data nodes in the memory and can provide different algorithm to pick an available node.
type Selector interface {
	AddNode(nodeID string)
	RemoveNode(nodeID string)
	Pick(group, name string, shardID uint32) (string, error)
}

// NewPickFirstSelector returns a simple selector that always returns the first node if exists.
func NewPickFirstSelector() (Selector, error) {
	return &pickFirstSelector{}, nil
}

// pickFirstSelector always pick the first node in the sorted node ids list.
type pickFirstSelector struct {
	nodeIDMap map[string]struct{}
	nodeIds   []string
	mu        sync.RWMutex
}

func (p *pickFirstSelector) AddNode(nodeID string) {
	p.mu.RLock()
	if _, ok := p.nodeIDMap[nodeID]; !ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeIDMap[nodeID] = struct{}{}
	p.nodeIds = append(p.nodeIds, nodeID)
	slices.Sort(p.nodeIds)
}

func (p *pickFirstSelector) RemoveNode(nodeID string) {
	p.mu.RLock()
	if _, ok := p.nodeIDMap[nodeID]; !ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeIDMap[nodeID] = struct{}{}
	p.nodeIds = append(p.nodeIds, nodeID)
	slices.Sort(p.nodeIds)
}

func (p *pickFirstSelector) Pick(_, _ string, _ uint32) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.nodeIds) == 0 {
		return "", ErrNoAvailableNode
	}
	return p.nodeIds[0], nil
}

func formatNodeKey(group, name string, shardID uint32) string {
	return group + "/" + name + "#" + strconv.FormatUint(uint64(shardID), 10)
}
