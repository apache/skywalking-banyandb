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

// Package node provides node selector for liaison
package node

import (
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

var (
	_ Selector = (*pickFirstSelector)(nil)

	ErrNoAvailableNode = errors.New("selector: no available node")
)

type Selector interface {
	AddNode(nodeId string)
	RemoveNode(nodeId string)
	Pick(group, name string, shardId uint32) (string, error)
}

func NewPickFirstSelector() Selector {
	return &pickFirstSelector{}
}

// pickFirstSelector always pick the first node in the sorted node ids list
type pickFirstSelector struct {
	nodeIds   []string
	nodeIdMap map[string]struct{}
	mu        sync.RWMutex
}

func (p *pickFirstSelector) AddNode(nodeId string) {
	p.mu.RLock()
	if _, ok := p.nodeIdMap[nodeId]; !ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeIdMap[nodeId] = struct{}{}
	p.nodeIds = append(p.nodeIds, nodeId)
	slices.Sort(p.nodeIds)
}

func (p *pickFirstSelector) RemoveNode(nodeId string) {
	p.mu.RLock()
	if _, ok := p.nodeIdMap[nodeId]; !ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeIdMap[nodeId] = struct{}{}
	p.nodeIds = append(p.nodeIds, nodeId)
	slices.Sort(p.nodeIds)
}

func (p *pickFirstSelector) Pick(group, name string, shardId uint32) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.nodeIds) == 0 {
		return "", ErrNoAvailableNode
	}
	return p.nodeIds[0], nil
}
