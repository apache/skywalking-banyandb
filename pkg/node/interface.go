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
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ Selector = (*pickFirstSelector)(nil)

	// ErrNoAvailableNode will be returned if no node is available.
	ErrNoAvailableNode = errors.New("selector: no available node")
)

// Selector keeps all data nodes in the memory and can provide different algorithm to pick an available node.
//
//go:generate mockgen -destination=mock/node_selector_mock.go -package=mock github.com/apache/skywalking-banyandb/pkg/node Selector
type Selector interface {
	AddNode(node *databasev1.Node)
	RemoveNode(node *databasev1.Node)
	SetNodeSelector(selector *pub.LabelSelector)
	Pick(group, name string, shardID, replicaID uint32) (string, error)
	run.PreRunner
	fmt.Stringer
}

// NewPickFirstSelector returns a simple selector that always returns the first node if exists.
func NewPickFirstSelector() (Selector, error) {
	return &pickFirstSelector{
		nodeIDMap: make(map[string]struct{}),
	}, nil
}

// pickFirstSelector always pick the first node in the sorted node ids list.
type pickFirstSelector struct {
	nodeIDMap map[string]struct{}
	nodeIDs   []string
	mu        sync.RWMutex
}

// SetNodeSelector implements Selector.
func (p *pickFirstSelector) SetNodeSelector(_ *pub.LabelSelector) {}

// String implements Selector.
func (p *pickFirstSelector) String() string {
	n, err := p.Pick("", "", 0, 0)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("pick [%s] from %s", n, p.nodeIDs)
}

func (p *pickFirstSelector) PreRun(context.Context) error {
	return nil
}

func (p *pickFirstSelector) Name() string {
	return "pick-first"
}

func (p *pickFirstSelector) AddNode(node *databasev1.Node) {
	nodeID := node.GetMetadata().GetName()
	p.mu.RLock()
	if _, ok := p.nodeIDMap[nodeID]; ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeIDMap[nodeID] = struct{}{}
	p.nodeIDs = append(p.nodeIDs, nodeID)
	slices.Sort(p.nodeIDs)
}

func (p *pickFirstSelector) RemoveNode(node *databasev1.Node) {
	nodeID := node.GetMetadata().GetName()
	p.mu.RLock()
	if _, ok := p.nodeIDMap[nodeID]; !ok {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodeIDMap, nodeID)
	idx, ok := slices.BinarySearch(p.nodeIDs, nodeID)
	if !ok {
		return
	}
	p.nodeIDs = slices.Delete(p.nodeIDs, idx, idx+1)
}

func (p *pickFirstSelector) Pick(_, _ string, _, _ uint32) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.nodeIDs) == 0 {
		return "", ErrNoAvailableNode
	}
	return p.nodeIDs[0], nil
}
