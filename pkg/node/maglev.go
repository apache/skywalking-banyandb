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

package node

import (
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/kkdai/maglev"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

const lookupTableSize = 65537

var _ Selector = (*maglevSelector)(nil)

type maglevSelector struct {
	routers sync.Map
	nodes   []string
	mutex   sync.RWMutex
}

func (m *maglevSelector) Name() string {
	return "maglev-selector"
}

func (m *maglevSelector) PreRun(context.Context) error {
	return nil
}

func (m *maglevSelector) AddNode(node *databasev1.Node) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := range m.nodes {
		if m.nodes[i] == node.GetMetadata().GetName() {
			return
		}
	}
	m.nodes = append(m.nodes, node.GetMetadata().GetName())
	sort.StringSlice(m.nodes).Sort()
	m.routers.Range(func(_, value any) bool {
		_ = value.(*maglev.Maglev).Set(m.nodes)
		return true
	})
}

func (m *maglevSelector) RemoveNode(node *databasev1.Node) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := range m.nodes {
		if m.nodes[i] == node.GetMetadata().GetName() {
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			break
		}
	}
	m.routers.Range(func(_, value any) bool {
		_ = value.(*maglev.Maglev).Set(m.nodes)
		return true
	})
}

func (m *maglevSelector) Pick(group, name string, shardID uint32) (string, error) {
	router, ok := m.routers.Load(group)
	if ok {
		return router.(*maglev.Maglev).Get(formatSearchKey(name, shardID))
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	router, ok = m.routers.Load(group)
	if ok {
		return router.(*maglev.Maglev).Get(formatSearchKey(name, shardID))
	}

	mTab, err := maglev.NewMaglev(m.nodes, lookupTableSize)
	if err != nil {
		return "", err
	}
	m.routers.Store(group, mTab)
	return mTab.Get(formatSearchKey(name, shardID))
}

// NewMaglevSelector creates a new backend selector based on Maglev hashing algorithm.
func NewMaglevSelector() Selector {
	return &maglevSelector{}
}

func formatSearchKey(name string, shardID uint32) string {
	return name + "-" + strconv.FormatUint(uint64(shardID), 10)
}
