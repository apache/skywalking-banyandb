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
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type roundRobinSelector struct {
	schemaRegistry metadata.Repo
	closeCh        chan struct{}
	lookupTable    sync.Map
	nodes          []string
	mu             sync.RWMutex
}

// NewRoundRobinSelector creates a new round-robin selector.
func NewRoundRobinSelector(schemaRegistry metadata.Repo) Selector {
	rrs := &roundRobinSelector{
		nodes:          make([]string, 0),
		closeCh:        make(chan struct{}),
		schemaRegistry: schemaRegistry,
	}
	return rrs
}

func (r *roundRobinSelector) Name() string {
	return "round-robin-selector"
}

func (r *roundRobinSelector) PreRun(context.Context) error {
	r.schemaRegistry.RegisterHandler("liaison", schema.KindGroup, r)
	return nil
}

func (r *roundRobinSelector) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	group := schemaMetadata.Spec.(*commonv1.Group)
	for i := uint32(0); i < group.ResourceOpts.ShardNum; i++ {
		k := key{group: group.Metadata.Name, shardID: i}
		r.lookupTable.Store(k, 0)
	}
	r.sortEntries()
}

func (r *roundRobinSelector) OnDelete(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	group := schemaMetadata.Spec.(*commonv1.Group)
	for i := uint32(0); i < group.ResourceOpts.ShardNum; i++ {
		k := key{group: group.Metadata.Name, shardID: i}
		r.lookupTable.Delete(k)
	}
	r.sortEntries()
}

func (r *roundRobinSelector) OnInit(kinds []schema.Kind) (bool, []int64) {
	if len(kinds) != 1 {
		return false, nil
	}
	if kinds[0] != schema.KindGroup {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	gg, err := r.schemaRegistry.GroupRegistry().ListGroup(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to list group: %v", err))
	}
	var revision int64
	r.lookupTable = sync.Map{}
	for _, g := range gg {
		if g.Metadata.ModRevision > revision {
			revision = g.Metadata.ModRevision
		}
		for i := uint32(0); i < g.ResourceOpts.ShardNum; i++ {
			k := key{group: g.Metadata.Name, shardID: i}
			r.lookupTable.Store(k, 0)
		}
	}
	r.sortEntries()
	return true, []int64{revision}
}

func (r *roundRobinSelector) AddNode(node *databasev1.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = append(r.nodes, node.Metadata.Name)
	sort.StringSlice(r.nodes).Sort()
}

func (r *roundRobinSelector) RemoveNode(node *databasev1.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, n := range r.nodes {
		if n == node.Metadata.Name {
			r.nodes = append(r.nodes[:i], r.nodes[i+1:]...)
			break
		}
	}
}

func (r *roundRobinSelector) Pick(group, _ string, shardID uint32) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k := key{group: group, shardID: shardID}
	if len(r.nodes) == 0 {
		return "", errors.New("no nodes available")
	}
	entry, ok := r.lookupTable.Load(k)
	if ok {
		return r.selectNode(entry), nil
	}
	return "", fmt.Errorf("%s-%d is a unknown shard", group, shardID)
}

func (r *roundRobinSelector) sortEntries() {
	var keys []key
	r.lookupTable.Range(func(k, _ any) bool {
		keys = append(keys, k.(key))
		return true
	})
	slices.SortFunc(keys, func(a, b key) int {
		n := strings.Compare(a.group, b.group)
		if n != 0 {
			return n
		}
		return int(a.shardID) - int(b.shardID)
	})
	for i := range keys {
		r.lookupTable.Store(keys[i], i)
	}
}

func (r *roundRobinSelector) Close() {
	close(r.closeCh)
}

func (r *roundRobinSelector) selectNode(entry any) string {
	index := entry.(int)
	return r.nodes[index%len(r.nodes)]
}

type key struct {
	group   string
	shardID uint32
}
