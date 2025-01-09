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
	"encoding/json"
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
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type roundRobinSelector struct {
	name           string
	schemaRegistry metadata.Repo
	lookupTable    []key
	nodes          []string
	mu             sync.RWMutex
}

func (r *roundRobinSelector) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]string)
	for _, entry := range r.lookupTable {
		n, err := r.Pick(entry.group, "", entry.shardID)
		key := fmt.Sprintf("%s-%d", entry.group, entry.shardID)
		if err != nil {
			result[key] = fmt.Sprintf("%v", err)
			continue
		}
		result[key] = n
	}
	if len(result) < 1 {
		return ""
	}
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	return convert.BytesToString(jsonBytes)
}

// NewRoundRobinSelector creates a new round-robin selector.
func NewRoundRobinSelector(name string, schemaRegistry metadata.Repo) Selector {
	rrs := &roundRobinSelector{
		name:           name,
		nodes:          make([]string, 0),
		schemaRegistry: schemaRegistry,
		lookupTable:    make([]key, 0),
	}
	return rrs
}

func (r *roundRobinSelector) Name() string {
	return r.name
}

func (r *roundRobinSelector) PreRun(context.Context) error {
	r.schemaRegistry.RegisterHandler(r.name, schema.KindGroup, r)
	return nil
}

func (r *roundRobinSelector) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	group, ok := schemaMetadata.Spec.(*commonv1.Group)
	if !ok || !validateGroup(group) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeGroup(group.Metadata.Name)
	for i := uint32(0); i < group.ResourceOpts.ShardNum; i++ {
		k := key{group: group.Metadata.Name, shardID: i}
		r.lookupTable = append(r.lookupTable, k)
	}
	r.sortEntries()
}

func (r *roundRobinSelector) removeGroup(group string) {
	for i := 0; i < len(r.lookupTable); {
		if r.lookupTable[i].group == group {
			copy(r.lookupTable[i:], r.lookupTable[i+1:])
			r.lookupTable = r.lookupTable[:len(r.lookupTable)-1]
		} else {
			i++
		}
	}
}

func (r *roundRobinSelector) OnDelete(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	group := schemaMetadata.Spec.(*commonv1.Group)
	r.removeGroup(group.Metadata.Name)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	var revision int64
	r.lookupTable = r.lookupTable[:0]
	for _, g := range gg {
		if !validateGroup(g) {
			continue
		}
		if g.Metadata.ModRevision > revision {
			revision = g.Metadata.ModRevision
		}
		for i := uint32(0); i < g.ResourceOpts.ShardNum; i++ {
			k := key{group: g.Metadata.Name, shardID: i}
			r.lookupTable = append(r.lookupTable, k)
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
	i := sort.Search(len(r.lookupTable), func(i int) bool {
		if r.lookupTable[i].group == group {
			return r.lookupTable[i].shardID >= shardID
		}
		return r.lookupTable[i].group > group
	})
	if i < len(r.lookupTable) && r.lookupTable[i] == k {
		return r.selectNode(i), nil
	}
	return "", fmt.Errorf("%s-%d is a unknown shard", group, shardID)
}

func (r *roundRobinSelector) sortEntries() {
	slices.SortFunc(r.lookupTable, func(a, b key) int {
		n := strings.Compare(a.group, b.group)
		if n != 0 {
			return n
		}
		return int(a.shardID) - int(b.shardID)
	})
}

func (r *roundRobinSelector) selectNode(entry any) string {
	index := entry.(int)
	return r.nodes[index%len(r.nodes)]
}

func validateGroup(group *commonv1.Group) bool {
	if group.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return false
	}
	if group.ResourceOpts == nil {
		return false
	}
	return true
}

type key struct {
	group   string
	shardID uint32
}
