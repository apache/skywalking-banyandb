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

package property

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type repair struct {
	shardPath     string
	statePath     string
	treeSlotCount int
}

func newRepair(shardPath string, treeSlotCount int) *repair {
	return &repair{
		shardPath:     shardPath,
		statePath:     path.Join(shardPath, "state.json"),
		treeSlotCount: treeSlotCount,
	}
}

func (r *repair) buildStatus() (*repairStatus, error) {
	state, err := r.readState()
	if err != nil {
		return nil, fmt.Errorf("reading state failure: %w", err)
	}
	indexConfig := index.DefaultConfig(r.shardPath)
	blugeConf := bluge.DefaultConfig(r.shardPath)
	items, err := indexConfig.DirectoryFunc().List(index.ItemKindSegment)
	if err != nil {
		return nil, fmt.Errorf("reading item kind segment failure: %w", err)
	}
	sort.Sort(snapshotIDList(items))
	// check the snapshot ID have any updated
	// if no updates, the building Trees should be skipped
	if state != nil && len(items) != 0 && items[len(items)-1] == state.LastSnapshotID {
		return state, nil
	}

	// otherwise, we need to build the Trees
	tree, err := r.buildingTree(blugeConf, items)
	if err != nil {
		return nil, fmt.Errorf("building Trees failure: %w", err)
	}

	var latestSnapshotID uint64
	if len(items) > 0 {
		latestSnapshotID = items[len(items)-1]
	}
	// save the Trees to the state
	state = &repairStatus{
		LastSnapshotID: latestSnapshotID,
		Trees:          tree,
	}
	stateVal, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshall state failure: %w", err)
	}
	err = os.WriteFile(r.statePath, stateVal, 0o600)
	if err != nil {
		return nil, fmt.Errorf("writing state file failure: %w", err)
	}
	return state, nil
}

func (r *repair) buildingTree(conf bluge.Config, existingSnapshots []uint64) (map[string]*repairTree, error) {
	if len(existingSnapshots) == 0 {
		return make(map[string]*repairTree), nil
	}
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return nil, fmt.Errorf("opening index reader failure: %w", err)
	}
	totalCount, err := reader.Count()
	if err != nil {
		return nil, fmt.Errorf("counting documents failure: %w", err)
	}
	treeBuilders := make(map[string]*repairTreeBuilder)
	for itemID := uint64(0); itemID < totalCount; itemID++ {
		var shaValue string
		var source []byte
		var deleteTime int64
		err = reader.VisitStoredFields(itemID, func(field string, value []byte) bool {
			switch field {
			case shaValueField:
				shaValue = convert.BytesToString(value)
			case sourceField:
				source = value
			case deleteField:
				deleteTime = convert.BytesToInt64(value)
			}
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("visiting stored fields failure: %w", err)
		}

		// building the entity
		if len(source) == 0 {
			continue
		}
		var property propertyv1.Property
		err = protojson.Unmarshal(source, &property)
		if err != nil {
			continue
		}
		entity := GetEntity(&property)
		if shaValue == "" {
			shaValue, err = r.buildingShaValue(source, deleteTime)
			if err != nil {
				continue
			}
		}
		group := property.Metadata.Group
		builder := treeBuilders[group]
		if builder == nil {
			builder = newRepairTreeBuilder(r.treeSlotCount)
			treeBuilders[group] = builder
		}
		builder.append(entity, shaValue, property.Metadata.ModRevision)
	}
	result := make(map[string]*repairTree, len(treeBuilders))
	for group, treeBuilder := range treeBuilders {
		tree, err := treeBuilder.build()
		if err != nil {
			return nil, fmt.Errorf("building tree for group %s failure: %w", group, err)
		}
		result[group] = tree
	}
	return result, nil
}

func (r *repair) buildingShaValue(source []byte, deleteTime int64) (string, error) {
	var err error
	hash := sha512.New()
	_, err = hash.Write(source)
	if err != nil {
		return "", fmt.Errorf("hashing source failure: %w", err)
	}
	_, err = hash.Write([]byte(fmt.Sprintf("%d", deleteTime)))
	if err != nil {
		return "", fmt.Errorf("hashing delete time failure: %w", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (r *repair) readState() (*repairStatus, error) {
	stateFile, err := os.ReadFile(r.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var status repairStatus
	err = json.Unmarshal(stateFile, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

type repairStatus struct {
	Trees          map[string]*repairTree `json:"trees"`
	LastSnapshotID uint64                 `json:"last_snapshot_id"`
}

type repairTree struct {
	Root *repairTreeNode `json:"root"`
}

type repairTreeNode struct {
	ShaValue string            `json:"sha_value"`
	ID       string            `json:"id"`
	Children []*repairTreeNode `json:"children"`
}

type snapshotIDList []uint64

func (s snapshotIDList) Len() int           { return len(s) }
func (s snapshotIDList) Less(i, j int) bool { return s[i] < s[j] }
func (s snapshotIDList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type repairTreeBuilder struct {
	slots []map[string]*repairTreeBuilderNode
}

type repairTreeBuilderNode struct {
	shaValue string
	modTime  int64
}

func newRepairTreeBuilder(slotCount int) *repairTreeBuilder {
	if slotCount < 1 {
		slotCount = 1
	}
	return &repairTreeBuilder{
		slots: make([]map[string]*repairTreeBuilderNode, slotCount),
	}
}

func (r *repairTreeBuilder) append(id, shaVal string, modVersion int64) {
	if len(id) == 0 || len(shaVal) == 0 {
		return
	}
	slotIndex := int(murmur3.Sum32([]byte(id))) % len(r.slots)
	slot := r.slots[slotIndex]
	if slot == nil {
		slot = make(map[string]*repairTreeBuilderNode)
		r.slots[slotIndex] = slot
	}
	node := slot[id]
	// if the node is not exist or the mod version is newer, we need to create a new node
	if node == nil || node.modTime < modVersion {
		node = &repairTreeBuilderNode{
			shaValue: shaVal,
			modTime:  modVersion,
		}
		slot[id] = node
	}
}

func (r *repairTreeBuilder) build() (*repairTree, error) {
	realSlots := make([]*repairTreeNode, 0, len(r.slots))
	var err error
	rootHash := sha512.New()
	for slotInx, slot := range r.slots {
		if slot == nil {
			continue
		}
		slotNode := &repairTreeNode{
			Children: make([]*repairTreeNode, 0, len(slot)),
		}
		realSlots = append(realSlots, slotNode)
		// calculate the sha value for the slot
		slotHash := sha512.New()
		for nodeID, node := range slot {
			_, err = slotHash.Write([]byte(node.shaValue))
			if err != nil {
				return nil, fmt.Errorf("hashing slot Children failure: %w", err)
			}
			slotNode.Children = append(slotNode.Children, &repairTreeNode{
				ShaValue: node.shaValue,
				ID:       nodeID,
			})
		}
		slotHashVal := slotHash.Sum(nil)
		slotNode.ShaValue = fmt.Sprintf("%x", slotHashVal)
		slotNode.ID = strconv.FormatInt(int64(slotInx), 10)
		_, err = rootHash.Write(slotHashVal)
		if err != nil {
			return nil, fmt.Errorf("hashing slot failure: %w", err)
		}
	}

	// building Root node
	return &repairTree{
		Root: &repairTreeNode{
			ID:       "Root",
			Children: realSlots,
			ShaValue: fmt.Sprintf("%x", rootHash.Sum(nil)),
		},
	}, nil
}
