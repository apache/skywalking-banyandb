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
	"context"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const repairBatchSearchSize = 100

type repair struct {
	shardPath       string
	statePath       string
	tmpFilePath     string
	treeSlotCount   int
	batchSearchSize int
}

func newRepair(shardPath string, batchSearchSize, treeSlotCount int) *repair {
	return &repair{
		shardPath:       shardPath,
		statePath:       path.Join(shardPath, "state.json"),
		tmpFilePath:     path.Join(shardPath, "state.tmp"),
		treeSlotCount:   treeSlotCount,
		batchSearchSize: batchSearchSize,
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
	if len(items) == 0 {
		return nil, nil
	}

	// otherwise, we need to build the Trees
	tree, err := r.buildTree(blugeConf)
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

func (r *repair) buildTree(conf bluge.Config) (map[string]*repairTree, error) {
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return nil, fmt.Errorf("opening index reader failure: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()
	topNSearch := bluge.NewTopNSearch(r.batchSearchSize, bluge.NewMatchAllQuery())
	topNSearch.SortBy([]string{
		fmt.Sprintf("+%s", groupField),
		fmt.Sprintf("+%s", nameField),
		fmt.Sprintf("+%s", entityID),
		fmt.Sprintf("+%s", timestampField),
	})

	var latestProperty *searchingProperty
	treeComposer, err := newRepairTreeComposer(r.tmpFilePath, r.treeSlotCount)
	if err != nil {
		return nil, fmt.Errorf("creating repair tree composer failure: %w", err)
	}
	defer func() {
		_ = treeComposer.closeAndRemove()
	}()
	err = r.pageSearch(reader, topNSearch, func(source []byte, shaValue string, deleteTime int64) error {
		// building the entity
		if len(source) == 0 {
			return nil
		}
		var property propertyv1.Property
		err = protojson.Unmarshal(source, &property)
		if err != nil {
			return err
		}
		entity := GetEntity(&property)
		if shaValue == "" {
			shaValue, err = r.buildShaValue(source, deleteTime)
			if err != nil {
				return fmt.Errorf("building sha value failure: %w", err)
			}
		}

		s := newSearchingProperty(&property, shaValue, entity)
		if latestProperty != nil && latestProperty.entityID != entity {
			// the entity is changed, need to save the property
			if err = treeComposer.append(latestProperty.group, latestProperty.entityID, latestProperty.shaValue); err != nil {
				return fmt.Errorf("appending property to tree composer failure: %w", err)
			}
		}
		latestProperty = s
		return nil
	})
	if err != nil {
		return nil, err
	}
	// if the latestProperty is not nil, it means the latest property need to be saved
	if latestProperty != nil {
		if err = treeComposer.append(latestProperty.group, latestProperty.entityID, latestProperty.shaValue); err != nil {
			return nil, fmt.Errorf("appending latest property to tree composer failure: %w", err)
		}
	}

	result := make(map[string]*repairTree)
	trees, err := treeComposer.compose()
	for group, treeBuilder := range trees {
		tree, err := treeBuilder.build()
		if err != nil {
			return nil, fmt.Errorf("building tree for group %s failure: %w", group, err)
		}
		result[group] = tree
	}
	return result, nil
}

func (r *repair) pageSearch(reader *bluge.Reader, searcher *bluge.TopNSearch, each func(source []byte, shaValue string, deleteTime int64) error) error {
	var latestDocValues [][]byte
	for {
		searcher.After(latestDocValues)
		result, err := reader.Search(context.Background(), searcher)
		if err != nil {
			return fmt.Errorf("searching index failure: %w", err)
		}

		next, err := result.Next()
		var hitNumber int
		if err != nil {
			return errors.WithMessage(err, "iterate document match iterator")
		}
		// if next is nil, it means no more documents to process
		if next == nil {
			return nil
		}
		var shaValue string
		var source []byte
		var deleteTime int64
		for err == nil && next != nil {
			hitNumber = next.HitNumber
			var errTime error
			err = next.VisitStoredFields(func(field string, value []byte) bool {
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
			if err = multierr.Combine(err, errTime); err != nil {
				return errors.WithMessagef(err, "visit stored fields, hit: %d", hitNumber)
			}
			err = each(source, shaValue, deleteTime)
			if err != nil {
				return errors.WithMessagef(err, "processing source failure, hit: %d", hitNumber)
			}
			latestDocValues = next.SortValue
			next, err = result.Next()
		}
	}
}

func (r *repair) buildShaValue(source []byte, deleteTime int64) (string, error) {
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

type repairTreeComposer struct {
	file      *os.File
	path      string
	slotCount int
}

func newRepairTreeComposer(path string, slotCount int) (r *repairTreeComposer, err error) {
	if path == "" {
		return nil, fmt.Errorf("repair tree file path cannot be empty")
	}
	r = &repairTreeComposer{
		path:      path,
		slotCount: slotCount,
	}
	r.file, err = os.OpenFile(r.path, os.O_CREATE|os.O_RDWR, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("opening repair tree file %s failure: %w", r.path, err)
	}
	return
}

func (r *repairTreeComposer) append(group, id, shaVal string) (err error) {
	if err = r.writeString(group); err != nil {
		return fmt.Errorf("writing group %s to repair tree file %s failure: %w", group, r.path, err)
	}
	if err = r.writeString(id); err != nil {
		return fmt.Errorf("writing id %s to repair tree file %s failure: %w", id, r.path, err)
	}
	if err = r.writeString(shaVal); err != nil {
		return fmt.Errorf("writing sha value %s to repair tree file %s failure: %w", shaVal, r.path, err)
	}
	return nil
}

func (r *repairTreeComposer) writeString(val string) error {
	bytes := []byte(val)
	if err := binary.Write(r.file, binary.LittleEndian, int32(len(bytes))); err != nil {
		return fmt.Errorf("writing string %s to repair tree file %s failure: %w", val, r.path, err)
	}
	_, err := r.file.Write(bytes)
	return err
}

func (r *repairTreeComposer) readString() (v string, err error) {
	var length int32
	if err = binary.Read(r.file, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	bytes := make([]byte, length)
	if _, err = io.ReadFull(r.file, bytes); err != nil {
		return "", fmt.Errorf("reading string from repair tree file %s failure: %w", r.path, err)
	}
	return string(bytes), nil
}

func (r *repairTreeComposer) closeAndRemove() (err error) {
	err = r.file.Close()
	if err != nil {
		return err
	}
	if err = os.Remove(r.path); err != nil {
		return err
	}
	return err
}

func (r *repairTreeComposer) compose() (map[string]*repairTreeBuilder, error) {
	if err := r.file.Sync(); err != nil {
		return nil, fmt.Errorf("syncing repair tree file %s failure: %w", r.path, err)
	}
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start of repair tree file %s failure: %w", r.path, err)
	}

	treeBuilders := make(map[string]*repairTreeBuilder)
	var readErr error
	for readErr == nil || errors.Is(readErr, io.EOF) {
		var groupID, id, shaVal string
		if groupID, readErr = r.readString(); readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break // end of file reached
			}
			return nil, fmt.Errorf("reading group from repair tree file %s failure: %w", r.path, readErr)
		}
		if id, readErr = r.readString(); readErr != nil {
			return nil, fmt.Errorf("reading id from repair tree file %s failure: %w", r.path, readErr)
		}
		if shaVal, readErr = r.readString(); readErr != nil {
			return nil, fmt.Errorf("reading sha value from repair tree file %s failure: %w", r.path, readErr)
		}

		builder := treeBuilders[groupID]
		if builder == nil {
			builder = newRepairTreeBuilder(r.slotCount)
			treeBuilders[groupID] = builder
		}
		builder.append(id, shaVal)
	}
	return treeBuilders, nil
}

type repairTreeBuilder struct {
	Slots [][]*repairTreeBuilderNode
}

type repairTreeBuilderNode struct {
	ID       string
	ShaValue string
}

func newRepairTreeBuilder(slotCount int) *repairTreeBuilder {
	if slotCount < 1 {
		slotCount = 1
	}
	return &repairTreeBuilder{
		Slots: make([][]*repairTreeBuilderNode, slotCount),
	}
}

func (r *repairTreeBuilder) append(id, shaVal string) {
	val := xxhash.Sum64([]byte(id))
	slotIndex := val % uint64(len(r.Slots))
	r.Slots[slotIndex] = append(r.Slots[slotIndex], &repairTreeBuilderNode{
		ID:       id,
		ShaValue: shaVal,
	})
}

func (r *repairTreeBuilder) build() (*repairTree, error) {
	realSlots := make([]*repairTreeNode, 0, len(r.Slots))
	var err error
	rootHash := sha512.New()
	for slotInx, slot := range r.Slots {
		if slot == nil {
			continue
		}
		slotNode := &repairTreeNode{
			Children: make([]*repairTreeNode, 0, len(slot)),
		}
		realSlots = append(realSlots, slotNode)
		// calculate the sha value for the slot
		slotHash := sha512.New()
		for _, node := range slot {
			_, err = slotHash.Write([]byte(node.ShaValue))
			if err != nil {
				return nil, fmt.Errorf("hashing slot Children failure: %w", err)
			}
			slotNode.Children = append(slotNode.Children, &repairTreeNode{
				ShaValue: node.ShaValue,
				ID:       node.ID,
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

type searchingProperty struct {
	group    string
	shaValue string
	entityID string
}

func newSearchingProperty(property *propertyv1.Property, shaValue, entity string) *searchingProperty {
	return &searchingProperty{
		group:    property.Metadata.Group,
		shaValue: shaValue,
		entityID: entity,
	}
}
