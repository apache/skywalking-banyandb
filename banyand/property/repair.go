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
	"bufio"
	"context"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

const repairBatchSearchSize = 100

type repair struct {
	takeSnapshot             func(string) error
	ctx                      context.Context
	cancel                   context.CancelFunc
	repairTreeCron           *cron.Cron
	quickRepairNotified      *int32
	metrics                  *repairMetrics
	l                        *logger.Logger
	shardPath                string
	repairBasePath           string
	snapshotDir              string
	statePath                string
	composeAppendFilePath    string
	composeTreeFilePathFmt   string
	repairBuildTreeCron      string
	repairQuickBuildTreeTime time.Duration
	treeSlotCount            int
	batchSearchSize          int
	repairTreeCronTask       cron.EntryID
}

func newRepair(
	shardPath string,
	l *logger.Logger,
	metricsFactory *observability.Factory,
	batchSearchSize,
	treeSlotCount int,
	repairBuildTreeCron string,
	repairQuickBuildTreeTime time.Duration,
	takeSnapshot func(string) error,
) (r *repair, err error) {
	repairBase := path.Join(shardPath, "repair")
	c := cron.New()
	var quickRepairNotified int32
	ctx, cancelFunc := context.WithCancel(context.Background())
	r = &repair{
		shardPath:                shardPath,
		l:                        l,
		repairBasePath:           repairBase,
		snapshotDir:              path.Join(repairBase, "snapshot"),
		statePath:                path.Join(repairBase, "state.json"),
		composeAppendFilePath:    path.Join(repairBase, "state-append.tmp"),
		composeTreeFilePathFmt:   path.Join(repairBase, "state-tree-%s.json"),
		treeSlotCount:            treeSlotCount,
		batchSearchSize:          batchSearchSize,
		takeSnapshot:             takeSnapshot,
		repairBuildTreeCron:      repairBuildTreeCron,
		repairQuickBuildTreeTime: repairQuickBuildTreeTime,
		repairTreeCron:           c,
		quickRepairNotified:      &quickRepairNotified,
		metrics:                  newRepairMetrics(metricsFactory),
		ctx:                      ctx,
		cancel:                   cancelFunc,
	}
	r.repairTreeCronTask, err = c.AddJob(repairBuildTreeCron, c)
	if err != nil {
		return nil, fmt.Errorf("failed to add repair build tree cron task: %w", err)
	}
	c.Start()
	return r, nil
}

func (r *repair) documentUpdatesNotify() {
	if !atomic.CompareAndSwapInt32(r.quickRepairNotified, 0, 1) {
		return
	}

	// stop the normal cron job
	r.repairTreeCron.Remove(r.repairTreeCronTask)

	go func() {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.repairQuickBuildTreeTime):
			r.Run()
			var err error
			// after the quick repair, we can start the normal cron job again
			r.repairTreeCronTask, err = r.repairTreeCron.AddJob(r.repairBuildTreeCron, r)
			if err != nil {
				r.l.Err(fmt.Errorf("repair build tree cron task err: %w", err))
			}
			atomic.StoreInt32(r.quickRepairNotified, 0)
		}
	}()
}

func (r *repair) Run() {
	_, err := r.buildStatus()
	if err != nil {
		r.l.Err(fmt.Errorf("repair build status failure: %w", err))
		return
	}
}

func (r *repair) buildStatus() (state *repairStatus, err error) {
	startTime := time.Now()
	defer func() {
		r.metrics.totalBuildTreeFinished.Inc(1)
		if err != nil {
			r.metrics.totalBuildTreeFailures.Inc(1)
		}
		r.metrics.totalBuildTreeDuration.Inc(time.Since(startTime).Seconds())
	}()
	// reading the state file to check they have any updates
	state, err = r.readState()
	if err != nil {
		return nil, fmt.Errorf("reading state failure: %w", err)
	}
	indexConfig := index.DefaultConfig(r.shardPath)
	items, err := indexConfig.DirectoryFunc().List(index.ItemKindSegment)
	if err != nil {
		return nil, fmt.Errorf("reading item kind segment failure: %w", err)
	}
	sort.Sort(snapshotIDList(items))
	// check the snapshot ID have any updated
	// if no updates, the building Trees should be skipped
	if state != nil && len(items) != 0 && items[len(items)-1] == state.LastSnpID {
		return state, nil
	}
	if len(items) == 0 {
		return nil, nil
	}

	// otherwise, we need to building the trees
	// take a new snapshot first
	err = r.takeSnapshot(r.snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("taking snapshot failure: %w", err)
	}
	blugeConf := bluge.DefaultConfig(r.snapshotDir)
	err = r.buildTree(blugeConf)
	if err != nil {
		return nil, fmt.Errorf("building trees failure: %w", err)
	}

	var latestSnapshotID uint64
	if len(items) > 0 {
		latestSnapshotID = items[len(items)-1]
	}
	// save the Trees to the state
	state = &repairStatus{
		LastSnpID:    latestSnapshotID,
		LastSyncTime: time.Now(),
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

func (r *repair) buildTree(conf bluge.Config) error {
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		return fmt.Errorf("opening index reader failure: %w", err)
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
	treeComposer, err := newRepairTreeComposer(r.composeAppendFilePath, r.composeTreeFilePathFmt, r.treeSlotCount)
	if err != nil {
		return fmt.Errorf("creating repair tree composer failure: %w", err)
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
		if latestProperty != nil {
			if latestProperty.group != property.Metadata.Group {
				// if the group have changed, we need to append the latest property to the tree composer, and compose builder
				// the entity is changed, need to save the property
				if err = treeComposer.append(latestProperty.group, latestProperty.entityID, latestProperty.shaValue); err != nil {
					return fmt.Errorf("appending property to tree composer failure: %w", err)
				}
				err = treeComposer.composeNextGroupAndSave()
				if err != nil {
					return fmt.Errorf("composing group failure: %w", err)
				}
			} else if latestProperty.entityID != entity {
				// the entity is changed, need to save the property
				if err = treeComposer.append(latestProperty.group, latestProperty.entityID, latestProperty.shaValue); err != nil {
					return fmt.Errorf("appending property to tree composer failure: %w", err)
				}
			}
		}
		latestProperty = s
		return nil
	})
	if err != nil {
		return err
	}
	// if the latestProperty is not nil, it means the latest property need to be saved
	if latestProperty != nil {
		if err = treeComposer.append(latestProperty.group, latestProperty.entityID, latestProperty.shaValue); err != nil {
			return fmt.Errorf("appending latest property to tree composer failure: %w", err)
		}
		err = treeComposer.composeNextGroupAndSave()
		if err != nil {
			return fmt.Errorf("composing last group failure: %w", err)
		}
	}

	return nil
}

func (r *repair) pageSearch(reader *bluge.Reader, searcher *bluge.TopNSearch, each func(source []byte, shaValue string, deleteTime int64) error) error {
	var latestDocValues [][]byte
	for {
		searcher.After(latestDocValues)
		result, err := reader.Search(r.ctx, searcher)
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

func (r *repair) readTree(group string) (*repairTree, error) {
	if group == "" {
		return nil, fmt.Errorf("group cannot be empty when reading repair tree")
	}
	treeFile := fmt.Sprintf(r.composeTreeFilePathFmt, group)
	treeFileContent, err := os.ReadFile(treeFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no tree file found for the group
		}
		return nil, fmt.Errorf("reading repair tree file %s failure: %w", treeFile, err)
	}
	var tree repairTree
	err = json.Unmarshal(treeFileContent, &tree)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling repair tree file %s failure: %w", treeFile, err)
	}
	return &tree, nil
}

func (r *repair) close() {
	r.cancel()
	r.repairTreeCron.Stop()
}

type repairStatus struct {
	LastSyncTime time.Time `json:"last_sync_time"`
	LastSnpID    uint64    `json:"last_snp_id"`
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
	file        *os.File
	fileWriter  *bufio.Writer
	fileReader  *bufio.Reader
	appendFile  string
	treeFileFmt string
	slotCount   int

	lastComposeOffset int64
}

func newRepairTreeComposer(appendFilePath, treeFilePathFmt string, slotCount int) (r *repairTreeComposer, err error) {
	if appendFilePath == "" {
		return nil, fmt.Errorf("repair tree file path cannot be empty")
	}
	r = &repairTreeComposer{
		appendFile:  appendFilePath,
		treeFileFmt: treeFilePathFmt,
		slotCount:   slotCount,
	}
	r.file, err = os.OpenFile(r.appendFile, os.O_CREATE|os.O_RDWR, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("opening repair tree file %s failure: %w", r.appendFile, err)
	}
	r.fileWriter = bufio.NewWriter(r.file)
	r.fileReader = bufio.NewReader(r.file)
	return
}

func (r *repairTreeComposer) append(group, id, shaVal string) (err error) {
	if err = r.writeString(group); err != nil {
		return fmt.Errorf("writing group %s to repair tree file %s failure: %w", group, r.appendFile, err)
	}
	if err = r.writeString(id); err != nil {
		return fmt.Errorf("writing id %s to repair tree file %s failure: %w", id, r.appendFile, err)
	}
	if err = r.writeString(shaVal); err != nil {
		return fmt.Errorf("writing sha value %s to repair tree file %s failure: %w", shaVal, r.appendFile, err)
	}
	return nil
}

func (r *repairTreeComposer) writeString(val string) error {
	bytes := []byte(val)
	if err := binary.Write(r.fileWriter, binary.LittleEndian, int32(len(bytes))); err != nil {
		return fmt.Errorf("writing string %s to repair tree file %s failure: %w", val, r.appendFile, err)
	}
	_, err := r.fileWriter.Write(bytes)
	return err
}

func (r *repairTreeComposer) readString() (v string, length int32, err error) {
	if err = binary.Read(r.fileReader, binary.LittleEndian, &length); err != nil {
		return "", 0, err
	}
	bytes := make([]byte, length)
	if _, err = io.ReadFull(r.fileReader, bytes); err != nil {
		return "", 0, fmt.Errorf("reading string from repair tree file %s failure: %w", r.appendFile, err)
	}
	return string(bytes), length + 4, nil
}

func (r *repairTreeComposer) closeAndRemove() (err error) {
	err = r.file.Close()
	if err != nil {
		return err
	}
	if err = os.Remove(r.appendFile); err != nil {
		return err
	}
	return err
}

func (r *repairTreeComposer) composeNextGroupAndSave() error {
	if err := r.fileWriter.Flush(); err != nil {
		return fmt.Errorf("syncing repair tree file %s failure: %w", r.appendFile, err)
	}
	if _, err := r.file.Seek(r.lastComposeOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to start of repair tree file %s failure: %w", r.appendFile, err)
	}
	r.fileReader.Reset(r.file)

	builder := newRepairTreeBuilder(r.slotCount)
	var readErr error
	var group string
	defer func() {
		// update the last compose offset to the current position in the file
		r.lastComposeOffset, _ = r.file.Seek(0, io.SeekCurrent)
	}()
	for readErr == nil || errors.Is(readErr, io.EOF) {
		var currGroup, id, shaVal string
		var strLen int32
		if currGroup, strLen, readErr = r.readString(); readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break // end of file reached
			}
			return fmt.Errorf("reading group from repair tree file %s failure: %w", r.appendFile, readErr)
		}
		if group == "" {
			group = currGroup
		} else if group != currGroup {
			// if the group have changed, we need to return the current group and builder
			_, err := r.file.Seek(-int64(strLen), io.SeekCurrent) // move back to the start of the next group
			if err != nil {
				return fmt.Errorf("seeking to start of next group in repair tree file %s failure: %w", r.appendFile, err)
			}
			break
		}
		if id, _, readErr = r.readString(); readErr != nil {
			return fmt.Errorf("reading id from repair tree file %s failure: %w", r.appendFile, readErr)
		}
		if shaVal, _, readErr = r.readString(); readErr != nil {
			return fmt.Errorf("reading sha value from repair tree file %s failure: %w", r.appendFile, readErr)
		}

		builder.append(id, shaVal)
	}
	_, saveErr := r.saveTree(group, builder)
	if saveErr != nil {
		return fmt.Errorf("saving repair tree for group %s failure: %w", group, saveErr)
	}
	return nil
}

func (r *repairTreeComposer) saveTree(group string, builder *repairTreeBuilder) (*repairTree, error) {
	if group == "" {
		return nil, fmt.Errorf("group cannot be empty when saving repair tree")
	}
	treeFile := fmt.Sprintf(r.treeFileFmt, group)
	if err := os.MkdirAll(filepath.Dir(treeFile), storage.FilePerm); err != nil {
		return nil, fmt.Errorf("creating directory for repair tree file %s failure: %w", treeFile, err)
	}
	tree, err := builder.build()
	if err != nil {
		return nil, fmt.Errorf("building repair tree for group %s failure: %w", group, err)
	}
	treeJSON, err := json.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("marshaling repair tree for group %s failure: %w", group, err)
	}
	return tree, os.WriteFile(treeFile, treeJSON, storage.FilePerm)
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

type repairMetrics struct {
	totalBuildTreeFinished meter.Counter
	totalBuildTreeFailures meter.Counter
	totalBuildTreeDuration meter.Counter
}

func newRepairMetrics(fac *observability.Factory) *repairMetrics {
	return &repairMetrics{
		totalBuildTreeFinished: fac.NewCounter("property_repair_build_tree_finished"),
		totalBuildTreeFailures: fac.NewCounter("property_repair_build_tree_failures"),
		totalBuildTreeDuration: fac.NewCounter("property_repair_build_tree_duration"),
	}
}
