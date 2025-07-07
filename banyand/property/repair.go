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
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
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
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const repairBatchSearchSize = 100

type repair struct {
	latestBuildTreeSchedule   time.Time
	buildTreeClock            clock.Clock
	closer                    *run.Closer
	repairTreeScheduler       *timestamp.Scheduler
	metrics                   *repairMetrics
	l                         *logger.Logger
	quickRepairNotified       *int32
	takeSnapshot              func(string) error
	shardPath                 string
	repairBasePath            string
	snapshotDir               string
	statePath                 string
	composeSlotAppendFilePath string
	composeTreeFilePathFmt    string
	treeSlotCount             int
	batchSearchSize           int
	buildTreeScheduleInterval time.Duration
	repairQuickBuildTreeTime  time.Duration
	buildTreeLocker           sync.Mutex
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
	var quickRepairNotified int32
	r = &repair{
		shardPath:                 shardPath,
		l:                         l,
		repairBasePath:            repairBase,
		snapshotDir:               path.Join(repairBase, "snapshot"),
		statePath:                 path.Join(repairBase, "state.json"),
		composeSlotAppendFilePath: path.Join(repairBase, "state-append-%d.tmp"),
		composeTreeFilePathFmt:    path.Join(repairBase, "state-tree-%s.data"),
		treeSlotCount:             treeSlotCount,
		batchSearchSize:           batchSearchSize,
		takeSnapshot:              takeSnapshot,
		repairQuickBuildTreeTime:  repairQuickBuildTreeTime,
		quickRepairNotified:       &quickRepairNotified,
		metrics:                   newRepairMetrics(metricsFactory),
		closer:                    run.NewCloser(1),
	}
	if err = r.initScheduler(repairBuildTreeCron); err != nil {
		return nil, fmt.Errorf("init scheduler: %w", err)
	}
	return r, nil
}

func (r *repair) initScheduler(exp string) error {
	r.buildTreeClock = clock.New()
	c := timestamp.NewScheduler(r.l, r.buildTreeClock)
	r.repairTreeScheduler = c
	err := c.Register("repair", cron.Descriptor, exp, func(t time.Time, _ *logger.Logger) bool {
		r.doRepairScheduler(t, true)
		return true
	})
	if err != nil {
		return fmt.Errorf("failed to add repair build tree cron task: %w", err)
	}
	interval, nextTime, exist := c.Interval("repair")
	if !exist {
		return fmt.Errorf("failed to get repair build tree cron task interval")
	}
	r.buildTreeScheduleInterval = interval
	r.latestBuildTreeSchedule = nextTime.Add(-interval)
	return nil
}

func (r *repair) documentUpdatesNotify() {
	if !atomic.CompareAndSwapInt32(r.quickRepairNotified, 0, 1) {
		return
	}

	go func() {
		select {
		case <-r.closer.CloseNotify():
			return
		case <-time.After(r.repairQuickBuildTreeTime):
			r.doRepairScheduler(r.buildTreeClock.Now(), false)
			// reset the notified flag to allow the next notification
			atomic.StoreInt32(r.quickRepairNotified, 0)
		}
	}()
}

func (r *repair) doRepairScheduler(t time.Time, triggerByCron bool) {
	if !triggerByCron {
		// if not triggered by cron, we need to check if the time is after the (last scheduled time + half of the interval)
		if r.buildTreeClock.Now().After(r.latestBuildTreeSchedule.Add(r.buildTreeScheduleInterval / 2)) {
			return
		}
	} else {
		r.latestBuildTreeSchedule = t
	}

	// if already building the tree, skip this run
	if !r.buildTreeLocker.TryLock() {
		return
	}
	defer r.buildTreeLocker.Unlock()
	err := r.buildStatus()
	if err != nil {
		r.l.Err(fmt.Errorf("repair build status failure: %w", err))
	}
}

func (r *repair) buildStatus() (err error) {
	startTime := time.Now()
	defer func() {
		r.metrics.totalBuildTreeFinished.Inc(1)
		if err != nil {
			r.metrics.totalBuildTreeFailures.Inc(1)
		}
		r.metrics.totalBuildTreeDuration.Inc(time.Since(startTime).Seconds())
	}()
	var state *repairStatus
	// reading the state file to check they have any updates
	state, err = r.readState()
	if err != nil {
		return fmt.Errorf("reading state failure: %w", err)
	}
	indexConfig := index.DefaultConfig(r.shardPath)
	items, err := indexConfig.DirectoryFunc().List(index.ItemKindSegment)
	if err != nil {
		return fmt.Errorf("reading item kind segment failure: %w", err)
	}
	sort.Sort(snapshotIDList(items))
	// check the snapshot ID have any updated
	// if no updates, the building Trees should be skipped
	if state != nil && len(items) != 0 && items[len(items)-1] == state.LastSnpID {
		return nil
	}
	if len(items) == 0 {
		return nil
	}

	// otherwise, we need to building the trees
	// take a new snapshot first
	err = r.takeSnapshot(r.snapshotDir)
	if err != nil {
		return fmt.Errorf("taking snapshot failure: %w", err)
	}
	blugeConf := bluge.DefaultConfig(r.snapshotDir)
	err = r.buildTree(blugeConf)
	if err != nil {
		return fmt.Errorf("building trees failure: %w", err)
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
		return fmt.Errorf("marshall state failure: %w", err)
	}
	err = os.WriteFile(r.statePath, stateVal, storage.FilePerm)
	if err != nil {
		return fmt.Errorf("writing state file failure: %w", err)
	}
	return nil
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
	treeComposer := newRepairTreeComposer(r.composeSlotAppendFilePath, r.composeTreeFilePathFmt, r.treeSlotCount, r.l)
	if err != nil {
		return fmt.Errorf("creating repair tree composer failure: %w", err)
	}
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
				if err = treeComposer.append(latestProperty.entityID, latestProperty.shaValue); err != nil {
					return fmt.Errorf("appending property to tree composer failure: %w", err)
				}
				err = treeComposer.composeNextGroupAndSave(latestProperty.group)
				if err != nil {
					return fmt.Errorf("composing group failure: %w", err)
				}
			} else if latestProperty.entityID != entity {
				// the entity is changed, need to save the property
				if err = treeComposer.append(latestProperty.entityID, latestProperty.shaValue); err != nil {
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
		if err = treeComposer.append(latestProperty.entityID, latestProperty.shaValue); err != nil {
			return fmt.Errorf("appending latest property to tree composer failure: %w", err)
		}
		err = treeComposer.composeNextGroupAndSave(latestProperty.group)
		if err != nil {
			return fmt.Errorf("composing last group failure: %w", err)
		}
	}

	return nil
}

//nolint:contextcheck
func (r *repair) pageSearch(reader *bluge.Reader, searcher *bluge.TopNSearch, each func(source []byte, shaValue string, deleteTime int64) error) error {
	var latestDocValues [][]byte
	for {
		searcher.After(latestDocValues)
		result, err := reader.Search(r.closer.Ctx(), searcher)
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

func (r *repair) close() {
	r.closer.Done()
	r.closer.CloseThenWait()
	r.repairTreeScheduler.Close()
}

type repairStatus struct {
	LastSyncTime time.Time `json:"last_sync_time"`
	LastSnpID    uint64    `json:"last_snp_id"`
}

type repairTreeNodeType int

const (
	repairTreeNodeTypeRoot repairTreeNodeType = iota
	repairTreeNodeTypeSlot
	repairTreeNodeTypeLeaf
)

type repairTreeNode struct {
	shaValue  string
	id        string
	tp        repairTreeNodeType
	leafStart int64
	leafLen   int64
}
type repairTreeReader struct {
	file   *os.File
	reader *bufio.Reader
	footer *repairTreeFooter
}

func (r *repair) treeReader(group string) (*repairTreeReader, error) {
	groupFile := fmt.Sprintf(r.composeTreeFilePathFmt, group)
	file, err := os.OpenFile(groupFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// if the file does not exist, it means no repair tree for this group
			return nil, nil
		}
		return nil, fmt.Errorf("opening repair tree file %s failure: %w", group, err)
	}
	reader := &repairTreeReader{
		file:   file,
		reader: bufio.NewReader(file),
	}
	if err = reader.readFoot(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("reading footer from repair tree file %s failure: %w", groupFile, err)
	}
	return reader, nil
}

func (r *repairTreeReader) readFoot() error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("getting file stat for %s failure: %w", r.file.Name(), err)
	}
	footerOffset := stat.Size() - 1
	if _, err = r.file.Seek(footerOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to footer offset %d in file %s failure: %w", footerOffset, r.file.Name(), err)
	}
	var footLen uint8
	if err = binary.Read(r.file, binary.LittleEndian, &footLen); err != nil {
		return fmt.Errorf("reading footer length from file %s failure: %w", r.file.Name(), err)
	}
	_, err = r.file.Seek(-(int64(footLen) + 1), io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("seeking to start of footer in file %s failure: %w", r.file.Name(), err)
	}
	footerBytes := make([]byte, footLen)
	if _, err = io.ReadFull(r.file, footerBytes); err != nil {
		return fmt.Errorf("reading footer from file %s failure: %w", r.file.Name(), err)
	}
	footerData := make([]int64, 4)
	_, err = encoding.BytesToVarInt64List(footerData, footerBytes)
	if err != nil {
		return fmt.Errorf("decoding footer from file %s failure: %w", r.file.Name(), err)
	}
	r.footer = &repairTreeFooter{
		leafNodeFinishedOffset: footerData[0],
		slotNodeLen:            footerData[1],
		slotNodeFinishedOffset: footerData[2],
		rootNodeLen:            footerData[3],
	}
	return nil
}

func (r *repairTreeReader) seekPosition(offset int64, whence int) error {
	_, err := r.file.Seek(offset, whence)
	if err != nil {
		return fmt.Errorf("seeking position failure: %w", err)
	}
	r.reader.Reset(r.file)
	return nil
}

func (r *repairTreeReader) read(parent *repairTreeNode) ([]*repairTreeNode, error) {
	if parent == nil {
		// reading the root node
		err := r.seekPosition(r.footer.slotNodeFinishedOffset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seeking to root node offset %d in file %s failure: %w", r.footer.slotNodeFinishedOffset, r.file.Name(), err)
		}
		rootDataBytes := make([]byte, r.footer.rootNodeLen)
		if _, err = io.ReadFull(r.reader, rootDataBytes); err != nil {
			return nil, fmt.Errorf("reading root node data from file %s failure: %w", r.file.Name(), err)
		}
		_, shaValue, err := encoding.DecodeBytes(rootDataBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding root node sha value from file %s failure: %w", r.file.Name(), err)
		}
		return []*repairTreeNode{
			{
				shaValue: fmt.Sprintf("%x", shaValue),
				tp:       repairTreeNodeTypeRoot,
			},
		}, nil
	}

	var err error
	nodes := make([]*repairTreeNode, 0)
	if parent.tp == repairTreeNodeTypeRoot {
		// reading the slot nodes
		if err = r.seekPosition(r.footer.leafNodeFinishedOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking to slot node offset %d in file %s failure: %w", r.footer.leafNodeFinishedOffset, r.file.Name(), err)
		}
		slotDataBytes := make([]byte, r.footer.slotNodeLen)
		if _, err = io.ReadFull(r.reader, slotDataBytes); err != nil {
			return nil, fmt.Errorf("reading slot node data from file %s failure: %w", r.file.Name(), err)
		}
		reduceBytesLen := int64(len(slotDataBytes))

		var slotNodeIndex, leafStartOff, leafLen int64
		var slotShaVal []byte
		for reduceBytesLen > 0 {
			slotDataBytes, slotNodeIndex, err = encoding.BytesToVarInt64(slotDataBytes)
			if err != nil {
				return nil, fmt.Errorf("decoding slot node index from file %s failure: %w", r.file.Name(), err)
			}
			slotDataBytes, slotShaVal, err = encoding.DecodeBytes(slotDataBytes)
			if err != nil {
				return nil, fmt.Errorf("decoding slot node sha value from file %s failure: %w", r.file.Name(), err)
			}
			slotDataBytes, leafStartOff, err = encoding.BytesToVarInt64(slotDataBytes)
			if err != nil {
				return nil, fmt.Errorf("decoding slot node leaf start offset from file %s failure: %w", r.file.Name(), err)
			}
			slotDataBytes, leafLen, err = encoding.BytesToVarInt64(slotDataBytes)
			if err != nil {
				return nil, fmt.Errorf("decoding slot node leaf length from file %s failure: %w", r.file.Name(), err)
			}
			reduceBytesLen = int64(len(slotDataBytes))
			nodes = append(nodes, &repairTreeNode{
				shaValue:  fmt.Sprintf("%x", slotShaVal),
				id:        fmt.Sprintf("%d", slotNodeIndex),
				tp:        repairTreeNodeTypeSlot,
				leafStart: leafStartOff,
				leafLen:   leafLen,
			})
		}
		return nodes, nil
	} else if parent.tp == repairTreeNodeTypeLeaf {
		return nil, nil
	}

	// otherwise, reading the leaf nodes
	err = r.seekPosition(parent.leafStart, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to leaf node offset %d in file %s failure: %w", r.footer.leafNodeFinishedOffset, r.file.Name(), err)
	}
	leafDataBytes := make([]byte, parent.leafLen)
	if _, err = io.ReadFull(r.reader, leafDataBytes); err != nil {
		return nil, fmt.Errorf("reading leaf node data from file %s failure: %w", r.file.Name(), err)
	}
	reduceBytesLen := int64(len(leafDataBytes))
	for reduceBytesLen > 0 {
		var entity, shaVal []byte
		leafDataBytes, entity, err = encoding.DecodeBytes(leafDataBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding leaf node entity from file %s failure: %w", r.file.Name(), err)
		}
		leafDataBytes, shaVal, err = encoding.DecodeBytes(leafDataBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding leaf node sha value from file %s failure: %w", r.file.Name(), err)
		}
		reduceBytesLen = int64(len(leafDataBytes))
		nodes = append(nodes, &repairTreeNode{
			shaValue: string(shaVal),
			id:       string(entity),
			tp:       repairTreeNodeTypeLeaf,
		})
	}
	return nodes, nil
}

type repairTreeFooter struct {
	leafNodeFinishedOffset int64
	slotNodeLen            int64
	slotNodeFinishedOffset int64
	rootNodeLen            int64
}

type snapshotIDList []uint64

func (s snapshotIDList) Len() int           { return len(s) }
func (s snapshotIDList) Less(i, j int) bool { return s[i] < s[j] }
func (s snapshotIDList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type repairTreeComposer struct {
	l             *logger.Logger
	appendFileFmt string
	treeFileFmt   string
	slotFiles     []*repairSlotFile
	slotCount     int
}

func newRepairTreeComposer(appendSlotFilePathFmt, treeFilePathFmt string, slotCount int, l *logger.Logger) *repairTreeComposer {
	return &repairTreeComposer{
		appendFileFmt: appendSlotFilePathFmt,
		treeFileFmt:   treeFilePathFmt,
		slotCount:     slotCount,
		slotFiles:     make([]*repairSlotFile, slotCount),
		l:             l,
	}
}

func (r *repairTreeComposer) append(id, shaVal string) (err error) {
	idBytes := []byte(id)
	shaValBytes := []byte(shaVal)
	val := xxhash.Sum64(idBytes)
	slotIndex := val % uint64(r.slotCount)
	file := r.slotFiles[slotIndex]
	if file == nil {
		file, err = newRepairSlotFile(int(slotIndex), r.appendFileFmt, r.l)
		if err != nil {
			return fmt.Errorf("creating repair slot file for slot %d failure: %w", slotIndex, err)
		}
		r.slotFiles[slotIndex] = file
	}
	return file.append(idBytes, shaValBytes)
}

// composeNextGroupAndSave composes the current group of slot files into a repair tree file.
// tree file format: [leaf nodes]+[slot nodes]+[root node]+[metadata]
// leaf nodes: each node contains: [entity]+[sha value]
// slot nodes: each node contains: [slot index]+[sha value]+[leaf nodes start offset]+[leaf nodes data length(binary encoded)]
// root node: contains [sha value]
// metadata: contains footer([slot nodes start offset]+[slot nodes length(data binary)]+[root node start offset]+[root node length])+[footer length(data binary)]
func (r *repairTreeComposer) composeNextGroupAndSave(group string) (err error) {
	treeFilePath := fmt.Sprintf(r.treeFileFmt, group)
	treeBuilder, err := newRepairTreeBuilder(treeFilePath)
	if err != nil {
		return fmt.Errorf("creating repair tree builder for group %s failure: %w", group, err)
	}
	defer func() {
		if closeErr := treeBuilder.close(); closeErr != nil {
			err = multierr.Append(err, fmt.Errorf("closing repair tree builder for group %s failure: %w", group, closeErr))
		}
	}()
	for _, f := range r.slotFiles {
		if f == nil {
			continue
		}
		if err = treeBuilder.appendSlot(f); err != nil {
			return fmt.Errorf("appending slot file %s to repair tree builder for group %s failure: %w", f.path, group, err)
		}
	}
	// reset the slot files for the next group
	r.slotFiles = make([]*repairSlotFile, r.slotCount)
	return treeBuilder.build()
}

type repairSlotFile struct {
	sha    hash.Hash
	file   *os.File
	writer *bufio.Writer
	l      *logger.Logger
	path   string
	slot   int
}

func newRepairSlotFile(slot int, pathFmt string, l *logger.Logger) (*repairSlotFile, error) {
	filePath := fmt.Sprintf(pathFmt, slot)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("opening repair slot file %s failure: %w", pathFmt, err)
	}
	return &repairSlotFile{
		path:   filePath,
		slot:   slot,
		file:   file,
		sha:    sha512.New(),
		writer: bufio.NewWriter(file),
		l:      l,
	}, nil
}

func (r *repairSlotFile) append(entity, shaValue []byte) error {
	_, err := r.sha.Write(shaValue)
	if err != nil {
		return fmt.Errorf("writing sha value to repair slot file %s failure: %w", r.path, err)
	}
	result := make([]byte, 0)
	result = encoding.EncodeBytes(result, entity)
	result = encoding.EncodeBytes(result, shaValue)
	_, err = r.writer.Write(result)
	if err != nil {
		return fmt.Errorf("writing entity and sha value to repair slot file %s failure: %w", r.path, err)
	}
	return nil
}

func (r *repairSlotFile) writeToAndDeleteSlotFile(f io.Writer) (int64, error) {
	defer func() {
		if closeErr := r.file.Close(); closeErr != nil {
			r.l.Warn().Str("file", r.path).Err(closeErr).Msg("closing repair slot file failure")
		}
		if removeErr := os.Remove(r.path); removeErr != nil {
			r.l.Warn().Str("file", r.path).Err(removeErr).Msg("removing repair slot file failure")
		}
	}()
	var err error
	if err = r.writer.Flush(); err != nil {
		return 0, fmt.Errorf("flushing repair slot file %s failure: %w", r.path, err)
	}
	if _, err = r.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seeking to start of repair slot file %s failure: %w", r.path, err)
	}
	return io.Copy(f, r.file)
}

type repairTreeBuilder struct {
	rootHash   hash.Hash
	groupFile  *os.File
	writer     *bufio.Writer
	Slots      []*repairTreeBuilderSlot
	currentOff int64
}

func newRepairTreeBuilder(groupFilePath string) (*repairTreeBuilder, error) {
	groupFile, err := os.OpenFile(groupFilePath, os.O_WRONLY|os.O_CREATE, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("opening repair tree file %s failure: %w", groupFilePath, err)
	}
	return &repairTreeBuilder{
		Slots:     make([]*repairTreeBuilderSlot, 0),
		rootHash:  sha512.New(),
		groupFile: groupFile,
		writer:    bufio.NewWriter(groupFile),
	}, nil
}

func (r *repairTreeBuilder) appendSlot(slot *repairSlotFile) (err error) {
	slotSha := slot.sha.Sum(nil)
	_, err = r.rootHash.Write(slotSha)
	if err != nil {
		return fmt.Errorf("hashing root sha value failure: %w", err)
	}
	writeLen, err := slot.writeToAndDeleteSlotFile(r.writer)
	if err != nil {
		return fmt.Errorf("writing slot file %s to repair tree file failure: %w", slot.path, err)
	}
	r.Slots = append(r.Slots, &repairTreeBuilderSlot{
		shaVal:         slotSha,
		repairSlotFile: slot,
		startOff:       r.currentOff,
		len:            writeLen,
	})
	r.currentOff += writeLen
	return nil
}

func (r *repairTreeBuilder) build() (err error) {
	leafNodesFinishedOffset := r.currentOff
	slotNodesFinishedOffset := r.currentOff
	var slotNodesLen int64
	// write the slot nodes
	var writedLen int
	for _, slot := range r.Slots {
		data := make([]byte, 0)
		data = encoding.VarInt64ToBytes(data, int64(slot.slot))
		data = encoding.EncodeBytes(data, slot.shaVal)
		data = encoding.VarInt64ToBytes(data, slot.startOff)
		data = encoding.VarInt64ToBytes(data, slot.len)
		writedLen, err = r.writer.Write(data)
		if err != nil {
			return fmt.Errorf("writing slot node to repair tree file failure: %w", err)
		}
		slotNodesLen += int64(writedLen)
		slotNodesFinishedOffset += int64(writedLen)
	}

	// write the root node
	data := make([]byte, 0)
	rootShaVal := r.rootHash.Sum(nil)
	data = encoding.EncodeBytes(data, rootShaVal)
	writedLen, err = r.writer.Write(data)
	if err != nil {
		return fmt.Errorf("writing root node to repair tree file failure: %w", err)
	}

	// write footer
	data = make([]byte, 0)
	data = encoding.VarInt64ToBytes(data, leafNodesFinishedOffset) // end offset of the slot nodes
	data = encoding.VarInt64ToBytes(data, slotNodesLen)            // length of the slot nodes
	data = encoding.VarInt64ToBytes(data, slotNodesFinishedOffset) // end offset of the root node
	data = encoding.VarInt64ToBytes(data, int64(writedLen))        // length of the root node
	footerLen := uint8(len(data))

	_, err = r.writer.Write(data)
	if err != nil {
		return fmt.Errorf("writing footer to repair tree file failure: %w", err)
	}
	if err = binary.Write(r.writer, binary.LittleEndian, footerLen); err != nil {
		return fmt.Errorf("writing footer length to repair tree file failure: %w", err)
	}
	return nil
}

type repairTreeBuilderSlot struct {
	*repairSlotFile
	shaVal   []byte
	startOff int64
	len      int64
}

func (r *repairTreeBuilder) close() (err error) {
	if flushErr := r.writer.Flush(); flushErr != nil {
		err = multierr.Append(err, fmt.Errorf("flushing repair tree file failure: %w", flushErr))
	}
	if closeErr := r.groupFile.Close(); closeErr != nil {
		err = multierr.Append(err, fmt.Errorf("closing repair tree file failure: %w", closeErr))
	}
	return err
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
