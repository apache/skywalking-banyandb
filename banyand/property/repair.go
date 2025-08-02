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
	"hash"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	grpclib "google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	repairBatchSearchSize = 100
	repairFileNewLine     = '\n'
)

type repair struct {
	metrics                   *repairMetrics
	l                         *logger.Logger
	scheduler                 *repairScheduler
	shardPath                 string
	repairBasePath            string
	snapshotDir               string
	statePath                 string
	composeSlotAppendFilePath string
	composeTreeFilePathFmt    string
	treeSlotCount             int
	batchSearchSize           int
}

func newRepair(
	shardPath string,
	repairPath string,
	l *logger.Logger,
	metricsFactory *observability.Factory,
	batchSearchSize int,
	treeSlotCount int,
	scheduler *repairScheduler,
) *repair {
	return &repair{
		shardPath:                 shardPath,
		l:                         l,
		repairBasePath:            repairPath,
		snapshotDir:               path.Join(repairPath, "snapshot"),
		statePath:                 path.Join(repairPath, "state.json"),
		composeSlotAppendFilePath: path.Join(repairPath, "state-append-%d.tmp"),
		composeTreeFilePathFmt:    path.Join(repairPath, "state-tree-%s.data"),
		batchSearchSize:           batchSearchSize,
		metrics:                   newRepairMetrics(metricsFactory),
		scheduler:                 scheduler,
		treeSlotCount:             treeSlotCount,
	}
}

func (r *repair) checkHasUpdates() (bool, error) {
	state, err := r.readState()
	if err != nil {
		return false, fmt.Errorf("reading state failure: %w", err)
	}
	indexConfig := index.DefaultConfig(r.shardPath)
	items, err := indexConfig.DirectoryFunc().List(index.ItemKindSnapshot)
	if err != nil {
		return false, fmt.Errorf("reading item kind snapshot failure: %w", err)
	}
	sort.Sort(snapshotIDList(items))
	// check the snapshot ID have any updated
	// if no updates, the building Trees should be skipped
	if state != nil && len(items) != 0 && items[len(items)-1] == state.LastSnpID {
		return false, nil
	}
	if len(items) == 0 {
		return false, nil
	}
	return true, nil
}

func (r *repair) buildStatus(ctx context.Context, snapshotPath string, group string) (err error) {
	startTime := time.Now()
	defer func() {
		r.metrics.totalBuildTreeFinished.Inc(1)
		if err != nil {
			r.metrics.totalBuildTreeFailures.Inc(1)
		}
		r.metrics.totalBuildTreeDuration.Inc(time.Since(startTime).Seconds())
	}()
	indexConfig := index.DefaultConfig(snapshotPath)
	items, err := indexConfig.DirectoryFunc().List(index.ItemKindSnapshot)
	if err != nil {
		return fmt.Errorf("reading item kind segment failure: %w", err)
	}
	sort.Sort(snapshotIDList(items))

	blugeConf := bluge.DefaultConfig(snapshotPath)
	err = r.buildTree(ctx, blugeConf, group)
	if err != nil {
		return fmt.Errorf("building trees failure: %w", err)
	}
	// if only update a specific group, the repair base status file doesn't need to update
	// because not all the group have been processed
	if group != "" {
		return nil
	}

	var latestSnapshotID uint64
	if len(items) > 0 {
		latestSnapshotID = items[len(items)-1]
	}
	// save the Trees to the state
	state := &repairStatus{
		LastSnpID:    latestSnapshotID,
		LastSyncTime: time.Now(),
	}
	stateVal, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshall state failure: %w", err)
	}
	if err = os.MkdirAll(filepath.Dir(r.statePath), storage.DirPerm); err != nil {
		return fmt.Errorf("creating state directory failure: %w", err)
	}
	err = os.WriteFile(r.statePath, stateVal, storage.FilePerm)
	if err != nil {
		return fmt.Errorf("writing state file failure: %w", err)
	}
	return nil
}

func (r *repair) buildTree(ctx context.Context, conf bluge.Config, group string) error {
	reader, err := bluge.OpenReader(conf)
	if err != nil {
		// means no data found
		if strings.Contains(err.Error(), "unable to find a usable snapshot") {
			return nil
		}
		return fmt.Errorf("opening index reader failure: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()
	query := bluge.Query(bluge.NewMatchAllQuery())
	if group != "" {
		query = bluge.NewTermQuery(group).SetField(groupField)
	}
	topNSearch := bluge.NewTopNSearch(r.batchSearchSize, query)
	topNSearch.SortBy([]string{
		fmt.Sprintf("+%s", groupField),
		fmt.Sprintf("+%s", nameField),
		fmt.Sprintf("+%s", entityID),
		fmt.Sprintf("+%s", timestampField),
	})

	var latestProperty *searchingProperty
	treeComposer := newRepairTreeComposer(r.composeSlotAppendFilePath, r.composeTreeFilePathFmt, r.treeSlotCount, r.l)
	err = r.pageSearch(ctx, reader, topNSearch, func(sortValue [][]byte, shaValue string) error {
		if len(sortValue) != 4 {
			return fmt.Errorf("unexpected sort value length: %d", len(sortValue))
		}
		group := convert.BytesToString(sortValue[0])
		name := convert.BytesToString(sortValue[1])
		entity := r.buildLeafNodeEntity(group, name, convert.BytesToString(sortValue[2]))

		s := newSearchingProperty(group, shaValue, entity)
		if latestProperty != nil {
			if latestProperty.group != group {
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

func (r *repair) buildLeafNodeEntity(group, name, entityID string) string {
	return fmt.Sprintf("%s/%s/%s", group, name, entityID)
}

func (r *repair) parseLeafNodeEntity(entity string) (string, string, string, error) {
	parts := strings.SplitN(entity, "/", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid leaf node entity format: %s", entity)
	}
	return parts[0], parts[1], parts[2], nil
}

func (r *repair) pageSearch(
	ctx context.Context,
	reader *bluge.Reader,
	searcher *bluge.TopNSearch,
	each func(sortValue [][]byte, shaValue string) error,
) error {
	var latestDocValues [][]byte
	for {
		searcher.After(latestDocValues)
		result, err := reader.Search(ctx, searcher)
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
		for err == nil && next != nil {
			hitNumber = next.HitNumber
			var errTime error
			err = next.VisitStoredFields(func(field string, value []byte) bool {
				if field == shaValueField {
					shaValue = convert.BytesToString(value)
					return false
				}
				return true
			})
			if err = multierr.Combine(err, errTime); err != nil {
				return errors.WithMessagef(err, "visit stored fields, hit: %d", hitNumber)
			}
			err = each(next.SortValue, shaValue)
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
	entity    string
	tp        repairTreeNodeType
	leafStart int64
	leafCount int64
	slotInx   int32
}

type repairTreeReader interface {
	read(parent *repairTreeNode, pagingSize int64, forceReadFromStart bool) ([]*repairTreeNode, error)
	close() error
}

type repairTreeFileReader struct {
	file   *os.File
	reader *bufio.Reader
	footer *repairTreeFooter
	paging *repairTreeReaderPage
}

func (r *repair) treeReader(group string) (repairTreeReader, error) {
	r.scheduler.treeLocker.RLock()
	defer r.scheduler.treeLocker.RUnlock()
	groupFile := fmt.Sprintf(r.composeTreeFilePathFmt, group)
	file, err := os.OpenFile(groupFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// if the file does not exist, it means no repair tree for this group
			return nil, nil
		}
		return nil, fmt.Errorf("opening repair tree file %s failure: %w", group, err)
	}
	reader := &repairTreeFileReader{
		file:   file,
		reader: bufio.NewReader(file),
	}
	if err = reader.readFoot(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("reading footer from repair tree file %s failure: %w", groupFile, err)
	}
	return reader, nil
}

func (r *repair) stateFileExist() (bool, error) {
	_, err := os.Stat(r.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checking state file existence failure: %w", err)
	}
	return true, nil
}

func (r *repairTreeFileReader) readFoot() error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("getting file stat for %s failure: %w", r.file.Name(), err)
	}
	footerOffset := stat.Size() - 8
	if _, err = r.file.Seek(footerOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to footer offset %d in file %s failure: %w", footerOffset, r.file.Name(), err)
	}
	var footLen int64
	if err = binary.Read(r.file, binary.LittleEndian, &footLen); err != nil {
		return fmt.Errorf("reading footer length from file %s failure: %w", r.file.Name(), err)
	}
	_, err = r.file.Seek(-(footLen + 8), io.SeekCurrent)
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
		slotNodeCount:          footerData[1],
		slotNodeFinishedOffset: footerData[2],
		rootNodeLen:            footerData[3],
	}
	return nil
}

func (r *repairTreeFileReader) seekPosition(offset int64, whence int) error {
	_, err := r.file.Seek(offset, whence)
	if err != nil {
		return fmt.Errorf("seeking position failure: %w", err)
	}
	r.reader.Reset(r.file)
	return nil
}

func (r *repairTreeFileReader) read(parent *repairTreeNode, pagingSize int64, forceReFromStart bool) ([]*repairTreeNode, error) {
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
				shaValue: string(shaValue),
				tp:       repairTreeNodeTypeRoot,
			},
		}, nil
	}

	var err error
	if parent.tp == repairTreeNodeTypeRoot {
		needSeek := false
		if r.paging == nil || r.paging.lastNode != parent || forceReFromStart {
			needSeek = true
			r.paging = newRepairTreeReaderPage(parent, r.footer.slotNodeCount)
		}
		if needSeek {
			// reading the slot nodes
			if err = r.seekPosition(r.footer.leafNodeFinishedOffset, io.SeekStart); err != nil {
				return nil, fmt.Errorf("seeking to slot node offset %d in file %s failure: %w", r.footer.leafNodeFinishedOffset, r.file.Name(), err)
			}
		}
		var slotNodeIndex, leafStartOff, leafCount int64
		var slotShaVal, slotDataBytes []byte
		count := r.paging.nextPage(pagingSize)
		nodes := make([]*repairTreeNode, 0, count)
		for i := int64(0); i < count; i++ {
			slotDataBytes, err = r.reader.ReadBytes(repairFileNewLine)
			if err != nil {
				return nil, fmt.Errorf("reading slot node data from file %s failure: %w", r.file.Name(), err)
			}
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
			_, leafCount, err = encoding.BytesToVarInt64(slotDataBytes)
			if err != nil {
				return nil, fmt.Errorf("decoding slot node leaf length from file %s failure: %w", r.file.Name(), err)
			}
			nodes = append(nodes, &repairTreeNode{
				shaValue:  string(slotShaVal),
				slotInx:   int32(slotNodeIndex),
				tp:        repairTreeNodeTypeSlot,
				leafStart: leafStartOff,
				leafCount: leafCount,
			})
		}

		return nodes, nil
	} else if parent.tp == repairTreeNodeTypeLeaf {
		return nil, nil
	}

	// otherwise, reading the leaf nodes
	needSeek := false
	if r.paging == nil || r.paging.lastNode != parent || forceReFromStart {
		needSeek = true
		r.paging = newRepairTreeReaderPage(parent, parent.leafCount)
	}
	if needSeek {
		err = r.seekPosition(parent.leafStart, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seeking to leaf node offset %d in file %s failure: %w", r.footer.leafNodeFinishedOffset, r.file.Name(), err)
		}
	}
	var entity, shaVal []byte
	count := r.paging.nextPage(pagingSize)
	nodes := make([]*repairTreeNode, 0, count)
	for i := int64(0); i < count; i++ {
		leafDataBytes, err := r.reader.ReadBytes(repairFileNewLine)
		if err != nil {
			return nil, fmt.Errorf("reading leaf node data from file %s failure: %w", r.file.Name(), err)
		}
		leafDataBytes, entity, err = encoding.DecodeBytes(leafDataBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding leaf node entity from file %s failure: %w", r.file.Name(), err)
		}
		_, shaVal, err = encoding.DecodeBytes(leafDataBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding leaf node sha value from file %s failure: %w", r.file.Name(), err)
		}
		nodes = append(nodes, &repairTreeNode{
			slotInx:  parent.slotInx,
			shaValue: string(shaVal),
			entity:   string(entity),
			tp:       repairTreeNodeTypeLeaf,
		})
	}
	return nodes, nil
}

func (r *repairTreeFileReader) close() error {
	return r.file.Close()
}

type repairTreeReaderPage struct {
	lastNode    *repairTreeNode
	reduceCount int64
}

func newRepairTreeReaderPage(parent *repairTreeNode, totalChildCount int64) *repairTreeReaderPage {
	return &repairTreeReaderPage{
		lastNode:    parent,
		reduceCount: totalChildCount,
	}
}

func (r *repairTreeReaderPage) nextPage(count int64) int64 {
	if r.reduceCount == 0 {
		return 0
	}
	readCount := r.reduceCount - count
	if readCount <= 0 {
		readCount = r.reduceCount
	}
	r.reduceCount -= readCount
	return readCount
}

type repairBufferLeafReader struct {
	reader      repairTreeReader
	currentSlot *repairTreeNode
	pagingLeafs []*repairTreeNode
}

func newRepairBufferLeafReader(reader repairTreeReader) *repairBufferLeafReader {
	return &repairBufferLeafReader{
		reader: reader,
	}
}

func (r *repairBufferLeafReader) next(slot *repairTreeNode) (*repairTreeNode, error) {
	// slot is nil means reset the reader
	if slot == nil {
		r.currentSlot = nil
		return nil, nil
	}
	if r.currentSlot == nil || r.currentSlot.slotInx != slot.slotInx {
		// if the current slot is nil or the slot index is changed, we need to read the leaf nodes from the slot
		err := r.readNodes(slot, true)
		if err != nil {
			return nil, err
		}
	}
	// if no more leaf nodes, trying to read slots
	if len(r.pagingLeafs) == 0 {
		err := r.readNodes(slot, false)
		if err != nil {
			return nil, err
		}
		// no more leaf nodes to read, return nil
		if len(r.pagingLeafs) == 0 {
			return nil, nil
		}
	}
	// pop the first leaf node from the paging leafs
	leaf := r.pagingLeafs[0]
	r.pagingLeafs = r.pagingLeafs[1:]
	return leaf, nil
}

func (r *repairBufferLeafReader) readNodes(slot *repairTreeNode, forceReadFromStart bool) error {
	nodes, err := r.reader.read(slot, repairBatchSearchSize, forceReadFromStart)
	if err != nil {
		return fmt.Errorf("reading leaf nodes from slot %d failure: %w", slot.slotInx, err)
	}
	r.pagingLeafs = nodes
	r.currentSlot = slot
	return nil
}

type repairTreeFooter struct {
	leafNodeFinishedOffset int64
	slotNodeCount          int64
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
// slot nodes: each node contains: [slot index]+[sha value]+[leaf nodes start offset]+[leaf nodes count]
// root node: contains [sha value]
// metadata: contains footer([slot nodes start offset]+[slot nodes count]+[root node start offset]+[root node length])+[footer length(data binary)]
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
	for i, f := range r.slotFiles {
		if f == nil {
			continue
		}
		if err = treeBuilder.appendSlot(f); err != nil {
			return fmt.Errorf("appending slot file %s to repair tree builder for group %s failure: %w", f.path, group, err)
		}
		r.slotFiles[i] = nil
	}
	return treeBuilder.build()
}

type repairSlotFile struct {
	sha    hash.Hash
	file   *os.File
	writer *bufio.Writer
	l      *logger.Logger
	path   string
	slot   int
	count  int64
}

func newRepairSlotFile(slot int, pathFmt string, l *logger.Logger) (*repairSlotFile, error) {
	filePath := fmt.Sprintf(pathFmt, slot)
	if err := os.MkdirAll(filepath.Dir(filePath), storage.DirPerm); err != nil {
		return nil, fmt.Errorf("creating directory for repair slot file %s failure: %w", filePath, err)
	}
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, storage.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("opening repair slot file %s failure: %w", filePath, err)
	}
	return &repairSlotFile{
		path:   filePath,
		slot:   slot,
		file:   file,
		sha:    sha512.New(),
		writer: bufio.NewWriter(file),
		l:      l,
		count:  0,
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
	result = append(result, repairFileNewLine)
	_, err = r.writer.Write(result)
	if err != nil {
		return fmt.Errorf("writing entity and sha value to repair slot file %s failure: %w", r.path, err)
	}
	r.count++
	return nil
}

func (r *repairSlotFile) writeToAndDeleteSlotFile(f io.Writer) (int64, int64, error) {
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
		return 0, 0, fmt.Errorf("flushing repair slot file %s failure: %w", r.path, err)
	}
	if _, err = r.file.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("seeking to start of repair slot file %s failure: %w", r.path, err)
	}
	writeBytes, err := io.Copy(f, r.file)
	if err != nil {
		return 0, 0, fmt.Errorf("copying repair slot file %s to writer failure: %w", r.path, err)
	}
	return writeBytes, r.count, nil
}

type repairTreeBuilder struct {
	rootHash   hash.Hash
	groupFile  *os.File
	writer     *bufio.Writer
	Slots      []*repairTreeBuilderSlot
	currentOff int64
}

func newRepairTreeBuilder(groupFilePath string) (*repairTreeBuilder, error) {
	if err := os.MkdirAll(filepath.Dir(groupFilePath), storage.DirPerm); err != nil {
		return nil, fmt.Errorf("creating directory for repair tree file %s failure: %w", groupFilePath, err)
	}
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
	shaValBytes := []byte(fmt.Sprintf("%x", slotSha))
	_, err = r.rootHash.Write(shaValBytes)
	if err != nil {
		return fmt.Errorf("hashing root sha value failure: %w", err)
	}
	bytesSize, leafCount, err := slot.writeToAndDeleteSlotFile(r.writer)
	if err != nil {
		return fmt.Errorf("writing slot file %s to repair tree file failure: %w", slot.path, err)
	}
	r.Slots = append(r.Slots, &repairTreeBuilderSlot{
		shaVal:         shaValBytes,
		repairSlotFile: slot,
		startOff:       r.currentOff,
		leafCount:      leafCount,
		writeLen:       bytesSize,
	})
	r.currentOff += bytesSize
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
		data = encoding.VarInt64ToBytes(data, slot.leafCount)
		data = append(data, repairFileNewLine)
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
	data = encoding.EncodeBytes(data, []byte(fmt.Sprintf("%x", rootShaVal)))
	writedLen, err = r.writer.Write(data)
	if err != nil {
		return fmt.Errorf("writing root node to repair tree file failure: %w", err)
	}

	// write footer
	data = make([]byte, 0)
	data = encoding.VarInt64ToBytes(data, leafNodesFinishedOffset) // end offset of the slot nodes
	data = encoding.VarInt64ToBytes(data, int64(len(r.Slots)))     // count of the slot nodes
	data = encoding.VarInt64ToBytes(data, slotNodesFinishedOffset) // end offset of the root node
	data = encoding.VarInt64ToBytes(data, int64(writedLen))        // length of the root node
	footerLen := int64(len(data))

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
	shaVal    []byte
	startOff  int64
	leafCount int64
	writeLen  int64
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

func newSearchingProperty(group, shaValue, entity string) *searchingProperty {
	return &searchingProperty{
		group:    group,
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

type repairScheduler struct {
	latestBuildTreeSchedule   time.Time
	buildTreeClock            clock.Clock
	gossipMessenger           gossip.Messenger
	closer                    *run.Closer
	buildSnapshotFunc         func(context.Context) (string, error)
	repairTreeScheduler       *timestamp.Scheduler
	quickRepairNotified       *int32
	db                        *database
	l                         *logger.Logger
	metrics                   *repairSchedulerMetrics
	treeSlotCount             int
	buildTreeScheduleInterval time.Duration
	quickBuildTreeTime        time.Duration
	lastBuildTimeLocker       sync.Mutex
	treeLocker                sync.RWMutex
}

// nolint: contextcheck
func newRepairScheduler(
	l *logger.Logger,
	omr observability.MetricsRegistry,
	buildTreeCronExp string,
	quickBuildTreeTime time.Duration,
	triggerCronExp string,
	gossipMessenger gossip.Messenger,
	treeSlotCount int,
	db *database,
	buildSnapshotFunc func(context.Context) (string, error),
) (*repairScheduler, error) {
	var quickRepairNotified int32
	s := &repairScheduler{
		l:                   l,
		buildTreeClock:      clock.New(),
		db:                  db,
		buildSnapshotFunc:   buildSnapshotFunc,
		quickRepairNotified: &quickRepairNotified,
		closer:              run.NewCloser(1),
		quickBuildTreeTime:  quickBuildTreeTime,
		metrics:             newRepairSchedulerMetrics(omr.With(propertyScope.SubScope("scheduler"))),
		gossipMessenger:     gossipMessenger,
		treeSlotCount:       treeSlotCount,
	}
	c := timestamp.NewScheduler(l, s.buildTreeClock)
	s.repairTreeScheduler = c
	err := c.Register("build-tree", cron.Descriptor, buildTreeCronExp, func(t time.Time, _ *logger.Logger) bool {
		s.doBuildTreeScheduler(t, true)
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add repair build tree cron task: %w", err)
	}
	err = c.Register("trigger", cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor,
		triggerCronExp, func(time.Time, *logger.Logger) bool {
			gossipErr := s.doRepairGossip(s.closer.Ctx())
			if gossipErr != nil {
				s.l.Err(fmt.Errorf("repair gossip failure: %w", gossipErr))
			}
			return true
		})
	if err != nil {
		return nil, fmt.Errorf("failed to add repair trigger cron task: %w", err)
	}
	err = s.initializeInterval()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (r *repairScheduler) doBuildTreeScheduler(t time.Time, triggerByCron bool) {
	if !r.verifyShouldExecuteBuildTree(t, triggerByCron) {
		return
	}

	err := r.doBuildTree()
	if err != nil {
		r.l.Err(fmt.Errorf("repair build status failure: %w", err))
	}
}

func (r *repairScheduler) verifyShouldExecuteBuildTree(t time.Time, triggerByCron bool) bool {
	r.lastBuildTimeLocker.Lock()
	defer r.lastBuildTimeLocker.Unlock()
	if !triggerByCron {
		// if not triggered by cron, we need to check if the time is after the (last scheduled time + half of the interval)
		if r.buildTreeClock.Now().After(r.latestBuildTreeSchedule.Add(r.buildTreeScheduleInterval / 2)) {
			return false
		}
	} else {
		r.latestBuildTreeSchedule = t
	}
	return true
}

func (r *repairScheduler) initializeInterval() error {
	r.lastBuildTimeLocker.Lock()
	defer r.lastBuildTimeLocker.Unlock()
	interval, nextTime, exist := r.repairTreeScheduler.Interval("build-tree")
	if !exist {
		return fmt.Errorf("failed to get repair build tree cron task interval")
	}
	r.buildTreeScheduleInterval = interval
	r.latestBuildTreeSchedule = nextTime.Add(-interval)
	return nil
}

//nolint:contextcheck
func (r *repairScheduler) doBuildTree() (err error) {
	now := time.Now()
	r.metrics.totalRepairBuildTreeStarted.Inc(1)
	defer func() {
		r.metrics.totalRepairBuildTreeFinished.Inc(1)
		r.metrics.totalRepairBuildTreeLatency.Inc(time.Since(now).Seconds())
		if err != nil {
			r.metrics.totalRepairBuildTreeFailures.Inc(1)
		}
	}()
	sLst := r.db.sLst.Load()
	if sLst == nil {
		return nil
	}
	hasUpdates := false
	for _, s := range *sLst {
		hasUpdates, err = s.repairState.checkHasUpdates()
		if err != nil {
			return err
		}
		if hasUpdates {
			break
		}
	}
	// if no updates, skip the repair
	if !hasUpdates {
		return nil
	}

	// otherwise, we need to build the trees
	return r.buildingTree(nil, "", false)
}

// nolint: contextcheck
func (r *repairScheduler) buildingTree(shards []common.ShardID, group string, force bool) error {
	if force {
		r.treeLocker.Lock()
	} else if !r.treeLocker.TryLock() {
		// if not forced, we try to lock the build tree locker
		r.metrics.totalRepairBuildTreeConflicts.Inc(1)
		return nil
	}
	defer r.treeLocker.Unlock()

	buildAll := len(shards) == 0

	// take a new snapshot first
	snapshotPath, err := r.buildSnapshotFunc(r.closer.Ctx())
	if err != nil {
		return fmt.Errorf("taking snapshot failure: %w", err)
	}
	return walkDir(snapshotPath, "shard-", func(suffix string) error {
		id, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		if !buildAll {
			// if not building all shards, check if the shard is in the list
			found := false
			for _, s := range shards {
				if s == common.ShardID(id) {
					found = true
					break
				}
			}
			if !found {
				return nil // skip this shard
			}
		}
		s, err := r.db.loadShard(r.closer.Ctx(), common.ShardID(id))
		if err != nil {
			return fmt.Errorf("loading shard %d failure: %w", id, err)
		}
		err = s.repairState.buildStatus(r.closer.Ctx(), path.Join(snapshotPath, fmt.Sprintf("shard-%s", suffix)), group)
		if err != nil {
			return fmt.Errorf("building status for shard %d failure: %w", id, err)
		}
		return nil
	})
}

func (r *repairScheduler) documentUpdatesNotify() {
	if !atomic.CompareAndSwapInt32(r.quickRepairNotified, 0, 1) {
		return
	}

	go func() {
		select {
		case <-r.closer.CloseNotify():
			return
		case <-time.After(r.quickBuildTreeTime):
			r.doBuildTreeScheduler(r.buildTreeClock.Now(), false)
			// reset the notified flag to allow the next notification
			atomic.StoreInt32(r.quickRepairNotified, 0)
		}
	}()
}

func (r *repairScheduler) close() {
	r.repairTreeScheduler.Close()
	r.closer.Done()
	r.closer.CloseThenWait()
}

func (r *repairScheduler) doRepairGossip(ctx context.Context) error {
	group, shardNum, err := r.randomSelectGroup(ctx)
	if err != nil {
		return fmt.Errorf("selecting random group failure: %w", err)
	}

	nodes, err := r.gossipMessenger.LocateNodes(group.Metadata.Name, shardNum, uint32(r.copiesCount(group)))
	if err != nil {
		return fmt.Errorf("locating nodes for group %s, shard %d failure: %w", group.Metadata.Name, shardNum, err)
	}
	return r.gossipMessenger.Propagation(nodes, group.Metadata.Name, shardNum)
}

func (r *repairScheduler) randomSelectGroup(ctx context.Context) (*commonv1.Group, uint32, error) {
	allGroups, err := r.db.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("listing groups failure: %w", err)
	}

	groups := make([]*commonv1.Group, 0)
	for _, group := range allGroups {
		if group.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
			continue
		}
		// if the group don't have copies, skip it
		if r.copiesCount(group) < 2 {
			continue
		}
		groups = append(groups, group)
	}

	if len(groups) == 0 {
		return nil, 0, fmt.Errorf("no groups found with enough copies for repair")
	}
	// #nosec G404 -- not security-critical, just for random selection
	group := groups[rand.Int64()%int64(len(groups))]
	// #nosec G404 -- not security-critical, just for random selection
	return group, rand.Uint32() % group.ResourceOpts.ShardNum, nil
}

func (r *repairScheduler) copiesCount(group *commonv1.Group) int {
	return int(group.ResourceOpts.Replicas + 1)
}

func (r *repairScheduler) registerServerToGossip() func(server *grpclib.Server) {
	s := newRepairGossipServer(r)
	return func(server *grpclib.Server) {
		propertyv1.RegisterRepairServiceServer(server, s)
	}
}

func (r *repairScheduler) registerClientToGossip(messenger gossip.Messenger) {
	messenger.Subscribe(newRepairGossipClient(r))
}

type repairSchedulerMetrics struct {
	totalRepairBuildTreeStarted   meter.Counter
	totalRepairBuildTreeFinished  meter.Counter
	totalRepairBuildTreeFailures  meter.Counter
	totalRepairBuildTreeLatency   meter.Counter
	totalRepairBuildTreeConflicts meter.Counter

	totalRepairSuccessCount meter.Counter
	totalRepairFailedCount  meter.Counter
}

func newRepairSchedulerMetrics(omr *observability.Factory) *repairSchedulerMetrics {
	return &repairSchedulerMetrics{
		totalRepairBuildTreeStarted:   omr.NewCounter("repair_build_tree_started"),
		totalRepairBuildTreeFinished:  omr.NewCounter("repair_build_tree_finished"),
		totalRepairBuildTreeFailures:  omr.NewCounter("repair_build_tree_failures"),
		totalRepairBuildTreeLatency:   omr.NewCounter("repair_build_tree_latency"),
		totalRepairBuildTreeConflicts: omr.NewCounter("repair_build_tree_conflicts"),

		totalRepairSuccessCount: omr.NewCounter("property_repair_success_count", "group", "shard"),
		totalRepairFailedCount:  omr.NewCounter("property_repair_failure_count", "group", "shard"),
	}
}
