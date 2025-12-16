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

package trace

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

const (
	snapshotSuffix = ".snp"
	sidxDirName    = "sidx"
)

type tsTable struct {
	pm            protector.Memory
	fileSystem    fs.FileSystem
	handoffCtrl   *handoffController
	metrics       *metrics
	snapshot      *snapshot
	loopCloser    *run.Closer
	getNodes      func() []string
	l             *logger.Logger
	sidxMap       map[string]sidx.SIDX
	introductions chan *introduction
	p             common.Position
	root          string
	group         string
	gc            garbageCleaner
	option        option
	curPartID     uint64
	sync.RWMutex
	shardID common.ShardID
}

func (tst *tsTable) loadSnapshot(epoch uint64, loadedParts []uint64) {
	parts := tst.mustReadSnapshot(epoch)
	snp := snapshot{
		epoch: epoch,
	}
	needToPersist := false
	for _, id := range loadedParts {
		var find bool
		for j := range parts {
			if id == parts[j] {
				find = true
				break
			}
		}
		if !find {
			tst.gc.removePart(id)
			continue
		}
		err := validatePartMetadata(tst.fileSystem, partPath(tst.root, id))
		if err != nil {
			tst.l.Info().Err(err).Uint64("id", id).Msg("cannot validate part metadata. skip and delete it")
			tst.gc.removePart(id)
			needToPersist = true
			continue
		}
		p := mustOpenFilePart(id, tst.root, tst.fileSystem)
		p.partMetadata.ID = id
		snp.parts = append(snp.parts, newPartWrapper(nil, p))
		if tst.curPartID < id {
			tst.curPartID = id
		}
	}
	tst.gc.registerSnapshot(&snp)
	tst.gc.clean()
	if len(snp.parts) < 1 {
		return
	}
	snp.incRef()
	tst.snapshot = &snp
	tst.loadSidxMap(parts)
	if needToPersist {
		tst.persistSnapshot(&snp)
	}
}

func (tst *tsTable) startLoop(cur uint64) {
	tst.loopCloser = run.NewCloser(1 + 3)
	tst.introductions = make(chan *introduction)
	flushCh := make(chan *flusherIntroduction)
	mergeCh := make(chan *mergerIntroduction)
	introducerWatcher := make(watcher.Channel, 1)
	flusherWatcher := make(watcher.Channel, 1)
	go tst.introducerLoop(flushCh, mergeCh, introducerWatcher, cur+1)
	go tst.flusherLoop(flushCh, mergeCh, introducerWatcher, flusherWatcher, cur)
	go tst.mergeLoop(mergeCh, flusherWatcher)
}

func (tst *tsTable) startLoopWithConditionalMerge(cur uint64) {
	tst.loopCloser = run.NewCloser(1 + 3)
	tst.introductions = make(chan *introduction)
	flushCh := make(chan *flusherIntroduction)
	syncCh := make(chan *syncIntroduction)
	mergeCh := make(chan *mergerIntroduction)
	introducerWatcher := make(watcher.Channel, 1)
	flusherWatcher := make(watcher.Channel, 1)
	go tst.introducerLoopWithSync(flushCh, mergeCh, syncCh, introducerWatcher, cur+1)
	go tst.flusherLoop(flushCh, mergeCh, introducerWatcher, flusherWatcher, cur)
	go tst.syncLoop(syncCh, flusherWatcher)
}

func parseEpoch(epochStr string) (uint64, error) {
	p, err := strconv.ParseUint(epochStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse path %s: %w", epochStr, err)
	}
	return p, nil
}

func (tst *tsTable) mustWriteSnapshot(snapshot uint64, partNames []string) {
	data, err := json.Marshal(partNames)
	if err != nil {
		logger.Panicf("cannot marshal partNames to JSON: %s", err)
	}
	snapshotPath := filepath.Join(tst.root, snapshotName(snapshot))
	lf, err := tst.fileSystem.CreateLockFile(snapshotPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", snapshotPath, err)
	}
	n, err := lf.Write(data)
	if err != nil {
		logger.Panicf("cannot write snapshot %s: %s", snapshotPath, err)
	}
	if n != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", snapshotPath, n, len(data))
	}
}

func (tst *tsTable) mustReadSnapshot(snapshot uint64) []uint64 {
	snapshotPath := filepath.Join(tst.root, snapshotName(snapshot))
	data, err := tst.fileSystem.Read(snapshotPath)
	if err != nil {
		logger.Panicf("cannot read %s: %s", snapshotPath, err)
	}
	var partNames []string
	if err := json.Unmarshal(data, &partNames); err != nil {
		logger.Panicf("cannot parse %s: %s", snapshotPath, err)
	}
	var result []uint64
	for i := range partNames {
		e, err := parseEpoch(partNames[i])
		if err != nil {
			logger.Panicf("cannot parse %s: %s", partNames[i], err)
		}
		result = append(result, e)
	}
	return result
}

// initTSTable initializes a tsTable and loads parts/snapshots, but does not start any background loops.
func initTSTable(fileSystem fs.FileSystem, rootPath string, p common.Position,
	l *logger.Logger, option option, m any,
) (*tsTable, uint64) {
	if option.protector == nil {
		logger.GetLogger("trace").
			Panic().
			Msg("protector can not be nil")
	}
	tst := tsTable{
		fileSystem: fileSystem,
		root:       rootPath,
		option:     option,
		l:          l,
		p:          p,
		pm:         option.protector,
	}
	if m != nil {
		tst.metrics = m.(*metrics)
	}
	tst.gc.init(&tst)
	ee := fileSystem.ReadDir(rootPath)
	if len(ee) == 0 {
		return &tst, uint64(time.Now().UnixNano())
	}
	var loadedParts []uint64
	var loadedSnapshots []uint64
	var needToDelete []string
	for i := range ee {
		if ee[i].IsDir() {
			if ee[i].Name() == sidxDirName {
				continue
			}
			if ee[i].Name() == storage.FailedPartsDirName {
				continue
			}
			p, err := parseEpoch(ee[i].Name())
			if err != nil {
				l.Info().Err(err).Msg("cannot parse part file name. skip and delete it")
				needToDelete = append(needToDelete, ee[i].Name())
				continue
			}
			err = validatePartMetadata(fileSystem, filepath.Join(rootPath, ee[i].Name()))
			if err != nil {
				l.Info().Err(err).Msg("cannot validate part metadata. skip and delete it")
				needToDelete = append(needToDelete, ee[i].Name())
				continue
			}
			loadedParts = append(loadedParts, p)
			continue
		}
		if filepath.Ext(ee[i].Name()) != snapshotSuffix {
			continue
		}
		snapshot, err := parseSnapshot(ee[i].Name())
		if err != nil {
			l.Info().Err(err).Msg("cannot parse snapshot file name. skip and delete it")
			needToDelete = append(needToDelete, ee[i].Name())
			continue
		}
		loadedSnapshots = append(loadedSnapshots, snapshot)
	}
	for i := range needToDelete {
		l.Info().Str("path", filepath.Join(rootPath, needToDelete[i])).Msg("delete invalid directory or file")
		fileSystem.MustRMAll(filepath.Join(rootPath, needToDelete[i]))
	}
	if len(loadedParts) == 0 || len(loadedSnapshots) == 0 {
		for _, id := range loadedSnapshots {
			fileSystem.MustRMAll(filepath.Join(rootPath, snapshotName(id)))
		}
		return &tst, uint64(time.Now().UnixNano())
	}
	sort.Slice(loadedSnapshots, func(i, j int) bool {
		return loadedSnapshots[i] > loadedSnapshots[j]
	})
	epoch := loadedSnapshots[0]
	tst.loadSnapshot(epoch, loadedParts)
	return &tst, epoch
}

func newTSTable(fileSystem fs.FileSystem, rootPath string, p common.Position,
	l *logger.Logger, _ timestamp.TimeRange, option option, m any,
) (*tsTable, error) {
	t, epoch := initTSTable(fileSystem, rootPath, p, l, option, m)
	t.startLoop(epoch)
	return t, nil
}

func (tst *tsTable) getSidx(name string) (sidx.SIDX, bool) {
	tst.RLock()
	defer tst.RUnlock()
	sidxInstance, ok := tst.sidxMap[name]
	return sidxInstance, ok
}

func (tst *tsTable) mustGetSidx(name string) sidx.SIDX {
	sidxInstance, ok := tst.getSidx(name)
	if !ok {
		tst.l.Panic().Str("name", name).Msg("sidx not found")
	}
	return sidxInstance
}

func (tst *tsTable) mustGetOrCreateSidx(name string) sidx.SIDX {
	sidxInstance, err := tst.getOrCreateSidx(name)
	if err != nil {
		tst.l.Panic().Str("name", name).Msg("cannot get or create sidx")
	}
	return sidxInstance
}

func (tst *tsTable) getOrCreateSidx(name string) (sidx.SIDX, error) {
	if sidxInstance, ok := tst.getSidx(name); ok {
		return sidxInstance, nil
	}

	tst.Lock()
	defer tst.Unlock()

	// Double-check after acquiring write lock
	if sidxInstance, ok := tst.sidxMap[name]; ok {
		return sidxInstance, nil
	}

	// Create new sidx instance
	sidxPath := filepath.Join(tst.root, sidxDirName, name)
	sidxOpts, err := sidx.NewOptions(sidxPath, tst.option.protector)
	if err != nil {
		return nil, fmt.Errorf("cannot create sidx options for %s: %w", name, err)
	}

	newSidx, err := sidx.NewSIDX(tst.fileSystem, sidxOpts)
	if err != nil {
		return nil, fmt.Errorf("cannot create sidx for %s: %w", name, err)
	}
	if tst.sidxMap == nil {
		tst.sidxMap = make(map[string]sidx.SIDX)
	}

	tst.sidxMap[name] = newSidx
	return newSidx, nil
}

// getAllSidx returns all sidx instances for potential multi-sidx queries.
func (tst *tsTable) getAllSidx() map[string]sidx.SIDX {
	tst.RLock()
	defer tst.RUnlock()

	result := make(map[string]sidx.SIDX, len(tst.sidxMap))
	for name, sidxInstance := range tst.sidxMap {
		result[name] = sidxInstance
	}
	return result
}

// enqueueForOfflineNodes enqueues parts for offline nodes via the handoff controller.
func (tst *tsTable) enqueueForOfflineNodes(onlineNodes []string, partsToSync []*part, partIDsToSync map[uint64]struct{}) {
	if tst.handoffCtrl == nil {
		return
	}

	// Check if there are any offline nodes before doing expensive preparation work
	offlineNodes := tst.handoffCtrl.calculateOfflineNodes(onlineNodes, tst.group, tst.shardID)
	tst.l.Debug().
		Str("group", tst.group).
		Uint32("shardID", uint32(tst.shardID)).
		Strs("onlineNodes", onlineNodes).
		Strs("offlineNodes", offlineNodes).
		Msg("handoff enqueue evaluation")
	if len(offlineNodes) == 0 {
		return
	}

	// Prepare core parts info
	coreParts := make([]partInfo, 0, len(partsToSync))
	for _, part := range partsToSync {
		coreParts = append(coreParts, partInfo{
			partID:  part.partMetadata.ID,
			path:    part.path,
			group:   tst.group,
			shardID: tst.shardID,
		})
	}

	// Get sidx part paths from each sidx instance
	sidxMap := tst.getAllSidx()
	sidxParts := make(map[string][]partInfo)
	for sidxName, sidxInstance := range sidxMap {
		partPaths := sidxInstance.PartPaths(partIDsToSync)
		if len(partPaths) == 0 {
			continue
		}

		parts := make([]partInfo, 0, len(partPaths))
		for partID, path := range partPaths {
			parts = append(parts, partInfo{
				partID:  partID,
				path:    path,
				group:   tst.group,
				shardID: tst.shardID,
			})
		}
		sidxParts[sidxName] = parts
	}

	// Call handoff controller with offline nodes
	if err := tst.handoffCtrl.enqueueForOfflineNodes(offlineNodes, coreParts, sidxParts); err != nil {
		tst.l.Warn().Err(err).Msg("handoff enqueue completed with errors")
	}
}

func (tst *tsTable) loadSidxMap(availablePartIDs []uint64) {
	tst.sidxMap = make(map[string]sidx.SIDX)
	sidxRootPath := filepath.Join(tst.root, sidxDirName)
	if _, err := os.Stat(sidxRootPath); os.IsNotExist(err) {
		tst.l.Debug().Str("path", sidxRootPath).Msg("sidx directory does not exist")
		return
	}
	entries := tst.fileSystem.ReadDir(sidxRootPath)
	if len(entries) == 0 {
		tst.l.Debug().Str("path", sidxRootPath).Msg("no sidx entries found")
		return
	}

	for i := range entries {
		if !entries[i].IsDir() {
			continue
		}
		sidxName := entries[i].Name()
		sidxPath := filepath.Join(sidxRootPath, sidxName)
		sidxOpts, err := sidx.NewOptions(sidxPath, tst.option.protector)
		if err != nil {
			tst.l.Error().Err(err).Str("name", sidxName).Msg("failed to create sidx options, skipping")
			continue
		}
		sidxOpts.AvailablePartIDs = availablePartIDs
		newSidx, err := sidx.NewSIDX(tst.fileSystem, sidxOpts)
		if err != nil {
			tst.l.Error().Err(err).Str("name", sidxName).Msg("failed to create sidx instance, skipping")
			continue
		}
		tst.sidxMap[sidxName] = newSidx
		tst.l.Debug().Str("name", sidxName).Str("path", sidxPath).Msg("loaded sidx instance")
	}
}

func (tst *tsTable) closeSidxMap() error {
	var lastErr error
	for name, sidxInstance := range tst.sidxMap {
		if err := sidxInstance.Close(); err != nil {
			tst.l.Error().Err(err).Str("sidx", name).Msg("failed to close sidx")
			lastErr = err
		}
	}
	return lastErr
}

func (tst *tsTable) Close() error {
	if tst.loopCloser != nil {
		tst.loopCloser.Done()
		tst.loopCloser.CloseThenWait()
	}
	tst.Lock()
	defer tst.Unlock()
	tst.deleteMetrics()
	if tst.snapshot == nil {
		return tst.closeSidxMap()
	}
	tst.snapshot.decRef()
	tst.snapshot = nil
	return tst.closeSidxMap()
}

func (tst *tsTable) mustAddMemPart(mp *memPart, sidxReqsMap map[string]*sidx.MemPart) {
	p := openMemPart(mp)

	ind := generateIntroduction()
	defer releaseIntroduction(ind)
	ind.applied = make(chan struct{})
	ind.memPart = newPartWrapper(mp, p)
	ind.memPart.p.partMetadata.ID = atomic.AddUint64(&tst.curPartID, 1)
	ind.sidxReqsMap = sidxReqsMap
	startTime := time.Now()
	totalCount := mp.partMetadata.TotalCount
	select {
	case tst.introductions <- ind:
	case <-tst.loopCloser.CloseNotify():
		ind.memPart.decRef()
		return
	}
	select {
	case <-ind.applied:
	case <-tst.loopCloser.CloseNotify():
	}
	tst.incTotalWritten(int(totalCount))
	tst.incTotalBatch(1)
	tst.incTotalBatchIntroLatency(time.Since(startTime).Seconds())
}

func (tst *tsTable) mustAddTraces(ts *traces, sidxReqsMap map[string]*sidx.MemPart) {
	tst.mustAddTracesWithSegmentID(ts, 0, sidxReqsMap, nil)
}

func (tst *tsTable) mustAddTracesWithSegmentID(ts *traces, segmentID int64, sidxReqsMap map[string]*sidx.MemPart, seriesMetadata []byte) {
	if len(ts.traceIDs) == 0 {
		return
	}

	mp := generateMemPart()
	mp.mustInitFromTraces(ts)
	mp.segmentID = segmentID
	if len(seriesMetadata) > 0 {
		// Write series metadata to buffer to avoid sharing the underlying slice
		_, err := mp.seriesMetadata.Write(seriesMetadata)
		if err != nil {
			logger.Panicf("cannot write series metadata to buffer: %s", err)
		}
	}

	tst.mustAddMemPart(mp, sidxReqsMap)
}

type tstIter struct {
	err    error
	parts  []*part
	piPool []*partIter
	idx    int
}

func (ti *tstIter) reset() {
	ti.parts = nil

	for i := range ti.piPool {
		ti.piPool[i].reset()
	}
	ti.piPool = ti.piPool[:0]

	ti.err = nil
	ti.idx = 0
}

func (ti *tstIter) init(bma *blockMetadataArray, parts []*part, groupedTids [][]string) {
	ti.reset()
	ti.parts = parts

	if n := len(ti.parts) - cap(ti.piPool); n > 0 {
		ti.piPool = append(ti.piPool[:cap(ti.piPool)], make([]*partIter, n)...)
	}
	ti.piPool = ti.piPool[:len(ti.parts)]
	for i, p := range ti.parts {
		ti.piPool[i] = &partIter{}
		ti.piPool[i].init(bma, p, groupedTids[i])
	}

	if len(ti.piPool) == 0 {
		ti.err = io.EOF
		return
	}
}

func (ti *tstIter) nextBlock() bool {
	if ti.err != nil {
		return false
	}

	ti.err = ti.next()
	if ti.err != nil {
		if errors.Is(ti.err, io.EOF) {
			ti.err = fmt.Errorf("cannot obtain the next block to search in the partition: %w", ti.err)
		}
		return false
	}
	return true
}

func (ti *tstIter) next() error {
	for ti.idx < len(ti.piPool) {
		pi := ti.piPool[ti.idx]
		if pi.nextBlock() {
			return nil
		}
		if err := pi.error(); err != nil {
			return err
		}
		ti.idx++
	}
	return io.EOF
}

func (ti *tstIter) Error() error {
	if errors.Is(ti.err, io.EOF) {
		return nil
	}
	return ti.err
}

func generateTstIter() *tstIter {
	v := tstIterPool.Get()
	if v == nil {
		return &tstIter{}
	}
	return v
}

func releaseTstIter(ti *tstIter) {
	ti.reset()
	tstIterPool.Put(ti)
}

var tstIterPool = pool.Register[*tstIter]("trace-tstIter")
