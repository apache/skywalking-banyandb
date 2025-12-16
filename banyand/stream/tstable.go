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

package stream

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

const (
	snapshotSuffix = ".snp"
)

type tsTable struct {
	fileSystem    fs.FileSystem
	pm            protector.Memory
	metrics       *metrics
	index         *elementIndex
	snapshot      *snapshot
	loopCloser    *run.Closer
	getNodes      func() []string
	l             *logger.Logger
	introductions chan *introduction
	p             common.Position
	group         string
	root          string
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
	mergeCh := make(chan *mergerIntroduction)
	syncCh := make(chan *syncIntroduction)
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
	l *logger.Logger, option option, m any, initIndex bool,
) (*tsTable, uint64, error) {
	if option.protector == nil {
		logger.GetLogger("stream").
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
	var indexMetrics *inverted.Metrics
	if m != nil {
		tst.metrics = m.(*metrics)
		indexMetrics = tst.metrics.indexMetrics
	}
	if initIndex {
		index, err := newElementIndex(context.TODO(), rootPath, option.elementIndexFlushTimeout.Nanoseconds()/int64(time.Second), indexMetrics)
		if err != nil {
			return nil, 0, err
		}
		tst.index = index
	}
	tst.gc.init(&tst)
	ee := fileSystem.ReadDir(rootPath)
	if len(ee) == 0 {
		return &tst, uint64(time.Now().UnixNano()), nil
	}
	var loadedParts []uint64
	var loadedSnapshots []uint64
	var needToDelete []string
	for i := range ee {
		if ee[i].IsDir() {
			if ee[i].Name() == elementIndexFilename {
				continue
			}
			if ee[i].Name() == inverted.ExternalSegmentTempDirName {
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
		return &tst, uint64(time.Now().UnixNano()), nil
	}
	sort.Slice(loadedSnapshots, func(i, j int) bool {
		return loadedSnapshots[i] > loadedSnapshots[j]
	})
	epoch := loadedSnapshots[0]
	tst.loadSnapshot(epoch, loadedParts)
	return &tst, epoch, nil
}

func newTSTable(fileSystem fs.FileSystem, rootPath string, p common.Position,
	l *logger.Logger, _ timestamp.TimeRange, option option, m any,
) (*tsTable, error) {
	t, epoch, err := initTSTable(fileSystem, rootPath, p, l, option, m, true)
	if err != nil {
		return nil, err
	}
	t.startLoop(epoch)
	return t, nil
}

func (tst *tsTable) Index() *elementIndex {
	if tst.index == nil {
		logger.Panicf("index is not initialized for this tsTable")
	}
	return tst.index
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
		if tst.index != nil {
			return tst.index.Close()
		}
		return nil
	}
	tst.snapshot.decRef()
	tst.snapshot = nil
	if tst.index != nil {
		return tst.index.Close()
	}
	return nil
}

func (tst *tsTable) mustAddMemPart(mp *memPart) {
	p := openMemPart(mp)

	ind := generateIntroduction()
	defer releaseIntroduction(ind)
	ind.applied = make(chan struct{})
	ind.memPart = newPartWrapper(mp, p)
	ind.memPart.p.partMetadata.ID = atomic.AddUint64(&tst.curPartID, 1)
	startTime := time.Now()
	totalCount := mp.partMetadata.TotalCount
	select {
	case tst.introductions <- ind:
	case <-tst.loopCloser.CloseNotify():
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

func (tst *tsTable) mustAddElements(es *elements) {
	tst.mustAddElementsWithSegmentID(es, 0, nil)
}

func (tst *tsTable) mustAddElementsWithSegmentID(es *elements, segmentID int64, seriesMetadata []byte) {
	if len(es.seriesIDs) == 0 {
		return
	}

	mp := generateMemPart()
	mp.mustInitFromElements(es)
	mp.segmentID = segmentID
	if len(seriesMetadata) > 0 {
		// Write series metadata to buffer to avoid sharing the underlying slice
		_, err := mp.seriesMetadata.Write(seriesMetadata)
		if err != nil {
			logger.Panicf("cannot write series metadata to buffer: %s", err)
		}
	}
	tst.mustAddMemPart(mp)
}

type tstIter struct {
	err           error
	parts         []*part
	piPool        []partIter
	piHeap        partIterHeap
	nextBlockNoop bool
}

func (ti *tstIter) reset() {
	for i := range ti.parts {
		ti.parts[i] = nil
	}
	ti.parts = ti.parts[:0]

	for i := range ti.piPool {
		ti.piPool[i].reset()
	}
	ti.piPool = ti.piPool[:0]

	for i := range ti.piHeap {
		ti.piHeap[i] = nil
	}
	ti.piHeap = ti.piHeap[:0]

	ti.err = nil
	ti.nextBlockNoop = false
}

func (ti *tstIter) init(bma *blockMetadataArray, parts []*part, sids []common.SeriesID, minTimestamp, maxTimestamp int64, blockFilter index.Filter) {
	ti.reset()
	ti.parts = parts

	if n := len(ti.parts) - cap(ti.piPool); n > 0 {
		ti.piPool = append(ti.piPool[:cap(ti.piPool)], make([]partIter, n)...)
	}
	ti.piPool = ti.piPool[:len(ti.parts)]
	for i, p := range ti.parts {
		ti.piPool[i].init(bma, p, sids, minTimestamp, maxTimestamp, blockFilter)
	}

	ti.piHeap = ti.piHeap[:0]
	for i := range ti.piPool {
		ps := &ti.piPool[i]
		if !ps.nextBlock() {
			if err := ps.error(); err != nil {
				ti.err = fmt.Errorf("cannot initialize tsTable iteration: %w", err)
				return
			}
			continue
		}
		ti.piHeap = append(ti.piHeap, ps)
	}
	if len(ti.piHeap) == 0 {
		ti.err = io.EOF
		return
	}
	heap.Init(&ti.piHeap)
	ti.nextBlockNoop = true
}

func (ti *tstIter) nextBlock() bool {
	if ti.err != nil {
		return false
	}
	if ti.nextBlockNoop {
		ti.nextBlockNoop = false
		return true
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
	psMin := ti.piHeap[0]
	if psMin.nextBlock() {
		heap.Fix(&ti.piHeap, 0)
		return nil
	}

	if err := psMin.error(); err != nil {
		return err
	}

	heap.Pop(&ti.piHeap)

	if len(ti.piHeap) == 0 {
		return io.EOF
	}
	return nil
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

var tstIterPool = pool.Register[*tstIter]("stream-tstIter")

type partIterHeap []*partIter

func (pih *partIterHeap) Len() int {
	return len(*pih)
}

func (pih *partIterHeap) Less(i, j int) bool {
	x := *pih
	return x[i].curBlock.less(x[j].curBlock)
}

func (pih *partIterHeap) Swap(i, j int) {
	x := *pih
	x[i], x[j] = x[j], x[i]
}

func (pih *partIterHeap) Push(x any) {
	*pih = append(*pih, x.(*partIter))
}

func (pih *partIterHeap) Pop() any {
	a := *pih
	v := a[len(a)-1]
	*pih = a[:len(a)-1]
	return v
}
