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
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort" // added for sorting parts
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/schema"
)

func (tst *tsTable) currentSnapshot() *snapshot {
	tst.RLock()
	defer tst.RUnlock()
	if tst.snapshot == nil {
		return nil
	}
	s := tst.snapshot
	s.incRef()
	return s
}

type snapshotCreator rune

const (
	snapshotCreatorMemPart = iota
	snapshotCreatorFlusher
	snapshotCreatorMerger
	snapshotCreatorMergedFlusher
	snapshotCreatorSyncer
)

type snapshot struct {
	parts   []*partWrapper
	epoch   uint64
	creator snapshotCreator

	ref int32
}

func (s *snapshot) getParts(dst []*part, minTimestamp int64, maxTimestamp int64) ([]*part, int) {
	var count int
	for _, p := range s.parts {
		pm := p.p.partMetadata
		if maxTimestamp < pm.MinTimestamp || minTimestamp > pm.MaxTimestamp {
			continue
		}
		// TODO: filter parts
		dst = append(dst, p.p)
		count++
	}
	return dst, count
}

func (s *snapshot) incRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *snapshot) decRef() {
	n := atomic.AddInt32(&s.ref, -1)
	if n > 0 {
		return
	}
	for i := range s.parts {
		s.parts[i].decRef()
	}
	s.parts = s.parts[:0]
}

func (s *snapshot) copyAllTo(nextEpoch uint64) snapshot {
	var result snapshot
	result.epoch = nextEpoch
	result.ref = 1
	for i := range s.parts {
		s.parts[i].incRef()
		result.parts = append(result.parts, s.parts[i])
	}
	return result
}

func (s *snapshot) merge(nextEpoch uint64, nextParts map[uint64]*partWrapper) snapshot {
	var result snapshot
	result.epoch = nextEpoch
	result.ref = 1
	for i := 0; i < len(s.parts); i++ {
		if n, ok := nextParts[s.parts[i].ID()]; ok {
			result.parts = append(result.parts, n)
			continue
		}
		s.parts[i].incRef()
		result.parts = append(result.parts, s.parts[i])
	}
	return result
}

func (s *snapshot) remove(nextEpoch uint64, merged map[uint64]struct{}) snapshot {
	var result snapshot
	result.epoch = nextEpoch
	result.ref = 1
	for i := 0; i < len(s.parts); i++ {
		if _, ok := merged[s.parts[i].ID()]; !ok {
			s.parts[i].incRef()
			result.parts = append(result.parts, s.parts[i])
			continue
		}
		s.parts[i].removable.Store(true)
	}
	return result
}

func getDisjointParts(parts []*part, asc bool) [][]*part {
	if len(parts) == 0 {
		return nil
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].partMetadata.MinTimestamp < parts[j].partMetadata.MinTimestamp
	})

	var groups [][]*part
	var currentGroup []*part
	var boundary int64
	for _, p := range parts {
		pMin := p.partMetadata.MinTimestamp
		pMax := p.partMetadata.MaxTimestamp
		if len(currentGroup) == 0 {
			currentGroup = append(currentGroup, p)
			boundary = pMax
		} else {
			if pMin <= boundary {
				currentGroup = append(currentGroup, p)
				if pMax > boundary {
					boundary = pMax
				}
			} else {
				groups = append(groups, currentGroup)
				currentGroup = []*part{p}
				boundary = pMax
			}
		}
	}

	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	if !asc {
		for i, j := 0, len(groups)-1; i < j; i, j = i+1, j-1 {
			groups[i], groups[j] = groups[j], groups[i]
		}
	}
	return groups
}

func snapshotName(snapshot uint64) string {
	return fmt.Sprintf("%016x%s", snapshot, snapshotSuffix)
}

func parseSnapshot(name string) (uint64, error) {
	if filepath.Ext(name) != snapshotSuffix {
		return 0, errors.New("invalid snapshot file ext")
	}
	if len(name) < 16 {
		return 0, errors.New("invalid snapshot file name")
	}
	return parseEpoch(name[:16])
}

func (tst *tsTable) TakeFileSnapshot(dst string) error {
	snapshot := tst.currentSnapshot()
	if snapshot == nil {
		return fmt.Errorf("no current snapshot available")
	}
	defer snapshot.decRef()

	for _, pw := range snapshot.parts {
		if pw.mp != nil {
			continue
		}

		part := pw.p
		srcPath := part.path
		destPartPath := filepath.Join(dst, filepath.Base(srcPath))

		if err := tst.fileSystem.CreateHardLink(srcPath, destPartPath, nil); err != nil {
			return fmt.Errorf("failed to create snapshot for part %d: %w", part.partMetadata.ID, err)
		}
	}
	tst.createMetadata(dst, snapshot)
	parent := filepath.Dir(dst)
	tst.fileSystem.SyncPath(parent)
	return nil
}

func (tst *tsTable) createMetadata(dst string, snapshot *snapshot) {
	var partNames []string
	for i := range snapshot.parts {
		partNames = append(partNames, partName(snapshot.parts[i].ID()))
	}
	data, err := json.Marshal(partNames)
	if err != nil {
		logger.Panicf("cannot marshal partNames to JSON: %s", err)
	}
	snapshotPath := filepath.Join(dst, snapshotName(snapshot.epoch))
	lf, err := tst.fileSystem.CreateFile(snapshotPath, storage.FilePerm)
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

func (s *standalone) takeGroupSnapshot(dstDir string, groupName string) error {
	group, ok := s.schemaRepo.LoadGroup(groupName)
	if !ok {
		return errors.Errorf("group %s not found", groupName)
	}
	db := group.SupplyTSDB()
	if db == nil {
		return errors.Errorf("group %s has no tsdb", group.GetSchema().Metadata.Name)
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	if err := tsdb.TakeFileSnapshot(dstDir); err != nil {
		return errors.WithMessagef(err, "snapshot %s fail to take file snapshot for group %s", dstDir, group.GetSchema().Metadata.Name)
	}
	return nil
}

type snapshotListener struct {
	*bus.UnImplementedHealthyListener
	s           *standalone
	snapshotSeq uint64
	snapshotMux sync.Mutex
}

// Rev takes a snapshot of the database.
func (s *snapshotListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	groups := message.Data().([]*databasev1.SnapshotRequest_Group)
	var gg []schema.Group
	if len(groups) == 0 {
		gg = s.s.schemaRepo.LoadAllGroups()
	} else {
		for _, g := range groups {
			if g.Catalog != commonv1.Catalog_CATALOG_TRACE {
				continue
			}
			group, ok := s.s.schemaRepo.LoadGroup(g.Group)
			if !ok {
				continue
			}
			gg = append(gg, group)
		}
	}
	if len(gg) == 0 {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	s.snapshotMux.Lock()
	defer s.snapshotMux.Unlock()
	storage.DeleteStaleSnapshots(s.s.snapshotDir, s.s.maxFileSnapshotNum, s.s.lfs)
	sn := s.snapshotName()
	var err error
	for _, g := range gg {
		select {
		case <-ctx.Done():
			return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
		default:
		}
		if errGroup := s.s.takeGroupSnapshot(filepath.Join(s.s.snapshotDir, sn, g.GetSchema().Metadata.Name), g.GetSchema().Metadata.Name); err != nil {
			s.s.l.Error().Err(errGroup).Str("group", g.GetSchema().Metadata.Name).Msg("fail to take group snapshot")
			err = multierr.Append(err, errGroup)
			continue
		}
	}
	snp := &databasev1.Snapshot{
		Name:    sn,
		Catalog: commonv1.Catalog_CATALOG_TRACE,
	}
	if err != nil {
		snp.Error = err.Error()
	}
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), snp)
}

func (s *snapshotListener) snapshotName() string {
	s.snapshotSeq++
	return fmt.Sprintf("%s-%08X", time.Now().UTC().Format("20060102150405"), s.snapshotSeq)
}
