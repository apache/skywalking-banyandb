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
	"fmt"
	"path/filepath"
	"sort" // added for sorting parts
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

func (s *snapshot) getParts(dst []*part, minTimestamp int64, maxTimestamp int64, traceIDs []string) ([]*part, int) {
	shouldSkip := func(p *part) bool {
		if p.traceIDFilter.filter == nil {
			return false
		}
		for _, traceID := range traceIDs {
			if p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
				return false
			}
		}
		return true
	}

	var count int
	for _, p := range s.parts {
		pm := p.p.partMetadata
		if maxTimestamp < pm.MinTimestamp || minTimestamp > pm.MaxTimestamp {
			continue
		}
		if shouldSkip(p.p) {
			continue
		}
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
	var removedCount int
	for i := 0; i < len(s.parts); i++ {
		if _, ok := merged[s.parts[i].ID()]; !ok {
			s.parts[i].incRef()
			result.parts = append(result.parts, s.parts[i])
			continue
		}
		s.parts[i].removable.Store(true)
		removedCount++
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
