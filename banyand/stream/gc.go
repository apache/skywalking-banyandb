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
	"path/filepath"
)

type garbageCleaner struct {
	parent          *tsTable
	liveParts       map[uint64]map[uint64]struct{}
	knownPartFiles  map[uint64]struct{}
	deletableEpochs []uint64
	liveEpoch       uint64
}

func (g *garbageCleaner) init(parent *tsTable) {
	g.parent = parent
	g.liveParts = make(map[uint64]map[uint64]struct{})
	g.knownPartFiles = make(map[uint64]struct{})
}

func (g *garbageCleaner) registerSnapshot(snapshot *snapshot) {
	parts := make(map[uint64]struct{})
	for _, part := range snapshot.parts {
		parts[part.ID()] = struct{}{}
		g.knownPartFiles[part.ID()] = struct{}{}
	}
	g.liveParts[snapshot.epoch] = parts

	if g.liveEpoch > 0 {
		g.deletableEpochs = append(g.deletableEpochs, g.liveEpoch)
	}
	g.liveEpoch = snapshot.epoch
}

func (g *garbageCleaner) submitParts(partID uint64) {
	g.knownPartFiles[partID] = struct{}{}
}

func (g *garbageCleaner) clean() {
	g.cleanSnapshots()
	g.cleanParts()
}

func (g *garbageCleaner) cleanSnapshots() {
	var remainingEpochs []uint64
	for _, deletableEpoch := range g.deletableEpochs {
		path := filepath.Join(g.parent.root, snapshotName(deletableEpoch))
		err := g.parent.fileSystem.DeleteFile(path)
		if err == nil {
			delete(g.liveParts, deletableEpoch)
			continue
		}
		g.parent.l.Warn().Err(err).Msgf("cannot delete snapshot file: %s", path)
		remainingEpochs = append(remainingEpochs, deletableEpoch)
	}
	g.deletableEpochs = remainingEpochs
}

func (g garbageCleaner) cleanParts() {
OUTER:
	for partID := range g.knownPartFiles {
		for _, partInSnapshot := range g.liveParts {
			if _, ok := partInSnapshot[partID]; ok {
				continue OUTER
			}
		}
		g.parent.fileSystem.MustRMAll(partPath(g.parent.root, partID))
		delete(g.knownPartFiles, partID)
	}
}
