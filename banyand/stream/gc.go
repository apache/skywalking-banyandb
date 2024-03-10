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
	deletableEpochs []uint64
	liveEpoch       uint64
}

func (g *garbageCleaner) init(parent *tsTable) {
	g.parent = parent
}

func (g *garbageCleaner) registerSnapshot(snapshot *snapshot) {
	if g.liveEpoch > 0 {
		g.deletableEpochs = append(g.deletableEpochs, g.liveEpoch)
	}
	g.liveEpoch = snapshot.epoch
}

func (g *garbageCleaner) removePart(partID uint64) {
	g.parent.fileSystem.MustRMAll(partPath(g.parent.root, partID))
}

func (g *garbageCleaner) clean() {
	var remainingEpochs []uint64
	for _, deletableEpoch := range g.deletableEpochs {
		path := filepath.Join(g.parent.root, snapshotName(deletableEpoch))
		err := g.parent.fileSystem.DeleteFile(path)
		if err == nil {
			continue
		}
		g.parent.l.Warn().Err(err).Msgf("cannot delete snapshot file: %s", path)
		remainingEpochs = append(remainingEpochs, deletableEpoch)
	}
	g.deletableEpochs = remainingEpochs
}
