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

package measure

import (
	"path"
	"sort"
)

type garbageCleaner struct {
	parent    *tsTable
	snapshots []uint64
	parts     []uint64
}

func (g *garbageCleaner) registerSnapshot(snapshot uint64) {
	g.snapshots = append(g.snapshots, snapshot)
}

func (g *garbageCleaner) submitParts(parts ...uint64) {
	g.parts = append(g.parts, parts...)
}

func (g garbageCleaner) clean() {
	if len(g.snapshots) > 1 {
		g.cleanSnapshots()
	}
	if len(g.parts) > 0 {
		g.cleanParts()
	}
}

func (g *garbageCleaner) cleanSnapshots() {
	if len(g.snapshots) < 2 {
		return
	}
	sort.Slice(g.snapshots, func(i, j int) bool {
		return g.snapshots[i] < g.snapshots[j]
	})
	// keep the latest snapshot
	for i := 0; i < len(g.snapshots)-1; i++ {
		filePath := path.Join(g.parent.root, snapshotName(g.snapshots[i]))
		if err := g.parent.fileSystem.DeleteFile(filePath); err != nil {
			g.parent.l.Warn().Err(err).Str("path", filePath).Msg("failed to delete snapshot. Please check manually")
		}
	}
	g.snapshots = g.snapshots[len(g.snapshots)-1:]
}

func (g garbageCleaner) cleanParts() {
	panic("implement me")
}
