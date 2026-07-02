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

package migration

import (
	"os"
	"path/filepath"
	"sort"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
)

// backupDirDate returns the plan's backup root + date, or empty strings in
// live mode (where every entry carries explicit Source paths instead).
func (p *CopyPlan) backupDirDate() (string, string) {
	if p.Source.Backup != nil {
		return p.Source.Backup.Root, p.Source.Backup.Date
	}
	return "", ""
}

// ResolveEntrySrcRoots picks the source group dirs for one (entry, group)
// pair. When entry.Source is set, treat each entry as a data root and look
// for "<source>/<group>"; otherwise fall back to the backup-snapshot layout
// via Nodes. Either path can yield zero dirs — the caller logs and skips.
func (p *CopyPlan) ResolveEntrySrcRoots(catalog commonv1.Catalog, entry ResolvedEntry, group string) []string {
	if len(entry.Source) > 0 {
		var roots []string
		for _, src := range entry.Source {
			candidate := filepath.Join(src, group)
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				roots = append(roots, candidate)
			}
		}
		sort.Strings(roots)
		return roots
	}
	backupDir, date := p.backupDirDate()
	return collectEntrySrcGroupRoots(backupDir, date, backupsnapshot.CatalogName(catalog), group, entry.Nodes)
}

// collectEntrySrcGroupRoots returns `<backupDir>/<node>/<date>/<catalogDir>/<group>` directories.
func collectEntrySrcGroupRoots(backupDir, date, catalogDir, group string, nodes []string) []string {
	var roots []string
	for _, node := range nodes {
		if node == "" {
			continue
		}
		candidate := filepath.Join(backupDir, node, date, catalogDir, group)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			roots = append(roots, candidate)
		}
	}
	sort.Strings(roots)
	return roots
}

// CollectAllSrcGroupRoots merges every entry's source roots for one group,
// deduplicated by path, so the per-group union sidx is built once from all
// sources.
func (p *CopyPlan) CollectAllSrcGroupRoots(catalog commonv1.Catalog, group string) []string {
	seen := make(map[string]struct{})
	var roots []string
	for _, entry := range p.ResolvedEntries() {
		for _, r := range p.ResolveEntrySrcRoots(catalog, entry, group) {
			if _, dup := seen[r]; dup {
				continue
			}
			seen[r] = struct{}{}
			roots = append(roots, r)
		}
	}
	sort.Strings(roots)
	return roots
}
