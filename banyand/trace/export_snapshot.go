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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// exportSnapshotAttempts bounds how often an export snapshot is retaken when a flush or
// merge between the core and the secondary-index snapshots leaves them inconsistent
// (design §3.5: the trace snapshot has no publication fence).
const exportSnapshotAttempts = 3

// takeConsistentSnapshot runs take into dst until verifyExportSnapshot accepts the result,
// at most exportSnapshotAttempts times. dst is emptied before every retake.
func takeConsistentSnapshot(dst string, l *logger.Logger, take func() error) error {
	for attempt := 1; ; attempt++ {
		if err := take(); err != nil {
			return err
		}
		verifyErr := verifyExportSnapshot(dst)
		if verifyErr == nil {
			return nil
		}
		if attempt >= exportSnapshotAttempts {
			return verifyErr
		}
		l.Warn().Err(verifyErr).Int("attempt", attempt).Msg("trace export snapshot inconsistent; retaking")
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
		if err := os.MkdirAll(dst, storage.DirPerm); err != nil {
			return err
		}
	}
}

// verifyExportSnapshot checks that every secondary-index part under
// <root>/<group>/seg-*/shard-*/sidx/<rule> has a core part with the same id in its shard.
func verifyExportSnapshot(root string) error {
	groups, err := readSubDirs(root, "")
	if err != nil {
		return err
	}
	for _, group := range groups {
		segs, segErr := readSubDirs(group, "seg-")
		if segErr != nil {
			return segErr
		}
		for _, seg := range segs {
			shards, shardErr := readSubDirs(seg, "shard-")
			if shardErr != nil {
				return shardErr
			}
			for _, shard := range shards {
				if err = verifyExportShard(shard); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func verifyExportShard(shard string) error {
	coreIDs, err := dump.DiscoverPartIDs(shard)
	if err != nil {
		return err
	}
	core := make(map[uint64]struct{}, len(coreIDs))
	for _, id := range coreIDs {
		core[id] = struct{}{}
	}
	rules, err := readSubDirs(filepath.Join(shard, sidxDirName), "")
	if err != nil {
		return err
	}
	for _, rule := range rules {
		sidxIDs, idErr := dump.DiscoverPartIDs(rule)
		if idErr != nil {
			return idErr
		}
		for _, id := range sidxIDs {
			if _, ok := core[id]; !ok {
				return fmt.Errorf("trace snapshot %s: sidx %s part %016x has no core part", shard, filepath.Base(rule), id)
			}
		}
	}
	return nil
}

// readSubDirs returns the paths of the directories under dir whose names start with
// prefix; a missing dir has none.
func readSubDirs(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out, nil
}
