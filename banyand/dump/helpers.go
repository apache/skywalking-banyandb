// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pkgdump "github.com/apache/skywalking-banyandb/pkg/dump"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	dirNameSidx = "sidx"
	dirNameMeta = "meta"
	logName     = "dump"
)

// DiscoverPartIDs returns the sorted part IDs found directly under shardPath.
func DiscoverPartIDs(shardPath string) ([]uint64, error) {
	entries, err := os.ReadDir(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard directory: %w", err)
	}

	var partIDs []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == dirNameSidx || name == dirNameMeta {
			continue
		}
		partID, parseErr := strconv.ParseUint(name, 16, 64)
		if parseErr == nil {
			partIDs = append(partIDs, partID)
		}
	}

	sort.Slice(partIDs, func(i, j int) bool {
		return partIDs[i] < partIDs[j]
	})

	return partIDs, nil
}

// LoadSegmentSeriesMap loads the segment-level SeriesID -> EntityValues map from
// the sidx inverted store under segmentPath. Requires a local filesystem path.
func LoadSegmentSeriesMap(segmentPath string) (map[common.SeriesID]string, error) {
	seriesIndexPath := filepath.Join(segmentPath, dirNameSidx)

	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   seriesIndexPath,
		Logger: logger.GetLogger(logName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open series index: %w", err)
	}
	defer store.Close()

	ctx := context.Background()
	iter, err := store.SeriesIterator(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create series iterator: %w", err)
	}
	defer iter.Close()

	seriesMap := make(map[common.SeriesID]string)
	for iter.Next() {
		series := iter.Val()
		if len(series.EntityValues) > 0 {
			seriesID := common.SeriesID(convert.Hash(series.EntityValues))
			seriesMap[seriesID] = string(series.EntityValues)
		}
	}

	return seriesMap, nil
}

// LoadPartSeriesMap reads the optional part-level smeta.bin under partPath and
// returns its SeriesID -> EntityValues map, or nil when smeta.bin is absent or
// cannot be parsed (the file is optional for backward compatibility).
func LoadPartSeriesMap(fileSystem fs.FileSystem, partPath string, id uint64) map[common.SeriesID][]byte {
	sm := pkgdump.TryOpenSeriesMetadata(fileSystem, partPath)
	if sm == nil {
		return nil
	}
	defer fs.MustClose(sm)

	tmp := make(map[uint64]map[common.SeriesID]string)
	if err := pkgdump.ParseSeriesMetadata(id, sm, tmp); err != nil {
		logger.GetLogger(logName).Warn().Err(err).Msg("failed to parse series metadata")
		return nil
	}
	m := tmp[id]
	if m == nil {
		return nil
	}
	seriesMap := make(map[common.SeriesID][]byte, len(m))
	for sid, ev := range m {
		seriesMap[sid] = []byte(ev)
	}
	return seriesMap
}
