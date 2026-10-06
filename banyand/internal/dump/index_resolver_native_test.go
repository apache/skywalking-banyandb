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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

func TestIndexResolverUsesCommittedNativeGeneration(t *testing.T) {
	segmentPath := t.TempDir()
	fixturePath := filepath.Join("..", "..", "..", "pkg", "index", "inverted", "testdata", "nidx01c", "sourceA")
	require.NoError(t, os.CopyFS(filepath.Join(segmentPath, "sidx"), os.DirFS(fixturePath)))
	oracle, err := native.OpenReadOnlyGeneration(filepath.Join(segmentPath, "sidx"))
	require.NoError(t, err)
	var entities [][]byte
	require.NoError(t, oracle.VisitIdentifiers(context.Background(), func(identifier []byte) bool {
		entities = append(entities, identifier)
		return len(entities) < 2
	}))
	require.NoError(t, oracle.Close())
	require.GreaterOrEqual(t, len(entities), 2)
	entityA, entityB := entities[0], entities[1]

	resolver, err := NewIndexResolver(segmentPath, 8, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, resolver.Close()) }()

	resolved, err := resolver.Resolve(common.SeriesID(convert.Hash(entityA)), entityA)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	seriesIDs := map[common.SeriesID]struct{}{
		common.SeriesID(convert.Hash(entityA)): {},
		common.SeriesID(convert.Hash(entityB)): {},
	}
	seriesMap, err := resolver.PartSeriesMap(seriesIDs)
	require.NoError(t, err)
	require.Equal(t, map[common.SeriesID][]byte{
		common.SeriesID(convert.Hash(entityA)): entityA,
		common.SeriesID(convert.Hash(entityB)): entityB,
	}, seriesMap)
}
