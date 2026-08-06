// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package sidx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestScanRawVisitsEveryPhysicalRow(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	root := t.TempDir()
	options := createTestOptions(t)
	options.Path = root
	writer, writerErr := NewSIDX(fileSystem, options)
	require.NoError(t, writerErr)

	requests := []WriteRequest{
		{SeriesID: common.SeriesID(7), Key: 10, Data: []byte("same-trace"), Tags: []Tag{{Name: "status", Value: []byte("first"), ValueType: pbv1.ValueTypeStr}}},
		{SeriesID: common.SeriesID(7), Key: 20, Data: []byte("same-trace"), Tags: []Tag{{Name: "status", Value: []byte("second"), ValueType: pbv1.ValueTypeStr}}},
		{SeriesID: common.SeriesID(7), Key: 30, Data: []byte("same-trace"), Tags: []Tag{{Name: "status", Value: []byte("third"), ValueType: pbv1.ValueTypeStr}}},
	}
	memPart, convertErr := writer.ConvertToMemPart(requests, 0, nil, nil)
	require.NoError(t, convertErr)
	memPart.MustFlush(fileSystem, partPath(root, 1))
	ReleaseMemPart(memPart)
	require.NoError(t, writer.Close())

	readerOptions := createTestOptions(t)
	readerOptions.Path = root
	readerOptions.AvailablePartIDs = []uint64{1}
	reader, readerErr := NewSIDX(fileSystem, readerOptions)
	require.NoError(t, readerErr)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	var keys []int64
	var values []string
	scanErr := ScanRaw(context.Background(), reader, func(row RawRow) error {
		keys = append(keys, row.Key)
		require.Len(t, row.Tags, 1)
		values = append(values, string(row.Tags[0].Value))
		assert.Equal(t, uint64(1), row.PartID)
		assert.Equal(t, uint64(0), row.BlockID)
		assert.Equal(t, common.SeriesID(7), row.SeriesID)
		assert.Equal(t, "same-trace", string(row.Data))
		return nil
	})
	require.NoError(t, scanErr)
	assert.Equal(t, []int64{10, 20, 30}, keys)
	assert.Equal(t, []string{"first", "second", "third"}, values)

	keys = nil
	require.NoError(t, ScanRawParts(context.Background(), fileSystem, root, []uint64{1}, func(row RawRow) error {
		keys = append(keys, row.Key)
		return nil
	}))
	assert.Equal(t, []int64{10, 20, 30}, keys)
}
