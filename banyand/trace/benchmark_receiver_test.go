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

package trace_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestBenchmarkPartReceiverReceiptReopenAndQueries(t *testing.T) {
	root := t.TempDir()
	fileSystem := fs.NewLocalFileSystem()
	rows := []storagetrace.PartEncoderRow{
		{TraceID: "trace-a", SpanID: "span-a", Span: []byte("a"), Timestamp: 100, Tags: []storagetrace.PartEncoderTag{
			{Name: "start_time", Value: storagetrace.EncodedTimestamp(100)}, {Name: "latency", Value: int64(10)},
		}},
		{TraceID: "trace-b", SpanID: "span-b", Span: []byte("b"), Timestamp: 200, Tags: []storagetrace.PartEncoderTag{
			{Name: "start_time", Value: storagetrace.EncodedTimestamp(200)}, {Name: "latency", Value: int64(20)},
		}},
	}
	corePath, releaseCore := storagetrace.EncodePart(filepath.Join(root, "sender-core"), fileSystem, 1, rows)
	t.Cleanup(releaseCore)
	indexPaths := make(map[string]string, 2)
	for _, indexName := range []string{"latency", "start_time"} {
		indexRoot := filepath.Join(root, "sender-sidx", indexName)
		options, optionsErr := sidx.NewOptions(indexRoot, protector.Nop{})
		require.NoError(t, optionsErr)
		instance, instanceErr := sidx.NewSIDX(fileSystem, options)
		require.NoError(t, instanceErr)
		requests := make([]sidx.WriteRequest, 0, len(rows))
		for rowIdx := range rows {
			key := int64((rowIdx + 1) * 10)
			if indexName == "start_time" {
				key = rows[rowIdx].Timestamp
			}
			requests = append(requests, sidx.WriteRequest{
				SeriesID: common.SeriesID(1), Key: key, Data: append([]byte{1}, rows[rowIdx].TraceID...),
			})
		}
		memoryPart, convertErr := instance.ConvertToMemPart(requests, 0, nil, nil)
		require.NoError(t, convertErr)
		partPath := filepath.Join(indexRoot, fmt.Sprintf("%016x", 1))
		memoryPart.MustFlush(fileSystem, partPath)
		sidx.ReleaseMemPart(memoryPart)
		require.NoError(t, instance.Close())
		indexPaths[indexName] = partPath
	}
	receiver, receiverErr := storagetrace.NewBenchmarkPartReceiver(filepath.Join(root, "receiver"))
	require.NoError(t, receiverErr)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	require.NoError(t, receiver.Receive(context.Background(), corePath, indexPaths))
	require.NoError(t, receiver.Reopen())
	results, queryErr := receiver.QueryTrace(context.Background(), "trace-a", 0, 1_000, map[string]pbv1.ValueType{
		"start_time": pbv1.ValueTypeTimestamp, "latency": pbv1.ValueTypeInt64,
	})
	require.NoError(t, queryErr)
	require.Len(t, results, 1)
	require.Equal(t, []string{"span-a"}, results[0].SpanIDs)
	for _, indexName := range []string{"latency", "start_time"} {
		rowCount := 0
		scanErr := receiver.ScanRawIndex(context.Background(), indexName, func(sidx.RawRow) error {
			rowCount++
			return nil
		})
		require.NoError(t, scanErr)
		require.Equal(t, 2, rowCount)
	}
	consolidated, consolidateErr := receiver.ConsolidatedCompressedSizes(context.Background(), 1)
	require.NoError(t, consolidateErr)
	require.Positive(t, consolidated.Core)
	require.Positive(t, consolidated.Indexes["latency"])
	require.Positive(t, consolidated.Indexes["start_time"])
	results, queryErr = receiver.QueryTrace(context.Background(), "trace-a", 0, 1_000, map[string]pbv1.ValueType{
		"start_time": pbv1.ValueTypeTimestamp, "latency": pbv1.ValueTypeInt64,
	})
	require.NoError(t, queryErr)
	require.Len(t, results, 1)
}
