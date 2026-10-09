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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// buildOneSeriesDoc builds a single, minimal index.Document for Insert.
func buildOneSeriesDoc(t *testing.T, subject string) index.Documents {
	t.Helper()
	var series pbv1.Series
	series.Subject = subject
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "s00"}}}}
	require.NoError(t, series.Marshal())
	return index.Documents{{EntityValues: append([]byte(nil), series.Buffer...), Timestamp: 1}}
}

// TestSeriesIndex_Insert_AsyncPersistFailureSurfacesOnClose is the L1
// regression. seriesIndex.batch used to always register a
// PersistentCallback, even in production's async mode
// (SeriesIndexFlushTimeoutSeconds > 0, s.wait == false). The owner only
// records a background persistence failure into the error Owner.Close()
// returns when a flush has NO pending callbacks (drainPersistence's
// len(callbacks) == 0 branch); an unconditional callback that nobody drains
// in the async case (Insert already returned) silently swallowed the
// failure instead, so admitted series were lost with no error anywhere and
// closeResourcesLocked never saw the failure.
//
// This drives the real production path -- newSeriesIndex's async mode and
// seriesIndex.Insert -- rather than calling owner.Batch directly (the
// adjacent TestCloseResourcesLocked_SeriesIndexCloseErrorDoesNotPanic's own
// comment admits it bypasses batch() this way, which is exactly why it
// could not have caught this bug). The failure is induced by replacing the
// sidx directory with a plain file after construction: admission is purely
// in-memory and still succeeds, but the later persist attempt (the async
// PersistInterval flush, or Close's own final flush) fails to recreate
// "sidx" as a directory ("not a directory").
func TestSeriesIndex_Insert_AsyncPersistFailureSurfacesOnClose(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	dir, fn := setUp(require.New(t))
	defer fn()

	// flushTimeoutSeconds > 0 selects async mode (s.wait == false), the
	// production default whenever SeriesIndexFlushTimeoutSeconds is
	// configured.
	si, err := newSeriesIndex(ctx, dir, 10, 0, nil, &testRootLease{})
	require.NoError(t, err)

	indexPath := filepath.Join(dir, seriesIndexDirName)
	require.NoError(t, os.RemoveAll(indexPath))
	require.NoError(t, os.WriteFile(indexPath, []byte("not a directory"), 0o600))

	require.NoError(t, si.Insert(buildOneSeriesDoc(t, testSubjectSvc)), "async admission is in-memory only and must still succeed")

	closeErr := si.Close()
	require.Error(t, closeErr, "a background persistence failure must surface through Close, not vanish silently")
}

// TestSegment_AsyncPersistFailureViaInsertMarksSegmentFailed is the
// segment-level half of the same L1 regression: closeResourcesLocked must
// observe the same Close() error (and so set CloseFailed) when it was
// produced through seriesIndex.Insert's real async admission path, not only
// through a raw owner.Batch call.
func TestSegment_AsyncPersistFailureViaInsertMarksSegmentFailed(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	dir, fn := setUp(require.New(t))
	defer fn()

	si, err := newSeriesIndex(ctx, dir, 10, 0, nil, &testRootLease{})
	require.NoError(t, err)

	indexPath := filepath.Join(dir, seriesIndexDirName)
	require.NoError(t, os.RemoveAll(indexPath))
	require.NoError(t, os.WriteFile(indexPath, []byte("not a directory"), 0o600))
	require.NoError(t, si.Insert(buildOneSeriesDoc(t, testSubjectSvc)))

	seg := &segment[*MockTSTable, any]{index: si, l: logger.GetLogger("test")}
	require.NotPanics(t, func() {
		seg.mu.Lock()
		seg.closeResourcesLocked()
		seg.mu.Unlock()
	})
	require.True(t, seg.CloseFailed(), "an async persist failure reached through Insert must mark the segment failed")
	require.Nil(t, seg.index)
}
