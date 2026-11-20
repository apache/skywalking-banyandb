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

package importer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// counted sums what the tests check of a view.
type counted struct {
	complete, started, inflightStreamParts int
	sentWindow, pushed, duplicates         uint64
}

func countView(t *testing.T, path string) (*ProgressView, counted) {
	t.Helper()
	v, err := ReadProgress(path)
	require.NoError(t, err)
	var c counted
	for _, it := range v.Done {
		if it.Complete {
			c.complete++
		}
		if it.Started {
			c.started++
			c.sentWindow += it.SentWindow
		}
	}
	for _, in := range v.Inflight {
		if in.Part && (strings.HasPrefix(in.Key, "stream/") || strings.HasPrefix(in.Key, "trace/")) {
			c.inflightStreamParts++
		}
	}
	for _, r := range v.Pushed {
		c.pushed += r.Rows
	}
	for _, r := range v.Duplicates {
		c.duplicates += r.Rows
	}
	return v, c
}

func TestReadProgress_MissingFileIsNotExist(t *testing.T) {
	_, err := ReadProgress(filepath.Join(t.TempDir(), "none.json"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// A progress file a resume refuses as unusable, such as one with a null record, is an error, not
// a panic.
func TestReadProgress_UnusableFileIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	state.Done = map[string]*itemRecord{"stream/g/hot/s/shard-0": nil}
	require.NoError(t, resume.WriteFile(path, state))
	_, err := ReadProgress(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is unusable")
}

// The progress file, its cursor side file and its plan the real tracker writes read back: interrupted
// with a CSV window sent and not acknowledged, and once the resume finished the import.
func TestReadProgress_CSV(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	path := ProgressPath(opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The first stream batch is sent and the files are frozen before its acknowledgement: its
	// rows are the sent window.
	crash, restore := crashAt(t, 1, cancel)
	f.setAckGate(func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 1 {
			crash("batch frozen")
		}
	})
	first := runImportEnv(ctx, f, li, opts, nil, nil)
	require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)
	f.setAckGate(nil)
	restore()
	require.FileExists(t, resume.SidePath(path, cursorSide), "the cursor side file of the interrupted run")
	cursors, err := os.ReadFile(resume.SidePath(path, cursorSide))
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(cursors), "{\n  \"") && strings.HasSuffix(string(cursors), "}\n"), "the cursor side file is indented JSON: %s", cursors)

	v, c := countView(t, path)
	assert.Nil(t, v.FinishedAt)
	assert.NotEmpty(t, v.Plan, "the plan of its side file")
	assert.NotEmpty(t, v.Liaisons)
	assert.NotEmpty(t, v.TargetNodes)
	assert.Equal(t, 1, c.started, "the stream item is started")
	assert.Positive(t, c.sentWindow, "the stream batch is sent and not acknowledged, as the cursor side file says")
	assert.Zero(t, c.duplicates)

	opts.Resume, opts.DuplicationPolicy = true, config.DuplicationWarn
	second := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, second.err, second.log)
	assert.NoFileExists(t, resume.SidePath(path, cursorSide))
	v, done := countView(t, path)
	require.NotNil(t, v.FinishedAt, "the resume finished the import")
	assert.Equal(t, 3, done.complete)
	assert.Equal(t, len(v.Plan), done.complete)
	assert.Zero(t, done.sentWindow)
	assert.Equal(t, c.sentWindow, done.duplicates, "the window pushed again is the duplicates")
	assert.Equal(t, uint64(7), done.pushed, "the fixture's rows: the window was not acknowledged before, so it counts once")
}

// The progress file the real tracker writes for a native import reads back: a stream part in
// flight, and once a resume pushed it again on one node (resumeDuplicationPolicy warn) its rows as
// duplicates.
func TestReadProgress_Native(t *testing.T) {
	shortenPartRetries(t)
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	crashOn(s, nativefmt.KindPart, cancel, "d1", "d2")
	_, err := nativeRun(ctx, t, li, path, false, "")
	require.Error(t, err)
	_, c := countView(t, path)
	assert.Equal(t, 1, c.inflightStreamParts, "the part is in flight")

	// Foreign rows on d2 leave the part undecidable there: warn pushes it again on d2.
	s.set(nil, map[string]int64{"d2": 2})
	log, err := nativeRun(context.Background(), t, li, path, true, config.DuplicationWarn)
	require.NoError(t, err, log)
	v, done := countView(t, path)
	assert.Equal(t, c.complete+1, done.complete, "the stream item is complete now")
	for key, it := range v.Done {
		assert.True(t, it.Complete, key)
	}
	assert.Zero(t, done.inflightStreamParts)
	assert.Equal(t, uint64(4), done.duplicates, "the part's 4 rows pushed again on d2")
	for _, r := range v.Duplicates {
		assert.Equal(t, "stream", r.Catalog)
	}
}
