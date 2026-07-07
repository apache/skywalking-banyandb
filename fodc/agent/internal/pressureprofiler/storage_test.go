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

package pressureprofiler

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeEvent creates a complete capture event with one heap profile of the given size.
func writeEvent(t *testing.T, s *store, profileID string, rss uint64, size int) {
	t.Helper()
	dir := s.eventDir(profileID)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	dest := filepath.Join(dir, "heap.pprof")
	require.NoError(t, os.WriteFile(dest, make([]byte, size), 0o600))
	require.NoError(t, s.finalize(profileID, meta{
		CapturedAt: "2026-06-27T10:11:12.000000000Z",
		RSSBytes:   rss,
		Profiles: []ProfileInfo{
			{Type: "heap", Filename: "heap.pprof", Filepath: dest, Format: "pprof", SizeBytes: int64(size)},
		},
	}))
}

func profileIDs(records []ProfileRecord) map[string]bool {
	out := make(map[string]bool, len(records))
	for _, r := range records {
		out[r.ProfileID] = true
	}
	return out
}

// TestRetentionByArtifactCount keeps the highest-RSS events within the count bound.
func TestRetentionByArtifactCount(t *testing.T) {
	s := newStore(t.TempDir(), 2, 0, nil)
	writeEvent(t, s, "evt-rss-100", 100, 10)
	writeEvent(t, s, "evt-rss-300", 300, 10)
	writeEvent(t, s, "evt-rss-200", 200, 10)

	kept := profileIDs(s.list())
	assert.Len(t, kept, 2)
	assert.True(t, kept["evt-rss-300"])
	assert.True(t, kept["evt-rss-200"])
	assert.False(t, kept["evt-rss-100"], "lowest-RSS event must be evicted")
}

// TestRetentionByDiskBound evicts the lowest-RSS events until the disk bound holds.
func TestRetentionByDiskBound(t *testing.T) {
	// 1000-byte disk cap; each event's profile is 400 bytes, so only 2 fit.
	s := newStore(t.TempDir(), 100, 1000, nil)
	writeEvent(t, s, "evt-rss-100", 100, 400)
	writeEvent(t, s, "evt-rss-300", 300, 400)
	writeEvent(t, s, "evt-rss-200", 200, 400)

	kept := profileIDs(s.list())
	assert.Len(t, kept, 2)
	assert.True(t, kept["evt-rss-300"])
	assert.True(t, kept["evt-rss-200"])
}

// TestListIgnoresIncompleteEvents only counts directories that have a meta.json.
func TestListIgnoresIncompleteEvents(t *testing.T) {
	s := newStore(t.TempDir(), 16, 0, nil)
	writeEvent(t, s, "complete", 100, 10)
	// An in-progress directory without meta.json must not be listed.
	require.NoError(t, os.MkdirAll(s.eventDir("incomplete"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(s.eventDir("incomplete"), "heap.pprof"), []byte("x"), 0o600))

	kept := profileIDs(s.list())
	assert.Len(t, kept, 1)
	assert.True(t, kept["complete"])
}

// TestOpenRejectsPathTraversal refuses to open files outside the storage directory.
func TestOpenRejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	s := newStore(dir, 16, 0, nil)
	writeEvent(t, s, "evt", 100, 8)

	f, err := s.open(filepath.Join(dir, "evt", "heap.pprof"))
	require.NoError(t, err)
	data, _ := io.ReadAll(f)
	require.NoError(t, f.Close())
	assert.Len(t, data, 8)

	_, err = s.open(filepath.Join(dir, "..", "escape.pprof"))
	assert.Error(t, err)
	_, err = s.open("/etc/passwd")
	assert.Error(t, err)
}

// TestSelfCheckPasses succeeds for a writable directory.
func TestSelfCheckPasses(t *testing.T) {
	s := newStore(t.TempDir(), 16, 0, nil)
	require.NoError(t, s.selfCheck())
}

// TestSelfCheckRejectsUncreatableDir surfaces a directory that cannot be created. A path under
// a regular file fails with ENOTDIR regardless of uid, so this is robust even when tests run
// as root (where a chmod-based read-only directory would be bypassed).
func TestSelfCheckRejectsUncreatableDir(t *testing.T) {
	parent := t.TempDir()
	filePath := filepath.Join(parent, "not-a-dir")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o600))
	s := newStore(filepath.Join(filePath, "sub"), 16, 0, nil)
	assert.Error(t, s.selfCheck())
}
