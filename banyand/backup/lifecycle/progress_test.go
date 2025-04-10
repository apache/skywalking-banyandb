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

package lifecycle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestProgress(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "progress-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	progressPath := filepath.Join(tmpDir, "progress.json")
	l := logger.GetLogger("test")

	t.Run("NewProgress", func(t *testing.T) {
		progress := NewProgress()
		assert.NotNil(t, progress)
		assert.Empty(t, progress.CompletedGroups)
		assert.Empty(t, progress.CompletedStreams)
		assert.Empty(t, progress.CompletedMeasures)
		assert.Empty(t, progress.DeletedStreamGroups)
		assert.Empty(t, progress.DeletedMeasureGroups)
	})

	t.Run("MarkAndCheckItems", func(t *testing.T) {
		progress := NewProgress()

		progress.MarkGroupCompleted("group1")
		assert.True(t, progress.IsGroupCompleted("group1"))
		assert.False(t, progress.IsGroupCompleted("group2"))

		progress.MarkStreamCompleted("group1", "stream1")
		assert.True(t, progress.IsStreamCompleted("group1", "stream1"))
		assert.False(t, progress.IsStreamCompleted("group1", "stream2"))
		assert.False(t, progress.IsStreamCompleted("group2", "stream1"))

		progress.MarkMeasureCompleted("group1", "measure1")
		assert.True(t, progress.IsMeasureCompleted("group1", "measure1"))
		assert.False(t, progress.IsMeasureCompleted("group1", "measure2"))
		assert.False(t, progress.IsMeasureCompleted("group2", "measure1"))

		progress.MarkStreamGroupDeleted("group1")
		assert.True(t, progress.IsStreamGroupDeleted("group1"))
		assert.False(t, progress.IsStreamGroupDeleted("group2"))

		progress.MarkMeasureGroupDeleted("group1")
		assert.True(t, progress.IsMeasureGroupDeleted("group1"))
		assert.False(t, progress.IsMeasureGroupDeleted("group2"))
	})

	t.Run("SaveAndLoad", func(t *testing.T) {
		progress := NewProgress()
		progress.MarkGroupCompleted("group1")
		progress.MarkStreamCompleted("group1", "stream1")
		progress.MarkMeasureCompleted("group1", "measure1")
		progress.MarkStreamGroupDeleted("group2")
		progress.MarkMeasureGroupDeleted("group2")

		progress.Save(progressPath, l)

		loaded := LoadProgress(progressPath, l)

		assert.True(t, loaded.IsGroupCompleted("group1"))
		assert.True(t, loaded.IsStreamCompleted("group1", "stream1"))
		assert.True(t, loaded.IsMeasureCompleted("group1", "measure1"))
		assert.True(t, loaded.IsStreamGroupDeleted("group2"))
		assert.True(t, loaded.IsMeasureGroupDeleted("group2"))

		assert.False(t, loaded.IsGroupCompleted("group3"))
		assert.False(t, loaded.IsStreamCompleted("group1", "stream2"))
		assert.False(t, loaded.IsMeasureCompleted("group1", "measure2"))
		assert.False(t, loaded.IsStreamGroupDeleted("group3"))
		assert.False(t, loaded.IsMeasureGroupDeleted("group3"))
	})

	t.Run("LoadNonExistent", func(t *testing.T) {
		nonExistentPath := filepath.Join(tmpDir, "nonexistent.json")
		progress := LoadProgress(nonExistentPath, l)
		assert.NotNil(t, progress)
		assert.Empty(t, progress.CompletedGroups)
	})

	t.Run("RemoveProgressFile", func(t *testing.T) {
		progress := NewProgress()
		progress.Save(progressPath, l)

		_, err := os.Stat(progressPath)
		assert.NoError(t, err)

		progress.Remove(progressPath, l)

		_, err = os.Stat(progressPath)
		assert.True(t, os.IsNotExist(err))
	})
}
