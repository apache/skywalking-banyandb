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

package accesslog

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const testTemplate = "access-%s.log"

func initTestLogger(t *testing.T) {
	t.Helper()
	err := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, err)
}

func newFileLogForTest(t *testing.T, root string, rotationInterval time.Duration, sampled bool) Log {
	t.Helper()
	initTestLogger(t)
	l := logger.GetLogger("test", "accesslog")
	flog, err := NewFileLog(root, testTemplate, rotationInterval, l, sampled)
	require.NoError(t, err)
	return flog
}

func listLogFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out
}

func countLines(t *testing.T, filePath string) int {
	t.Helper()
	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer f.Close()
	s := bufio.NewScanner(f)
	lines := 0
	for s.Scan() {
		if len(s.Bytes()) == 0 {
			continue
		}
		lines++
	}
	require.NoError(t, s.Err())
	return lines
}

func totalLinesInDir(t *testing.T, dir string) int {
	t.Helper()
	total := 0
	for _, f := range listLogFiles(t, dir) {
		total += countLines(t, f)
	}
	return total
}

func writeMessages(t *testing.T, l Log, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		// Use a simple well-known protobuf message
		msg := wrapperspb.String("msg")
		require.NoError(t, l.Write(msg))
	}
}

func waitForLines(t *testing.T, dir string, want int, within time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		return totalLinesInDir(t, dir) == want
	}, within, 20*time.Millisecond)
}

func TestFileLog_SimpleSampled(t *testing.T) {
	dir := t.TempDir()
	flog := newFileLogForTest(t, dir, 10*time.Second, true)
	defer flog.Close()

	writeMessages(t, flog, 10)

	// Rely on periodic flush (DefaultFlushInterval)
	waitForLines(t, dir, 10, 3*time.Second)
}

func TestFileLog_Unsampled(t *testing.T) {
	dir := t.TempDir()
	flog := newFileLogForTest(t, dir, 10*time.Second, false)
	defer flog.Close()

	writeMessages(t, flog, 10)
	waitForLines(t, dir, 10, 3*time.Second)
}

func TestFileLog_FlushBatchOnThreshold(t *testing.T) {
	dir := t.TempDir()
	flog := newFileLogForTest(t, dir, 10*time.Second, true)
	defer flog.Close()

	// Write exactly one batch; should flush immediately without waiting for flush ticker
	writeMessages(t, flog, DefaultBatchSize)

	// Expect lines to appear quickly since flush occurs on reaching batch size
	require.Eventually(t, func() bool {
		files := listLogFiles(t, dir)
		if len(files) == 0 {
			return false
		}
		return countLines(t, files[0]) == DefaultBatchSize
	}, 1*time.Second, 20*time.Millisecond)
}

func TestFileLog_Rotation(t *testing.T) {
	dir := t.TempDir()
	// Use 1s rotation interval; we'll manually invoke rotation after crossing second boundary
	flog := newFileLogForTest(t, dir, 1*time.Second, true)
	defer flog.Close()

	// Write a full batch to force immediate flush
	writeMessages(t, flog, DefaultBatchSize)
	require.Eventually(t, func() bool {
		return totalLinesInDir(t, dir) == DefaultBatchSize
	}, 2*time.Second, 20*time.Millisecond)

	// Ensure we cross a second boundary to get a new timestamped filename, then rotate explicitly
	time.Sleep(1100 * time.Millisecond)

	// Write more after rotation (again full batch to force flush)
	writeMessages(t, flog, DefaultBatchSize)

	// Eventually we should have lines across at least 2 files
	require.Eventually(t, func() bool {
		return len(listLogFiles(t, dir)) >= 2
	}, 3*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		volume := totalLinesInDir(t, dir)
		t.Log("volume", volume)
		return volume == 2*DefaultBatchSize
	}, 3*time.Second, 20*time.Millisecond)
}
