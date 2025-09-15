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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestQueryLogEntry_WriteQuery(t *testing.T) {
	dir := t.TempDir()
	initTestLogger(t)
	l := logger.GetLogger("test", "accesslog")
	flog, err := NewFileLog(dir, "query-test-%s.log", 10*time.Second, l, false)
	require.NoError(t, err)
	defer flog.Close()

	// Test data
	startTime := time.Now()
	duration := 150 * time.Millisecond
	testErr := fmt.Errorf("test error")

	// Create realistic stream query request
	streamReq := &streamv1.QueryRequest{
		Groups: []string{"default"},
		Name:   "service_traffic",
		Limit:  100,
	}

	// Create realistic measure query request
	measureReq := &measurev1.QueryRequest{
		Groups: []string{"default"},
		Name:   "service_cpm",
		Limit:  50,
	}

	// Write query log with timing information for stream
	err = flog.WriteQuery("stream", startTime, duration, streamReq, testErr)
	require.NoError(t, err)

	// Write query log without error for measure
	err = flog.WriteQuery("measure", startTime.Add(time.Second), duration*2, measureReq, nil)
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(2 * time.Second)

	// Verify log files were created and contain query entries
	files := listLogFiles(t, dir)
	require.Len(t, files, 1)

	// Read and verify log content
	file, err := os.Open(files[0])
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		lines++

		// Parse the JSON to verify it contains timing information
		var entry map[string]interface{}
		err := json.Unmarshal([]byte(line), &entry)
		require.NoError(t, err)

		// Verify query log entry structure
		require.Contains(t, entry, "service")
		require.Contains(t, entry, "start_time")
		require.Contains(t, entry, "duration_ms")
		require.Contains(t, entry, "request")

		// Verify service is correct
		service := entry["service"].(string)
		require.True(t, service == "stream" || service == "measure")

		// Verify Request can be Unmarshal
		requestStr := entry["request"].(string)
		var msg proto.Message
		if service == "stream" {
			msg = &streamv1.QueryRequest{}
		} else {
			msg = &measurev1.QueryRequest{}
		}
		require.NoError(t, protojson.Unmarshal([]byte(requestStr), msg))

		// Verify duration is recorded
		durationMs := entry["duration_ms"]
		require.NotNil(t, durationMs)

		// Check error field for first entry (stream)
		if service == "stream" {
			require.Contains(t, entry, "error")
			require.Equal(t, "test error", entry["error"])
		}
	}

	require.NoError(t, scanner.Err())
	require.Equal(t, 2, lines, "Should have exactly 2 log entries")
}

func TestQueryLogEntry_Creation(t *testing.T) {
	startTime := time.Now()
	duration := 100 * time.Millisecond
	testErr := fmt.Errorf("test error")

	// Create realistic measure query request
	req := &measurev1.QueryRequest{
		Groups: []string{"default", "prod"},
		Name:   "service_throughput",
		Limit:  200,
	}

	// Test with error
	entry := NewQueryLogEntry("measure", startTime, duration, req, testErr)
	require.Equal(t, "measure", entry.Service)
	require.Equal(t, startTime, entry.StartTime)
	require.Equal(t, duration, entry.Duration)
	require.Equal(t, req, entry.Request)
	require.Equal(t, "test error", entry.Error)

	// Test without error
	entryNoError := NewQueryLogEntry("measure", startTime, duration, req, nil)
	require.Equal(t, "measure", entryNoError.Service)
	require.Equal(t, startTime, entryNoError.StartTime)
	require.Equal(t, duration, entryNoError.Duration)
	require.Equal(t, req, entryNoError.Request)
	require.Empty(t, entryNoError.Error)
}
