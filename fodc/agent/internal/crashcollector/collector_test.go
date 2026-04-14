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

package crashcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func TestCollectorPollOnceFetchesCollections(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/diagnostics/collections", request.URL.Path)
		writer.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(writer).Encode([]panicdiag.Collection{
			{
				ArtifactDir: "artifact-1",
				Files:       []string{"panic.json", "crash.txt"},
				Record: &panicdiag.PanicRecord{
					Component:  "measure",
					PanicValue: "boom",
				},
			},
		}))
	}))
	defer server.Close()

	collector := New(testLogger(t), Config{
		SourceEndpoints: []string{server.URL + "/metrics"},
	})

	require.NoError(t, collector.PollOnce(context.Background()))

	records := collector.ListCollections()
	require.Len(t, records, 1)
	assert.Equal(t, server.URL+"/metrics", records[0].SourceEndpoint)
	assert.Equal(t, "artifact-1", records[0].Collection.ArtifactDir)
	assert.Equal(t, "measure", records[0].Collection.Record.Component)
}

func TestCollectorMarshalCollections(t *testing.T) {
	t.Helper()

	collector := New(testLogger(t), Config{})
	collector.storeCollections("http://localhost:2121/metrics", []panicdiag.Collection{
		{
			ArtifactDir: "artifact-1",
			Files:       []string{"panic.json"},
		},
	})

	data, err := collector.MarshalCollections()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"sourceEndpoint":"http://localhost:2121/metrics"`)
	assert.Contains(t, string(data), `"artifactDir":"artifact-1"`)
}

func TestCollectorRingBufferKeepsNewestRecords(t *testing.T) {
	t.Helper()

	collector := New(testLogger(t), Config{BufferSize: 2})
	collector.storeCollections("http://localhost:2121/metrics", []panicdiag.Collection{
		{ArtifactDir: "artifact-1"},
	})
	collector.storeCollections("http://localhost:2121/metrics", []panicdiag.Collection{
		{ArtifactDir: "artifact-2"},
	})
	collector.storeCollections("http://localhost:2121/metrics", []panicdiag.Collection{
		{ArtifactDir: "artifact-3"},
	})

	records := collector.ListCollections()
	require.Len(t, records, 2)
	assert.Equal(t, "artifact-2", records[0].Collection.ArtifactDir)
	assert.Equal(t, "artifact-3", records[1].Collection.ArtifactDir)
}

func TestCollectorStoreCollectionsDeduplicates(t *testing.T) {
	t.Helper()

	collector := New(testLogger(t), Config{BufferSize: 4})
	collections := []panicdiag.Collection{
		{ArtifactDir: "artifact-1"},
	}
	collector.storeCollections("http://localhost:2121/metrics", collections)
	collector.storeCollections("http://localhost:2121/metrics", collections)

	records := collector.ListCollections()
	require.Len(t, records, 1)
	assert.Equal(t, "artifact-1", records[0].Collection.ArtifactDir)
}

func TestCollectorCapacitySizeBytesLimitsBuffer(t *testing.T) {
	t.Helper()

	collector := New(testLogger(t), Config{
		BufferSize:        100,
		CapacitySizeBytes: estimatedRecordBytes * 2,
	})
	for idx := 0; idx < 4; idx++ {
		collector.storeCollections("http://localhost:2121/metrics", []panicdiag.Collection{
			{ArtifactDir: fmt.Sprintf("artifact-%d", idx)},
		})
	}

	records := collector.ListCollections()
	require.Len(t, records, 2)
	assert.Equal(t, "artifact-2", records[0].Collection.ArtifactDir)
	assert.Equal(t, "artifact-3", records[1].Collection.ArtifactDir)
}

func testLogger(t *testing.T) *logger.Logger {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	if initErr != nil {
		require.NoError(t, initErr)
	}
	return logger.GetLogger("test", "crashcollector")
}
