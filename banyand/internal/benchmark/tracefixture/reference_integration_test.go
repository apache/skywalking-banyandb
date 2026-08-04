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

package tracefixture

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
)

func TestReferenceSourcePlan(t *testing.T) {
	sourcePath := os.Getenv("BANYANDB_TRACE_FIXTURE_SOURCE")
	catalogPath := os.Getenv("BANYANDB_TRACE_FIXTURE_CATALOG")
	if sourcePath == "" || catalogPath == "" {
		t.Skip("reference source and catalog are not configured")
	}
	source, loadErr := LoadSource(context.Background(), LoadOptions{
		SourcePath: sourcePath, CatalogPath: catalogPath, Format: dumptrace.PartFormatLegacy,
	})
	require.NoError(t, loadErr)
	plan, planErr := BuildSourcePlan(source, Options{
		DayStart: time.Unix(1_767_225_600, 0).UTC(), DayDuration: 24 * time.Hour, Shapes: DefaultShapes(), CopyCount: CopyTraceCount,
	})
	require.NoError(t, planErr)
	require.Len(t, plan.Instances, GeneratedTraceCount)
	require.Len(t, plan.Writes, 1_610)
	partialTails := 0
	for writeIdx := range plan.Writes {
		write := &plan.Writes[writeIdx]
		seen := make(map[string]struct{}, len(write.Fragments))
		for fragmentIdx := range write.Fragments {
			traceID := write.Fragments[fragmentIdx].GeneratedTraceID
			_, exists := seen[traceID]
			require.False(t, exists)
			seen[traceID] = struct{}{}
		}
		if write.PartialTail {
			partialTails++
		}
	}
	require.Equal(t, 1, partialTails)
}
