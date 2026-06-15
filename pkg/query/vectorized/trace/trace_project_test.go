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

package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestProject_EmptyProjection(t *testing.T) {
	schema := NewPhase2Schema(nil)
	op := NewProject(schema)
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))
	require.Equal(t, schema, op.OutputSchema())

	batch := vectorized.NewRecordBatch(schema, 2)
	Phase2TraceIDs(batch).Append("trace-a")
	Phase2TraceIDs(batch).Append("trace-a")
	Phase2Keys(batch).Append(int64(1))
	Phase2Keys(batch).Append(int64(1))
	Phase2Spans(batch).Append([]byte("span1"))
	Phase2Spans(batch).Append([]byte("span2"))
	Phase2SpanIDs(batch).Append("s1")
	Phase2SpanIDs(batch).Append("s2")
	batch.Len = 2

	// Process is a no-op; must return nil
	processErr := op.Process(ctx, batch)
	require.NoError(t, processErr)

	// Batch should be unchanged
	require.Equal(t, 2, batch.Len)
	require.Equal(t, []string{"trace-a", "trace-a"}, Phase2TraceIDs(batch).Data())

	require.NoError(t, op.Close())
	// Idempotent close
	require.NoError(t, op.Close())
}

func TestProject_WithTags(t *testing.T) {
	schema := NewPhase2Schema([]string{"http.method", "status_code"})
	op := NewProject(schema)
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))

	batch := vectorized.NewRecordBatch(schema, 1)
	Phase2TraceIDs(batch).Append("trace-b")
	Phase2Keys(batch).Append(int64(42))
	Phase2Spans(batch).Append([]byte("spanData"))
	Phase2SpanIDs(batch).Append("sp1")
	Phase2TagCol(batch, 0).Append(pbv1.NullTagValue)
	Phase2TagCol(batch, 1).Append(&modelv1.TagValue{})
	batch.Len = 1

	processErr := op.Process(ctx, batch)
	require.NoError(t, processErr)

	// Batch unchanged — 2 tag columns still present
	require.Equal(t, 6, len(batch.Columns))
	require.Equal(t, 1, batch.Len)

	require.NoError(t, op.Close())
}
