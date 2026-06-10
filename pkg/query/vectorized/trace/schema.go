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

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

const (
	phase1ColumnKey = iota
	phase1ColumnSeriesID
	phase1ColumnPartID
	phase1ColumnPayload
	phase1ColumnTraceID
)

// Phase-1 column names.
const (
	Phase1ColumnNameKey      = "key"
	Phase1ColumnNameSeriesID = "seriesID"
	Phase1ColumnNamePartID   = "partID"
	Phase1ColumnNamePayload  = "payload"
	Phase1ColumnNameTraceID  = "traceID"
)

// NewPhase1Schema returns the thin trace-ID resolution batch schema.
func NewPhase1Schema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Name: Phase1ColumnNameKey, Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: Phase1ColumnNameSeriesID, Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Name: Phase1ColumnNamePartID, Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Name: Phase1ColumnNamePayload, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes},
		{Name: Phase1ColumnNameTraceID, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
	})
}

// Phase-2 column indices.
const (
	phase2ColumnTraceID = iota
	phase2ColumnKey
	phase2ColumnSpan
	phase2ColumnSpanID
	// phase2FixedColumnCount is the number of fixed (non-tag) Phase-2 columns;
	// dynamic tag columns start at this index.
	phase2FixedColumnCount
)

// Phase-2 column names.
const (
	Phase2ColumnNameTraceID = "traceID"
	Phase2ColumnNameKey     = "key"
	Phase2ColumnNameSpan    = "span"
	Phase2ColumnNameSpanID  = "spanID"
)

// NewPhase2Schema returns the span-materialization batch schema.
// tagCols are the identity-omitted projected tag names.
func NewPhase2Schema(tagCols []string) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{
		{Name: Phase2ColumnNameTraceID, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
		{Name: Phase2ColumnNameKey, Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: Phase2ColumnNameSpan, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes},
		{Name: Phase2ColumnNameSpanID, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeString},
	}
	for _, name := range tagCols {
		cols = append(cols, vectorized.ColumnDef{Name: name, Role: vectorized.RoleTag, Type: vectorized.ColumnTypeTagValue})
	}
	return vectorized.NewBatchSchema(cols)
}
