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

package stream

import (
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BuildStreamBatchSchema builds the columnar batch schema for a stream query.
// Columns are laid out as Timestamp, ElementID, SeriesID, one passthrough tag
// column per projected tag (grouped by family), and — when an ordered tag is
// supplied — a trailing comparable-bytes order-key column.
func BuildStreamBatchSchema(tagProjection []model.TagProjection, orderTagFamily, orderTagName string) *vectorized.BatchSchema {
	cols := []vectorized.ColumnDef{
		{Name: StreamColumnNameTimestamp, Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: StreamColumnNameElementID, Role: vectorized.RoleElementID, Type: vectorized.ColumnTypeInt64},
		{Name: StreamColumnNameSeriesID, Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
	}
	for _, projection := range tagProjection {
		for _, name := range projection.Names {
			cols = append(cols, vectorized.ColumnDef{
				Name:      name,
				TagFamily: projection.Family,
				Role:      vectorized.RoleTag,
				Type:      vectorized.ColumnTypeTagValue,
			})
		}
	}
	if orderTagFamily != "" && orderTagName != "" {
		cols = append(cols, vectorized.ColumnDef{
			Name: StreamColumnNameOrderKey,
			Role: vectorized.RoleOrderKey,
			Type: vectorized.ColumnTypeBytes,
		})
	}
	return vectorized.NewBatchSchema(cols)
}

// ElementIDToColumn reinterprets a uint64 element id as the int64 stored in the
// ElementID column. This is an exact bit reinterpretation, not a value cast.
func ElementIDToColumn(id uint64) int64 { return int64(id) }

// ColumnToElementID reinterprets an int64 ElementID column value back to the
// original uint64 element id. This is an exact bit reinterpretation.
func ColumnToElementID(v int64) uint64 { return uint64(v) }

// SeriesIDToColumn reinterprets a uint64 series id as the int64 stored in the
// SeriesID column. This is an exact bit reinterpretation, not a value cast.
func SeriesIDToColumn(id uint64) int64 { return int64(id) }

// ColumnToSeriesID reinterprets an int64 SeriesID column value back to the
// original uint64 series id. This is an exact bit reinterpretation.
func ColumnToSeriesID(v int64) uint64 { return uint64(v) }
