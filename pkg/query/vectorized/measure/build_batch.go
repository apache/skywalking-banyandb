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

package measure

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BuildMeasureBatchFromResult converts a *model.MeasureResult produced by a
// row-shaped MeasureQueryResult.Pull() call into a *model.MeasureBatch whose
// columns match the supplied BatchSchema.
//
// This is the G5b "dual-emit" wrapper helper: the storage layer can
// implement MeasureBatchResult.PullBatch by calling its existing Pull() and
// passing the result through this converter. It preserves the existing
// row-path decode pipeline; the architectural decode-elimination is left to
// G5c/G5d (block_cursor native column emit).
//
// Schema-declared tag/field columns missing from the result are null-filled
// using pbv1.Null{Tag,Field}Value singletons — matching the multi-group
// projection behavior in fillTags / fillFields when one group's schema
// lacks a tag the other has.
//
// Returns (nil, nil) when r is nil. Returns (nil, err) when a length
// invariant is violated (a value slice is shorter than the timestamp count).
func BuildMeasureBatchFromResult(r *model.MeasureResult, schema *vectorized.BatchSchema) (*model.MeasureBatch, error) {
	if r == nil {
		return nil, nil
	}
	if schema == nil {
		return nil, fmt.Errorf("BuildMeasureBatchFromResult: nil schema")
	}
	n := len(r.Timestamps)

	timestamps := make([]int64, n)
	copy(timestamps, r.Timestamps)
	versions := make([]int64, n)
	if len(r.Versions) >= n {
		copy(versions, r.Versions[:n])
	}
	shardIDs := make([]common.ShardID, n)
	if len(r.ShardIDs) >= n {
		copy(shardIDs, r.ShardIDs[:n])
	}
	seriesIDs := make([]common.SeriesID, n)
	for i := range seriesIDs {
		seriesIDs[i] = r.SID
	}

	resultTags := make(map[string]*model.Tag)
	for i := range r.TagFamilies {
		tf := &r.TagFamilies[i]
		for j := range tf.Tags {
			tag := &tf.Tags[j]
			resultTags[tf.Name+"\x00"+tag.Name] = tag
		}
	}
	resultFields := make(map[string]*model.Field, len(r.Fields))
	for i := range r.Fields {
		resultFields[r.Fields[i].Name] = &r.Fields[i]
	}

	tagCols := make([]vectorized.Column, 0, len(schema.Columns))
	fieldCols := make([]vectorized.Column, 0, len(schema.Columns))
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			col := vectorized.NewTagValueColumn(n)
			tag, present := resultTags[def.TagFamily+"\x00"+def.Name]
			if !present {
				for range n {
					col.Append(pbv1.NullTagValue)
				}
			} else {
				if len(tag.Values) < n {
					return nil, fmt.Errorf("BuildMeasureBatchFromResult: tag %s.%s has %d values, expected %d",
						def.TagFamily, def.Name, len(tag.Values), n)
				}
				for k := range n {
					col.Append(tag.Values[k])
				}
			}
			tagCols = append(tagCols, col)
		case vectorized.RoleField:
			col := vectorized.NewFieldValueColumn(n)
			fld, present := resultFields[def.Name]
			if !present {
				for range n {
					col.Append(pbv1.NullFieldValue)
				}
			} else {
				if len(fld.Values) < n {
					return nil, fmt.Errorf("BuildMeasureBatchFromResult: field %s has %d values, expected %d",
						def.Name, len(fld.Values), n)
				}
				for k := range n {
					col.Append(fld.Values[k])
				}
			}
			fieldCols = append(fieldCols, col)
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID:
			// Metadata roles are populated via the parallel slices on the
			// MeasureBatch itself; no per-column entry is needed.
		}
	}

	return &model.MeasureBatch{
		Schema:     schema,
		Timestamps: timestamps,
		Versions:   versions,
		ShardIDs:   shardIDs,
		SeriesIDs:  seriesIDs,
		Tags:       tagCols,
		Fields:     fieldCols,
	}, nil
}
