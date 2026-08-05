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
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BuildElementsFromBatch materializes []*streamv1.Element from a single columnar
// batch, honoring the batch Selection (post-pipeline batches carry one). The
// Element shape is byte-identical to the row path's BuildElementsFromStreamResult:
// hex-encoded elementID, timestamppb timestamp, and tag families/tags in
// projection order with NullTagValue for a missing projected tag.
//
// This is the single shared columnar→proto egress used by both the data-node
// standalone path (banyand/stream) and the liaison distributed merge
// (pkg/query/logical/stream), so the two cannot diverge and neither triggers an
// import cycle with the other.
func BuildElementsFromBatch(batch *vectorized.RecordBatch,
	projectionTags []model.TagProjection,
) ([]*streamv1.Element, error) {
	if batch == nil || batch.ActiveLen() == 0 {
		return nil, nil
	}
	schema := batch.Schema
	tsIdx, elemIdx := schema.TimestampIndex(), schema.ElementIDIndex()
	if tsIdx < 0 || elemIdx < 0 {
		return nil, fmt.Errorf("BuildElementsFromBatch: batch schema missing timestamp/elementID column")
	}
	tsCol := batch.Columns[tsIdx].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[elemIdx].(*vectorized.TypedColumn[int64])

	elements := make([]*streamv1.Element, 0, batch.ActiveLen())
	for active := 0; active < batch.ActiveLen(); active++ {
		row := active
		if batch.Selection != nil {
			row = int(batch.Selection[active])
		}
		elementID := ColumnToElementID(elemCol.Data()[row])
		e := &streamv1.Element{
			Timestamp: timestamppb.New(time.Unix(0, tsCol.Data()[row])),
			ElementId: hex.EncodeToString(convert.Uint64ToBytes(elementID)),
		}
		for _, proj := range projectionTags {
			tagFamily := &modelv1.TagFamily{Name: proj.Family}
			e.TagFamilies = append(e.TagFamilies, tagFamily)
			for _, tagName := range proj.Names {
				// A column carries nullness in its validity bitmap, which is
				// independent of the stored cell. AppendColumnRange appends the source
				// value unconditionally and only then marks the destination row null,
				// so a null row can retain a non-nil pointer — reading Data() alone
				// would emit that stale value as if it were real. The bitmap is the
				// source of truth; the nil check stays as a guard for producers that
				// leave a cell empty without marking it.
				var tagValue *modelv1.TagValue
				if colIdx, ok := schema.TagIndex(proj.Family, tagName); ok {
					tagCol := batch.Columns[colIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
					if !tagCol.IsNull(row) {
						tagValue = tagCol.Data()[row]
					}
				}
				if tagValue == nil {
					tagValue = pbv1.NullTagValue
				}
				tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
					Key:   tagName,
					Value: tagValue,
				})
			}
		}
		elements = append(elements, e)
	}
	return elements, nil
}

// BuildElementsFromBatches concatenates BuildElementsFromBatch over a slice of
// batches, preserving batch order and each batch's Selection.
func BuildElementsFromBatches(batches []*vectorized.RecordBatch,
	projectionTags []model.TagProjection,
) ([]*streamv1.Element, error) {
	var elements []*streamv1.Element
	for _, batch := range batches {
		batchElements, buildErr := BuildElementsFromBatch(batch, projectionTags)
		if buildErr != nil {
			return nil, buildErr
		}
		elements = append(elements, batchElements...)
	}
	return elements, nil
}
