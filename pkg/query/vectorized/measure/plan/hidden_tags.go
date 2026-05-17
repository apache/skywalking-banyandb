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

package plan

import (
	"context"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// augmentRequestWithHiddenTags returns a request whose TagProjection also
// carries the hidden criteria tags (grouped by family) so the vec Scan
// materializes them as columns. Storage needs the hidden tag values to
// evaluate criteria conditions on non-indexed tags; the row path performs
// the same projection extension in unresolvedIndexScan.Analyze before
// stripping the hidden tags at egress.
//
// The original request is left untouched: a shallow proto clone is taken
// and only its TagProjection is replaced. Hidden families are appended
// AFTER the visible projection families so the visible tags retain their
// projected order. BatchSchema coalesces same-named families into one
// TagFamilyGroup, so a hidden tag whose family is already projected lands
// in that family's group after the visible columns — and is removed again
// by the egress strip, leaving the wire bytes identical to a query with
// no hidden tags.
func augmentRequestWithHiddenTags(req *measurev1.QueryRequest, extras [][]*logical.Tag) *measurev1.QueryRequest {
	if len(extras) == 0 {
		return req
	}
	families := make([]*modelv1.TagProjection_TagFamily, 0)
	if tp := req.GetTagProjection(); tp != nil {
		families = append(families, tp.GetTagFamilies()...)
	}
	for _, group := range extras {
		if len(group) == 0 {
			continue
		}
		names := make([]string, 0, len(group))
		for _, t := range group {
			names = append(names, t.GetTagName())
		}
		families = append(families, &modelv1.TagProjection_TagFamily{
			Name: group[0].GetFamilyName(),
			Tags: names,
		})
	}
	cloned := proto.Clone(req).(*measurev1.QueryRequest)
	cloned.TagProjection = &modelv1.TagProjection{TagFamilies: families}
	return cloned
}

// hiddenTagsMIterator wraps an MIterator and strips hidden criteria tags
// from each Current() result. It is the vec counterpart of the row path's
// hiddenTagsMIterator (pkg/query/logical/measure): the hidden tags were
// projected so storage could filter on them, and are removed here so the
// serialized DataPoints carry only the requested projection — keeping the
// wire bytes byte-identical to a query without hidden criteria tags.
type hiddenTagsMIterator struct {
	inner      executor.MIterator
	hiddenTags logical.HiddenTagSet
}

func (h *hiddenTagsMIterator) Next() bool { return h.inner.Next() }

func (h *hiddenTagsMIterator) Current() []*measurev1.InternalDataPoint {
	dps := h.inner.Current()
	for _, dp := range dps {
		if dp == nil || dp.DataPoint == nil {
			continue
		}
		dp.DataPoint.TagFamilies = h.hiddenTags.StripHiddenTags(dp.DataPoint.TagFamilies)
	}
	return dps
}

func (h *hiddenTagsMIterator) Close() error { return h.inner.Close() }

// EmitFrame implements vmeasure.FrameEmitter. Draining via the wrapper's
// own Next / Current keeps the hidden-tag strip as the single source of
// truth — Current already removes the hidden criteria tags from each
// emitted DataPoint, so the reverse-serialised RecordBatch carries only
// the projected columns. The columnar wire then matches what a query
// without hidden criteria tags would have emitted, byte-identical to
// the row path.
func (h *hiddenTagsMIterator) EmitFrame(_ context.Context) ([]byte, error) {
	var idps []*measurev1.InternalDataPoint
	for h.Next() {
		current := h.Current()
		for _, dp := range current {
			if dp != nil {
				idps = append(idps, dp)
			}
		}
	}
	return vmeasure.SerializeDataPointsToFrame(idps)
}
