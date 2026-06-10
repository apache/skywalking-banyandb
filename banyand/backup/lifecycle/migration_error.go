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

package lifecycle

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Catalog names used in the migration report's structured errors.
const (
	catalogMeasure = "measure"
	catalogStream  = "stream"
	catalogTrace   = "trace"
)

// Scope names identifying which migration artifact an error refers to.
const (
	scopePart         = "part"
	scopeSeries       = "series"
	scopeElementIndex = "element_index"
	scopeShard        = "shard"
	scopeNode         = "node"
)

// MigrationError is a single structured migration error for the report. It
// carries the error location (group/segment/shard/part/node) as named fields plus
// the source/target stage and a human-readable segment directory name and
// segment interval. Shard/Part are pointers so a series- or
// node-scoped error omits them while a part-scoped error on shard 0 still emits
// "shard": 0.
type MigrationError struct {
	Shard       *uint32 `json:"shard,omitempty"`
	Part        *uint64 `json:"part,omitempty"`
	SourceStage string  `json:"source_stage"`
	TargetStage string  `json:"target_stage"`
	Group       string  `json:"group"`
	Catalog     string  `json:"catalog"`
	Scope       string  `json:"scope"`
	Segment     string  `json:"segment,omitempty"`
	Interval    string  `json:"interval,omitempty"`
	Node        string  `json:"node,omitempty"`
	Error       string  `json:"error"`
}

// locationKey identifies the migration artifact this error refers to. It
// excludes the message and stage so re-recording the same artifact (e.g. a part
// replayed to multiple target segments) overwrites the prior entry instead of
// accumulating a duplicate.
func (e MigrationError) locationKey() string {
	shard, part := "-", "-"
	if e.Shard != nil {
		shard = strconv.FormatUint(uint64(*e.Shard), 10)
	}
	if e.Part != nil {
		part = strconv.FormatUint(*e.Part, 10)
	}
	return strings.Join([]string{e.Catalog, e.Scope, e.Group, e.Segment, e.Node, shard, part}, "|")
}

// formatIntervalRule renders a segment interval as a compact "<num><unit>"
// string, e.g. "5d" or "1h", for the report's interval field.
func formatIntervalRule(r storage.IntervalRule) string {
	switch r.Unit {
	case storage.HOUR:
		return fmt.Sprintf("%dh", r.Num)
	case storage.DAY:
		return fmt.Sprintf("%dd", r.Num)
	default:
		return ""
	}
}

// segmentErrorLocation derives the source segment directory name (e.g.
// "seg-20260601") and its interval string ("5d") from a source segment time
// range and interval, for the report's segment/interval fields.
func segmentErrorLocation(segmentTR *timestamp.TimeRange, srcInterval storage.IntervalRule) (segment, interval string) {
	if segmentTR == nil {
		return "", ""
	}
	return "seg-" + storage.FormatSegmentTime(segmentTR.Start, srcInterval), formatIntervalRule(srcInterval)
}
