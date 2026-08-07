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

package trace

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// EncodedTimestamp marks an encoded tag value as a timestamp carrying Unix nanoseconds.
type EncodedTimestamp int64

// PartEncoderTag is one flat tag accepted by the production trace part encoder.
type PartEncoderTag struct {
	Value     any
	Name      string
	RawValue  []byte
	ValueType pbv1.ValueType
}

// PartEncoderRow is one span accepted by EncodePart. IndexSeries is ignored by
// the core encoder and preserves source series identity for companion indexes.
type PartEncoderRow struct {
	IndexSeries map[string]common.SeriesID
	IndexTags   map[string][]sidx.Tag
	TraceID     string
	SpanID      string
	Span        []byte
	Tags        []PartEncoderTag
	Timestamp   int64
}

// EncodePart converts rows through the same traces-to-mem-part encoder used by liaison writes and flushes one immutable part.
func EncodePart(root string, fileSystem fs.FileSystem, partID uint64, rows []PartEncoderRow) (string, func()) {
	traceData := generateTraces()
	for rowIdx := range rows {
		row := &rows[rowIdx]
		traceData.traceIDs = append(traceData.traceIDs, row.TraceID)
		traceData.timestamps = append(traceData.timestamps, row.Timestamp)
		traceData.spanIDs = append(traceData.spanIDs, row.SpanID)
		traceData.spans = append(traceData.spans, row.Span)
		traceData.tags = append(traceData.tags, buildPartEncoderTags(row.Tags))
	}

	memoryPart := &memPart{}
	memoryPart.mustInitFromTraces(traceData)
	path := partPath(root, partID)
	memoryPart.mustFlush(fileSystem, path)
	return path, func() { releaseTraces(traceData) }
}

func buildPartEncoderTags(tags []PartEncoderTag) []*tagValue {
	result := make([]*tagValue, 0, len(tags))
	for _, tag := range tags {
		if tag.ValueType != pbv1.ValueTypeUnknown {
			result = append(result, &tagValue{tag: tag.Name, valueType: tag.ValueType, value: append([]byte(nil), tag.RawValue...)})
			continue
		}
		result = append(result, encodePartEncoderTag(tag.Name, tag.Value))
	}
	return result
}

func encodePartEncoderTag(name string, value any) *tagValue {
	switch typedValue := value.(type) {
	case string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING,
			&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: typedValue}}})
	case int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT,
			&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: typedValue}}})
	case EncodedTimestamp:
		tagValue := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, int64(typedValue)))}}
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_TIMESTAMP, tagValue)
	case []string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING_ARRAY,
			&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: typedValue}}})
	case []int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT_ARRAY,
			&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: typedValue}}})
	case []byte:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_DATA_BINARY,
			&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: typedValue}})
	default:
		panic(fmt.Sprintf("unsupported part-encoder tag value type %T", value))
	}
}
