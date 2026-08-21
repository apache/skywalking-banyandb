// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package tools implements controlled schema discovery and BYDBQL execution for the TUI.
package tools

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
)

// A query response is shaped per resource type and nests tag families, so the preview is derived by
// flattening one result item into columns rather than by assuming a fixed schema.

func responseRows(response *bydbqlv1.QueryResponse) (int, string) {
	if measureResult := response.GetMeasureResult(); measureResult != nil {
		return len(measureResult.GetDataPoints()), "measure"
	}
	if streamResult := response.GetStreamResult(); streamResult != nil {
		return len(streamResult.GetElements()), "stream"
	}
	if propertyResult := response.GetPropertyResult(); propertyResult != nil {
		return len(propertyResult.GetProperties()), "property"
	}
	if traceResult := response.GetTraceResult(); traceResult != nil {
		if len(traceResult.GetTraces()) > 0 {
			return len(traceResult.GetTraces()), "trace"
		}
		if traceResult.GetTraceQueryResult() != nil {
			return 1, "trace"
		}
		return 0, "trace"
	}
	if topNResult := response.GetTopnResult(); topNResult != nil {
		rows := 0
		for _, topNList := range topNResult.GetLists() {
			rows += len(topNList.GetItems())
		}
		return rows, "topn"
	}
	return 0, "unknown"
}

func responsePreview(body []byte, maxRows int) ([]string, [][]string, bool) {
	var value any
	if unmarshalErr := json.Unmarshal(body, &value); unmarshalErr != nil {
		return nil, nil, false
	}
	items := firstArray(value)
	if len(items) == 0 {
		return nil, nil, false
	}
	columns := previewColumns(items, maxRows)
	if len(columns) == 0 {
		columns = []string{"value"}
	}
	previewLength := min(len(items), maxRows)
	preview := make([][]string, 0, previewLength)
	for _, item := range items[:previewLength] {
		preview = append(preview, previewRow(item, columns))
	}
	return columns, preview, len(items) > previewLength
}

func firstArray(value any) []any {
	switch typedValue := value.(type) {
	case map[string]any:
		preferredKeys := []string{"dataPoints", "elements", "properties", "traces", "items", "lists"}
		for _, key := range preferredKeys {
			if items := firstArray(typedValue[key]); len(items) > 0 {
				return items
			}
		}
		keys := make([]string, 0, len(typedValue))
		for key := range typedValue {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if items := firstArray(typedValue[key]); len(items) > 0 {
				return items
			}
		}
	case []any:
		return typedValue
	}
	return nil
}

var (
	previewSkipTagKeys = map[string]struct{}{
		"tags_raw_data": {},
	}
	preferredPreviewColumns = []string{
		"timestamp",
		"trace_id",
		"endpoint_id",
		"content",
		"service_id",
		"service_instance_id",
		"span_id",
		"tags",
		"elementId",
		"unique_id",
		"trace_segment_id",
	}
)

func previewColumns(items []any, maxRows int) []string {
	if len(items) > 0 {
		if flat := flattenPreviewItem(items[0]); len(flat) > 0 {
			return orderedPreviewColumns(flat)
		}
	}
	columnSet := make(map[string]struct{})
	for _, item := range items[:min(len(items), maxRows)] {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for key := range object {
			columnSet[key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(columnSet))
	for column := range columnSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func previewRow(item any, columns []string) []string {
	if flat := flattenPreviewItem(item); len(flat) > 0 {
		row := make([]string, 0, len(columns))
		for _, column := range columns {
			row = append(row, flat[column])
		}
		return row
	}
	row := make([]string, 0, len(columns))
	object, objectOK := item.(map[string]any)
	for _, column := range columns {
		if !objectOK {
			row = append(row, previewValue(item))
			continue
		}
		row = append(row, previewValue(object[column]))
	}
	return row
}

func flattenPreviewItem(item any) map[string]string {
	object, ok := item.(map[string]any)
	if !ok {
		return nil
	}
	tagFamilies, tagFamiliesOK := object["tagFamilies"].([]any)
	if !tagFamiliesOK || len(tagFamilies) == 0 {
		return nil
	}
	flat := make(map[string]string)
	for _, topKey := range []string{"elementId", "timestamp", "traceId"} {
		if value, exists := object[topKey]; exists {
			flat[topKey] = previewValue(value)
		}
	}
	for _, familyValue := range tagFamilies {
		family, familyOK := familyValue.(map[string]any)
		if !familyOK {
			continue
		}
		tags, _ := family["tags"].([]any)
		for _, tagValue := range tags {
			tag, tagOK := tagValue.(map[string]any)
			if !tagOK {
				continue
			}
			tagKey, _ := tag["key"].(string)
			if tagKey == "" {
				continue
			}
			if _, skip := previewSkipTagKeys[tagKey]; skip {
				continue
			}
			flat[tagKey] = previewTagValue(tag["value"])
		}
	}
	if len(flat) == 0 {
		return nil
	}
	return flat
}

func orderedPreviewColumns(flat map[string]string) []string {
	columns := make([]string, 0, len(flat))
	seen := make(map[string]struct{}, len(flat))
	for _, column := range preferredPreviewColumns {
		if _, exists := flat[column]; !exists {
			continue
		}
		columns = append(columns, column)
		seen[column] = struct{}{}
	}
	rest := make([]string, 0, len(flat))
	for column := range flat {
		if _, alreadyUsed := seen[column]; alreadyUsed {
			continue
		}
		rest = append(rest, column)
	}
	sort.Strings(rest)
	return append(columns, rest...)
}

func previewTagValue(value any) string {
	valueMap, ok := value.(map[string]any)
	if !ok {
		return previewValue(value)
	}
	if _, hasBinary := valueMap["binaryData"]; hasBinary {
		return "<binary>"
	}
	if stringWrap, stringOK := valueMap["str"].(map[string]any); stringOK {
		return fmt.Sprint(stringWrap["value"])
	}
	if intWrap, intOK := valueMap["int"].(map[string]any); intOK {
		return fmt.Sprint(intWrap["value"])
	}
	if strArrayWrap, strArrayOK := valueMap["strArray"].(map[string]any); strArrayOK {
		arrayValue, arrayOK := strArrayWrap["value"].([]any)
		if !arrayOK {
			return previewValue(value)
		}
		parts := make([]string, 0, len(arrayValue))
		for _, element := range arrayValue {
			parts = append(parts, fmt.Sprint(element))
		}
		return strings.Join(parts, ",")
	}
	return previewValue(value)
}

func previewValue(value any) string {
	if stringValue, ok := value.(string); ok {
		return stringValue
	}
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return "<unavailable>"
	}
	return string(encodedValue)
}

func truncateBody(value string) string {
	const maxBodyLength = 300
	if len(value) <= maxBodyLength {
		return value
	}
	return value[:maxBodyLength] + "..."
}
