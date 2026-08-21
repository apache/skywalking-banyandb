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

package agent

import "strings"

// dataRequestMarkers name an explicit request for stored rows rather than for schema shape.
var dataRequestMarkers = []string{
	"show me the data", "show the data", "show data", "query the data", "query data",
	"run it", "run the query", "run this", "execute it", "execute the query", "execute this",
	"give me the rows", "return the rows", "fetch the rows", "how many rows",
	"latest", "last 30", "recent", "top ", "trend", "average", "count of", "sum of",
	"查数据", "查询数据", "查一下数据", "看数据", "跑一下", "跑这个", "执行查询", "执行一下",
	"最近", "最新", "最慢", "最快", "多少行", "总数", "平均", "趋势", "排名",
}

// schemaQuestionMarkers name a question about schema shape or catalog contents.
var schemaQuestionMarkers = []string{
	"what fields", "which fields", "what columns", "which columns", "what tags", "which tags",
	"what schema", "which schema", "what resources", "which resources", "what groups", "which groups",
	"describe", "list the", "list all", "show me the schema", "show the schema", "show schema",
	"what is", "what are", "what does", "how do i", "how can i", "can i", "is there",
	"explain", "structure of", "definition of", "available",
	"有哪些", "哪些字段", "什么字段", "什么结构", "表结构", "字段类型", "什么类型",
	"看下schema", "看schema", "查schema", "查看schema", "schema是", "的schema", "下面的schema",
	"是什么", "怎么写", "怎么用", "如何写", "如何用", "能不能", "支持吗", "解释",
	// Description verbs: a turn asking to be told about a resource is answered in words.
	"描述", "介绍", "说明一下", "讲讲", "讲一下", "列出", "列一下", "有什么用", "是干什么",
}

// schemaDescriptionMarkers name a request for the shape of one named resource.
//
// This is the narrower half of schemaQuestionMarkers: it excludes catalog questions and the
// usage and how-to phrasings, which ask for advice rather than for a resource description.
var schemaDescriptionMarkers = []string{
	"what fields", "which fields", "what columns", "which columns", "what tags", "which tags",
	"what schema", "which schema", "describe", "show me the schema", "show the schema", "show schema",
	"structure of", "definition of", "fields of", "columns of", "tags of", "schema of",
	"有哪些字段", "哪些字段", "什么字段", "有哪些列", "哪些列", "有哪些标签", "哪些标签",
	"什么结构", "表结构", "字段类型", "字段有", "看下schema", "看schema", "查schema", "查看schema",
	"的schema", "下面的schema", "描述", "介绍", "说明一下", "字段说明", "结构是",
}

// IsInformationalRequest reports whether a turn asks about schema or usage rather than for stored data.
//
// A data marker always wins, so "which fields does X have, and show the latest 10 rows" stays a query turn.
func IsInformationalRequest(turnHint string) bool {
	normalized := normalizeTurnHint(turnHint)
	if normalized == "" {
		return false
	}
	if containsAnyMarker(normalized, dataRequestMarkers) {
		return false
	}
	return containsAnyMarker(normalized, schemaQuestionMarkers)
}

// IsSchemaDescriptionRequest reports whether a turn asks for the shape of a resource it names.
//
// Such a turn is answerable from the BanyanDB schema API alone, so bydbctl can describe the
// resource itself instead of spending an agent round trip that would reach the same tool.
func IsSchemaDescriptionRequest(turnHint string) bool {
	normalized := normalizeTurnHint(turnHint)
	if normalized == "" {
		return false
	}
	if containsAnyMarker(normalized, dataRequestMarkers) {
		return false
	}
	return containsAnyMarker(normalized, schemaDescriptionMarkers)
}

// normalizeTurnHint lowercases a turn and collapses runs of whitespace to one space.
//
// Mixed-script turns such as "看下 schema" must match the same markers as their unspaced form.
func normalizeTurnHint(turnHint string) string {
	return strings.ToLower(strings.Join(strings.Fields(turnHint), " "))
}

// containsAnyMarker matches a marker against the turn, ignoring the spacing inside the marker itself.
func containsAnyMarker(normalized string, markers []string) bool {
	unspaced := strings.ReplaceAll(normalized, " ", "")
	for _, marker := range markers {
		if strings.Contains(normalized, marker) {
			return true
		}
		if unspacedMarker := strings.ReplaceAll(marker, " ", ""); unspacedMarker != "" &&
			strings.Contains(unspaced, unspacedMarker) {
			return true
		}
	}
	return false
}
