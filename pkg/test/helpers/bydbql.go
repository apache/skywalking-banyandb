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

package helpers

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// ExtractQL assembles a BydbQL query string from a .ql file's content, skipping
// blank and comment lines, and decodes the optional `#!params:` directive into
// TagValues bound to the query's `?` placeholders. The directive value is a JSON
// array of protojson-encoded model.v1.TagValue objects, e.g.
// `#!params: [{"str":{"value":"-15m"}},{"int":{"value":"10"}}]`.
func ExtractQL(content string) (string, []*modelv1.TagValue, error) {
	var query strings.Builder
	var params []*modelv1.TagValue
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if paramsJSON, found := strings.CutPrefix(trimmed, "#!params:"); found {
			var rawParams []json.RawMessage
			if err := json.Unmarshal([]byte(paramsJSON), &rawParams); err != nil {
				return "", nil, fmt.Errorf("failed to decode #!params directive: %w", err)
			}
			for _, rawParam := range rawParams {
				param := &modelv1.TagValue{}
				if err := protojson.Unmarshal(rawParam, param); err != nil {
					return "", nil, fmt.Errorf("failed to decode #!params entry %s: %w", rawParam, err)
				}
				params = append(params, param)
			}
			continue
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if query.Len() > 0 {
			query.WriteByte(' ')
		}
		query.WriteString(trimmed)
	}
	return query.String(), params, nil
}
