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

package app

import (
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func schemaDetailLines(snapshot session.SchemaSnapshot) []string {
	if strings.TrimSpace(snapshot.Name) == "" {
		return nil
	}
	lines := []string{
		fmt.Sprintf("%s %s", snapshot.Type, snapshot.Name),
		"Group: " + strings.Join(snapshot.Groups, ","),
	}
	if !snapshot.Loaded {
		lines = append(lines,
			warnStyle.Render("Schema detail not loaded from BanyanDB API"),
			mutedStyle.Render("Check --addr and press enter again on the resource"))
		return lines
	}
	if len(snapshot.Columns) > 0 {
		lines = append(lines, titleStyle.Render("Typed columns"))
		for _, column := range snapshot.Columns {
			columnLabel := fmt.Sprintf("  · %s : %s(%s)", column.Name, strings.ToUpper(string(column.Kind)), column.Type)
			if column.Indexed {
				columnLabel += " · indexed"
			}
			lines = append(lines, columnLabel)
		}
		return lines
	}
	if len(snapshot.EntityTags) > 0 {
		lines = append(lines, titleStyle.Render("Entity (series key)"))
		for _, entityTag := range snapshot.EntityTags {
			lines = append(lines, "  · "+entityTag)
		}
	}
	if len(snapshot.Tags) > 0 {
		lines = append(lines, titleStyle.Render("Tags"))
		for _, tag := range snapshot.Tags {
			lines = append(lines, "  · "+tag)
		}
	}
	if len(snapshot.Fields) > 0 {
		lines = append(lines, titleStyle.Render("Fields"))
		for _, field := range snapshot.Fields {
			lines = append(lines, "  · "+field)
		}
	}
	if len(snapshot.IndexedFields) > 0 {
		lines = append(lines, titleStyle.Render("Indexed tags (ORDER BY)"))
		for _, indexedField := range snapshot.IndexedFields {
			lines = append(lines, "  · "+indexedField)
		}
	}
	if len(snapshot.Tags) == 0 && len(snapshot.Fields) == 0 && len(snapshot.EntityTags) == 0 {
		lines = append(lines, mutedStyle.Render("No tags or fields declared on this resource"))
	}
	return lines
}

func shortTypeLabel(resourceType session.ResourceType) string {
	switch resourceType {
	case session.ResourceTypeMeasure:
		return "MEASURE"
	case session.ResourceTypeStream:
		return "STREAM"
	case session.ResourceTypeTrace:
		return "TRACE"
	case session.ResourceTypeProperty:
		return "PROPERTY"
	case session.ResourceTypeTopN:
		return "TOPN"
	default:
		return "?"
	}
}
