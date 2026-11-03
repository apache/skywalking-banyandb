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

package main

import (
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

var (
	// number matches a decimal the expectations compare with gt, printed unquoted.
	number = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
	// plainKey matches a key printed unquoted.
	plainKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)
)

// printedValue is one non-null column of a printed row: its key, its value as read and as
// printed in yaml.
type printedValue struct {
	key   string
	raw   string
	value string
}

// printedRow returns the non-null columns of a row. A tag or field is keyed by its name and a
// fixed column by its header; a second column of the same name keeps its header. A string is
// printed decoded and a number unquoted; a long value is clipped.
func printedRow(columns []csvfmt.Column, row []string) []printedValue {
	var out []printedValue
	used := map[string]bool{}
	for i, c := range columns {
		cell := row[i]
		if cell == csvfmt.NullCell {
			continue
		}
		key := c.Name
		if key == "" || used[key] {
			key = c.Header
		}
		used[key] = true
		raw := cell
		if c.Type == csvfmt.ValueString {
			if v, _, err := csvfmt.DecodeCell(cell, c.Type); err == nil {
				raw = v.(string)
			}
		}
		out = append(out, printedValue{key: key, raw: raw, value: yamlScalar(raw)})
	}
	return out
}

// yamlScalar prints a value: a number as is, anything else as a quoted string, clipped.
func yamlScalar(v string) string {
	if number.MatchString(v) {
		return v
	}
	if len(v) > maxValueLength {
		cut := maxValueLength
		for cut > 0 && !utf8.RuneStart(v[cut]) {
			cut--
		}
		v = v[:cut] + "..."
	}
	return strconv.Quote(v)
}

// lookup returns the value of a printed row's column, "" when it is null.
func lookup(values []printedValue, key string) string {
	for _, v := range values {
		if v.key == key {
			return v.raw
		}
	}
	return ""
}

// knownSource reports whether a row comes from a source whose values the expectations can
// pin: an event report-events.sh reported, the traffic, call rate and response time of a
// trace-mocker service, a span of one, or the UI template the e2e setup creates.
func knownSource(group, resource string, values []printedValue) bool {
	switch group {
	case "sw_records":
		return resource == "event" && strings.HasPrefix(lookup(values, "uuid"), eventUUIDPrefix)
	case "sw_metadata":
		return resource == "service_traffic_minute" && strings.HasPrefix(lookup(values, "service_traffic_name"), mockServicePrefix)
	case "sw_metricsMinute", "sw_metricsHour", "sw_metricsDay":
		return (strings.HasPrefix(resource, "service_cpm_") || strings.HasPrefix(resource, "service_resp_time_")) &&
			mockServiceID(lookup(values, "entity_id"))
	case "sw_trace":
		return mockServiceID(lookup(values, "service_id"))
	case "sw_property":
		return resource == uiTemplate && lookup(values, "_id") == e2eTemplateID
	default:
		return false
	}
}

// mockServiceID reports whether an OAP service id (the base64 of the name, a dot and a flag)
// names one of trace-mocker's services.
func mockServiceID(id string) bool {
	name, _, _ := strings.Cut(id, ".")
	b, err := base64.StdEncoding.DecodeString(name)
	return err == nil && strings.HasPrefix(string(b), mockServicePrefix)
}

// printData prints the rows of known sources of a group under data, as one list per
// resource: the rows of one resource share their columns, so an expectation comparing a
// column with gt never meets a row without it.
func printData(byResource map[string][][]printedValue) {
	if len(byResource) == 0 {
		fmt.Println("    data: {}")
		return
	}
	fmt.Println("    data:")
	for _, resource := range slices.Sorted(maps.Keys(byResource)) {
		fmt.Printf("      %s:\n", yamlKey(resource))
		for _, row := range byResource[resource] {
			for i, v := range row {
				prefix := "          "
				if i == 0 {
					prefix = "        - "
				}
				fmt.Printf("%s%s: %s\n", prefix, yamlKey(v.key), v.value)
			}
		}
	}
}

// yamlKey prints a key, quoted unless it is a plain name.
func yamlKey(key string) string {
	if plainKey.MatchString(key) {
		return key
	}
	return strconv.Quote(key)
}
