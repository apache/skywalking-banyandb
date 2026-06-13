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
	"strings"
	"testing"
)

// IndexMode rejection tests.

func TestRejectIndexModeGroups_FailsOnIndexMode(t *testing.T) {
	schemas := map[string]map[string]*measureSchemaInfo{
		"sw_metricsMinute": {
			"service_cpm_minute": {Group: "sw_metricsMinute", Name: "service_cpm_minute", IndexMode: false},
		},
		"sw_metadata": {
			"service_traffic_minute":  {Group: "sw_metadata", Name: "service_traffic_minute", IndexMode: true},
			"endpoint_traffic_minute": {Group: "sw_metadata", Name: "endpoint_traffic_minute", IndexMode: true},
		},
	}
	err := rejectIndexModeGroups([]string{"sw_metricsMinute", "sw_metadata"}, schemas)
	if err == nil {
		t.Fatalf("expected error for IndexMode group, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sw_metadata/service_traffic_minute") ||
		!strings.Contains(msg, "sw_metadata/endpoint_traffic_minute") {
		t.Fatalf("error message %q must name both offending measures", msg)
	}
}

func TestRejectIndexModeGroups_PassesWhenAllRegular(t *testing.T) {
	schemas := map[string]map[string]*measureSchemaInfo{
		"sw_metricsMinute": {
			"a": {Group: "sw_metricsMinute", Name: "a", IndexMode: false},
			"b": {Group: "sw_metricsMinute", Name: "b", IndexMode: false},
		},
	}
	if err := rejectIndexModeGroups([]string{"sw_metricsMinute"}, schemas); err != nil {
		t.Fatalf("expected nil for non-IndexMode groups, got: %v", err)
	}
}
