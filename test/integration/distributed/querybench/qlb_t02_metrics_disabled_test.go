//go:build !querymetrics

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

package querybench

import (
	"fmt"
	"strings"
	"testing"
)

type qlbT02MetricsSnapshot struct{}

func qlbT02MetricsStart() (qlbT02MetricsSnapshot, error) {
	return qlbT02MetricsSnapshot{}, fmt.Errorf("qlb_t02 requires a test binary built with -tags=querymetrics")
}

func qlbT02MetricsFinish(qlbT02MetricsSnapshot, string, int, int) (map[string]uint64, []string, error) {
	return nil, nil, fmt.Errorf("qlb_t02 requires a test binary built with -tags=querymetrics")
}

func TestQLBT02RequiresQueryMetricsBuild(t *testing.T) {
	_, startErr := qlbT02MetricsStart()
	if startErr == nil || !strings.Contains(startErr.Error(), "-tags=querymetrics") {
		t.Fatalf("missing actionable build error: %v", startErr)
	}
	_, _, finishErr := qlbT02MetricsFinish(qlbT02MetricsSnapshot{}, "native_raw", 1, 1)
	if finishErr == nil || !strings.Contains(finishErr.Error(), "-tags=querymetrics") {
		t.Fatalf("missing actionable build error: %v", finishErr)
	}
}
