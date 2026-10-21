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

import "fmt"

type (
	qlbT05CacheStats      struct{ Compiles uint64 }
	qlbT05MetricsSnapshot struct{}
)

func qlbT05CacheSnapshot() qlbT05CacheStats { return qlbT05CacheStats{} }

func qlbT05MetricsStart() (qlbT05MetricsSnapshot, error) {
	return qlbT05MetricsSnapshot{}, fmt.Errorf("qlb_t05 requires a test binary built with -tags=querymetrics")
}

func qlbT05MetricsFinish(qlbT05MetricsSnapshot, string, int, int) (map[string]uint64, []string, error) {
	return nil, nil, fmt.Errorf("qlb_t05 requires a test binary built with -tags=querymetrics")
}
