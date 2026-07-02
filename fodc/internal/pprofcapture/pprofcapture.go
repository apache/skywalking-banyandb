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

// Package pprofcapture defines the canonical set of pprof profiles the FODC agent captures
// under memory pressure. It is shared so the agent (which fetches them) and the proxy (which
// validates download requests) agree on a single source of truth for the profile types.
package pprofcapture

// Target describes one pprof endpoint captured on every memory-pressure trigger.
type Target struct {
	Type string // "heap" | "goroutine"
	Path string // request path under the pprof base, e.g. "/debug/pprof/heap"
}

// Targets are the profiles pulled on every trigger: a heap snapshot and a full goroutine
// dump (debug=0 keeps the compact gzip-protobuf form for go tool pprof).
var Targets = []Target{
	{Type: "heap", Path: "/debug/pprof/heap"},
	{Type: "goroutine", Path: "/debug/pprof/goroutine?debug=0"},
}

// IsValidType reports whether t is one of the captured profile types.
func IsValidType(t string) bool {
	for _, target := range Targets {
		if target.Type == t {
			return true
		}
	}
	return false
}
