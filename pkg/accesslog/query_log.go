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

package accesslog

import (
	"encoding/json"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// QueryLogEntry wraps a query request with execution timing information.
type QueryLogEntry struct {
	StartTime time.Time
	Request   proto.Message
	Service   string
	Error     string
	Duration  time.Duration
}

type queryJSONObject struct {
	StartTime time.Time `json:"start_time"`
	Request   string    `json:"request"`
	Service   string    `json:"service"`
	Error     string    `json:"error,omitempty"`
	Duration  int64     `json:"duration_ms"`
}

// NewQueryLogEntry creates a new query log entry with timing information.
func NewQueryLogEntry(service string, startTime time.Time, duration time.Duration, request proto.Message, err error) *QueryLogEntry {
	entry := &QueryLogEntry{
		Service:   service,
		StartTime: startTime,
		Duration:  duration,
		Request:   request,
	}
	if err != nil {
		entry.Error = err.Error()
	}
	return entry
}

// Marshal marshals the log entry to JSON.
func (l *QueryLogEntry) Marshal() ([]byte, error) {
	request, err := protojson.Marshal(l.Request)
	if err != nil {
		return nil, err
	}
	obj := &queryJSONObject{
		StartTime: l.StartTime,
		Request:   string(request),
		Service:   l.Service,
		Error:     l.Error,
		Duration:  l.Duration.Milliseconds(),
	}
	return json.Marshal(obj)
}
